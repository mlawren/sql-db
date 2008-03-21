package SQL::DB;
use 5.006;
use strict;
use warnings;
use base qw(SQL::DB::Schema);
use Carp qw(carp croak confess);
use DBI;
use UNIVERSAL qw(isa);
use SQL::DB::Row;
use SQL::DB::Cursor;


our $VERSION = '0.11';
our $DEBUG   = 0;

our @EXPORT_OK = @SQL::DB::Schema::EXPORT_OK;
foreach (@EXPORT_OK) {
    no strict 'refs';
    *{$_} = *{'SQL::DB::Schema::'.$_};
}

# Define our sequence table
define_tables([
    table => 'sqldb',
    class => 'SQL::DB::Sequence',
    column => [name => 'name', type => 'VARCHAR(32)', unique => 1],
    column => [name => 'val', type => 'INTEGER'],
]);


# Tell each of the tables why type of DBI/Database we are connected to
sub _set_table_types {
    my $self = shift;
    (my $type = lc($self->{sqldb_dbi})) =~ s/^dbi:(.*?):.*/$1/;
    $type || confess 'bad/missing dbi: definition';

    foreach my $table ($self->tables) {
        $table->set_db_type($type);
    }
}


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = bless($class->SUPER::new(@_), $class);
    if (!eval {$self->table('sqldb');1;}) {
        $self->associate_table('sqldb');
    }
    return $self;
}


sub connect {
    my $self = shift;

    my ($dbi,$user,$pass,$attrs) = @_;

    if (my $dbh = DBI->connect($dbi,$user,$pass,$attrs)) {
        $self->{sqldb_dbh} = $dbh;
    }
    else {
        croak $DBI::errstr;
    }

    $self->{sqldb_dbi}    = $dbi;
    $self->{sqldb_user}   = $user;
    $self->{sqldb_pass}   = $pass;
    $self->{sqldb_attrs}  = $attrs;
    $self->{sqldb_qcount} = 0;
    $self->{sqldb_txn}    = 0;

    $self->_set_table_types();

    warn "debug: connected to $dbi" if($DEBUG);
    return;
}


sub connect_cached {
    my $self = shift;

    my ($dbi,$user,$pass,$attrs) = @_;

    if (my $dbh = DBI->connect_cached($dbi,$user,$pass,$attrs)) {
        $self->{sqldb_dbh} = $dbh;
    }
    else {
        croak $DBI::errstr;
    }

    $self->{sqldb_dbi}    = $dbi;
    $self->{sqldb_user}   = $user;
    $self->{sqldb_pass}   = $pass;
    $self->{sqldb_attrs}  = $attrs;
    $self->{sqldb_qcount} = 0;

    $self->_set_table_types();

    warn "debug: connect_cached to $dbi" if($DEBUG);
    return;
}


sub dbh {
    my $self = shift;
    return $self->{sqldb_dbh};
}


sub deploy {
    my $self = shift;

    # calculate which tables reference which other tables, and plan the
    # deployment order accordingly.
    my @tlist = grep {$_->name ne 'sqldb'} $self->tables;
    my @tables;
    my $deployed = {map {$_->name => 0} @tlist};

    my $max = 0;
    while (@tlist) {
        my @newtlist = ();
        foreach my $t (@tlist) {
            my $none = 1;
            foreach my $c ($t->columns) {
                if (my $ref = $c->references) {
                    # check for self reference or already deployed
                    next if ($ref->table == $t or
                             $deployed->{$ref->table->name});
                    $none = 0;
                }
            }
        
            if ($none) { # doesn't reference anyone not already deployed
                push(@tables, $t);
                $deployed->{$t->name} = 1;
            }
            else {
                push(@newtlist, $t);
            }
        }
        @tlist = @newtlist;

        if ($max++ > 2000) {
            croak 'infinite deployment calculation - reference columns loop?';
        }
    }

    if ($self->table('sqldb')) {
        unshift(@tables, $self->table('sqldb'));
    }

    TABLES: foreach my $table (@tables) {
        my $sth = $self->dbh->table_info('', '', $table->name, 'TABLE');
        if (!$sth) {
            die $DBI::errstr;
        }

        while (my $x = $sth->fetch) {
            if ($x->[2] eq $table->name) {
                carp 'Table '. $table->name .' already exists - not deploying';
                next TABLES;
            }
        }

        foreach my $action ($table->sql_create) {
            warn "debug: $action" if($DEBUG);
            my $res;
            eval {$res = $self->dbh->do($action);};
            if (!$res or $@) {
                die $self->dbh->errstr . ' query: '. $action;
            }
        }

        $self->create_seq($table->name);
    }
    return 1;
}



sub query_as_string {
    my $self = shift;
    my $sql  = shift || croak 'query_as_string requires an argument';
    
    foreach (@_) {
        if (defined($_) and $_ !~ /^[[:print:]]+$/) {
            $sql =~ s/\?/*BINARY DATA*/;
        }
        else {
            my $quote = $self->dbh->quote($_);
            $sql =~ s/\?/$quote/;
        }
    }
    return $sql;
}


sub do {
    my $self        = shift;
    my $query       = $self->query(@_);
    my $rv;

    eval {
        my $sth = $self->dbh->prepare_cached("$query");
        my $i = 1;
        foreach my $type ($query->bind_types) {
            if ($type) {
                $sth->bind_param($i, undef, $type);
                carp 'debug: binding param '.$i.' with '.$type if($DEBUG);
            }
            $i++;
        }
        $rv = $sth->execute($query->bind_values);
    };

    if ($@ or !defined($rv)) {
        croak "$@: Query was:\n"
            . $self->query_as_string("$query", $query->bind_values);
    }

    carp 'debug: '. $self->query_as_string("$query", $query->bind_values)
         ." /* Result: $rv */" if($DEBUG);

    $self->{sqldb_qcount}++;
    return $rv;
}


sub fetch {
    my $self  = shift;
    my $query = $self->query(@_);
    my $class = SQL::DB::Row->make_class_from($query->acolumns);

    my $sth;
    my $rv;

    eval {
        $sth = $self->dbh->prepare("$query");
        my $i = 1;
        foreach my $type ($query->bind_types) {
            if ($type) {
                $sth->bind_param($i, undef, $type);
            }
            $i++;
        }
        $rv = $sth->execute($query->bind_values);
    };

    if ($@ or !defined($rv)) {
        croak "$@: Query was:\n"
            . $self->query_as_string("$query", $query->bind_values);
    }

    if (wantarray) {
        my $arrayref;
        eval {
            $arrayref = $sth->fetchall_arrayref();
        };
        if (!$arrayref or $@) {
            croak "$@: Query was:\n"
                . $self->query_as_string("$query", $query->bind_values);
        }

        $self->{sqldb_qcount}++;
        carp 'debug: (Rows: '. scalar @$arrayref .') '.
              $self->query_as_string("$query", $query->bind_values) if($DEBUG);
        return map {$class->new_from_arrayref($_)->_inflate} @{$arrayref};
    }

    $self->{sqldb_qcount}++;
    carp 'debug: (Cursor call) '.
          $self->query_as_string("$query", $query->bind_values) if($DEBUG);

    return SQL::DB::Cursor->new($sth, $class);
}


# FIXME bind parameters in here?
sub fetch1 {
    my $self  = shift;
    my $query = $self->query(@_);
    my $class = SQL::DB::Row->make_class_from($query->acolumns);

    my @list;
    eval {
        @list = $self->dbh->selectrow_array("$query", undef,
                                             $query->bind_values);
    };
    if ($@) {
        croak "$@: Query was:\n"
            . $self->query_as_string("$query", $query->bind_values);
    }

    return unless(@list);

    $self->{sqldb_qcount}++;
    carp 'debug: (Rows: '. scalar @list .') '.
          $self->query_as_string("$query", $query->bind_values) if($DEBUG);
    return $class->new_from_arrayref(\@list)->_inflate;
}


sub txn {
    my $self = shift;
    my $subref = shift;
    (ref($subref) && ref($subref) eq 'CODE') || croak 'usage txn($subref)';

    $self->{sqldb_txn}++;

    if ($self->{sqldb_txn} == 1) {
        $self->dbh->begin_work;
        carp 'debug: BEGIN WORK (txn 1)' if($DEBUG);
    }
    else {
        carp 'debug: Begin Work (txn '.$self->{sqldb_txn}.')' if($DEBUG);
    }


    my $result = eval {local $SIG{__DIE__}; &$subref};

    if ($@) {
        my $tmp = $@;
        if ($self->{sqldb_txn} == 1) { # top-most txn
            carp 'debug: ROLLBACK (txn 1)' if($DEBUG);
            eval {$self->dbh->rollback};
        }
        else { # nested txn - die so the outer txn fails
            warn 'debug: FAIL Work (txn '.$self->{sqldb_txn}.'): '
                 . $tmp if($DEBUG);
            $self->{sqldb_txn}--;
            die $tmp;
        }
        $self->{sqldb_txn}--;
        if (wantarray) {
            return (undef, $tmp);
        }
        return;
    }

    if ($self->{sqldb_txn} == 1) {
        carp 'debug: COMMIT (txn 1)' if($DEBUG);
        $self->dbh->commit;
    }
    else {
        carp 'debug: End Work (txn '.$self->{sqldb_txn}.')' if($DEBUG);
    }
    $self->{sqldb_txn}--;

    if (wantarray) {
        return (1);
    }
    return 1;
}


sub create_seq {
    my $self = shift;
    my $name = shift || croak 'usage: $db->create_seq($name)';

    $self->{sqldb_dbi} || croak 'Must be connected before calling create_seq';

    my $s = SQL::DB::Schema::ARow::sqldb->new;

    if (eval {
        $self->do(
            insert  => [$s->name, $s->val],
            values  => [$name, 0],
        );
        }) {
        return 1;
    }
    croak "create_seq: $@";
    return;
}


sub seq {
    my $self = shift;
    my $name = shift || croak 'usage: $db->seq($name)';
    my $count = shift || 1;

    if ($count> 1 and !wantarray) {
        croak 'you should want the full array of sequences';
    }

    $self->{sqldb_dbi} || croak 'Must be connected before calling seq';

    my $sqldb = SQL::DB::Schema::ARow::sqldb->new;
    my $seq;
    my $no_updates;

    eval {
        # Aparent MySQL bug - no locking with FOR UPDATE
        if ($self->{sqldb_dbi} =~ m/mysql/i) {
            $self->dbh->do('LOCK TABLES sqldb WRITE, sqldb AS '.
                                    $sqldb->_alias .' WRITE');
        }

        $seq = $self->fetch1(
            select     => [$sqldb->val],
            from       => $sqldb,
            where      => $sqldb->name == $name,
            for_update => ($self->{sqldb_dbi} !~ m/sqlite/i),
        );

        croak "Can't find sequence '$name'" unless($seq);

        $no_updates = $self->do(
            update  => [$sqldb->val->set($seq->val + $count)],
            where   => $sqldb->name == $name,
        );

        if ($self->{sqldb_dbi} =~ m/mysql/i) {
            $self->dbh->do('UNLOCK TABLES');
        }

    };

    if ($@ or !$no_updates) {
        my $tmp = $@;

        if ($self->{sqldb_dbi} =~ m/mysql/i) {
            $self->dbh->do('UNLOCK TABLES');
        }

        croak "seq: $tmp";
    }


    if (wantarray) {
        my $start = $seq->val + 1;
        my $stop  = $start + $count - 1;
        return ($start..$stop);
    }
    return $seq->val + $count;
}


sub insert {
    my $self = shift;
    foreach my $obj (@_) {
        unless(ref($obj) and $obj->can('q_update')) {
            croak "Not an insertable object: $obj";
        }
        my ($arows, @inserts) = $obj->q_insert; # reference hand-holding
        foreach (@inserts) {
            if (!$self->do(@$_)) {
                croak 'INSERT for '. ref($obj) . ' object failed';
            }
        }
    }
    return 1;
}


sub update {
    my $self = shift;
    foreach my $obj (@_) {
        unless(ref($obj) and $obj->can('q_update')) {
            croak "Not an updatable object: $obj";
        }
        my ($arows, @updates) = $obj->q_update; # reference hand-holding
        foreach (@updates) {
            if ($self->do(@$_) != 1) {
                croak 'UPDATE for '. ref($obj) . ' object failed';
            }
        }
    }
    return 1;
}


sub delete {
    my $self = shift;
    foreach my $obj (@_) {
        unless(ref($obj) and $obj->can('q_update')) {
            croak "Not a deletable object: $obj";
        }
        my ($arows, @deletes) = $obj->q_delete; # reference hand-holding
        foreach (@deletes) {
            if ($self->do(@$_) != 1) {
                croak 'DELETE for '. ref($obj) . ' object failed';
            }
        }
    }
    return 1;
}


sub qcount {
    my $self = shift;
    return $self->{sqldb_qcount};
}


sub quickrows {
    my $self = shift;
    return unless(@_);

    my @keys = $_[0]->_column_names;
    my $c = join(' ', map {'%-'.(length($_)+ 2).'.'
                           .(length($_)+ 2).'s'} @keys) . "\n";

    my $str = sprintf($c, @keys);

    foreach my $row (@_) {
        my @values = map {$row->$_} @keys;
        my @print = map {
            !defined($_) ?
            'NULL' :
            ($_ !~ m/^[[:print:]]*$/ ? '*BINARY*' : $_)
        } @values;

        $str .= sprintf($c, @print);
    }
    return $str;
}
    

sub disconnect {
    my $self = shift;
    if ($self->dbh) {
        warn 'debug: Disconnecting from DBI' if($DEBUG);
        $self->dbh->disconnect;
        delete $self->{sqldb_dbh};
    }
    return;
}


#DESTROY {
#    my $self = shift;
#    $self->disconnect;
#    return;
#}


1;
__END__

=head1 NAME

SQL::DB - Perl interface to SQL Databases

=head1 VERSION

0.11. Development release.

=head1 SYNOPSIS

  use SQL::DB qw(define_tables count max);

  define_tables(
    [
      table  => 'addresses',
      class  => 'Address',
      column => [name => 'id',   type => 'INTEGER', primary => 1],
      column => [name => 'kind', type => 'INTEGER'],
      column => [name => 'city', type => 'INTEGER'],
    ],
    [
      table  => 'persons',
      class  => 'Person',
      column => [name => 'id',      type => 'INTEGER', primary => 1],
      column => [name => 'name',    type => 'VARCHAR(255)'],
      column => [name => 'age',     type => 'INTEGER'],
      column => [name => 'address', type => 'INTEGER',
                                    ref  => 'addresses(id)',
                                    null => 1],
      column => [name => 'parent',  type => 'INTEGER',
                                    ref  => 'persons(id)',
                                    null => 1],
      index  => 'name',
    ]
  );

  my $db = SQL::DB->new();

  $db->connect('dbi:SQLite:/tmp/sqldbtest.db', 'user', 'pass', {});
  $db->deploy;

  my $persons   = $db->arow('persons');
  my $addresses = $db->arow('addresses');

  $db->do(
    insert => [$persons->id, $persons->name, $persons->age],
    values => [1, 'Homer', 43],
  );

  $db->do(
    insert => [$addresses->id, $addresses->kind, $addresses->city],
    values => [2, 'residential', 'Springfield'],  # Pg: [nextval('id')...
  );

  $db->do(
    update => [$persons->set_address(2)],
    where  => $persons->name == 'Homer',
  );


  my $ans = $db->fetch1(
    select => [count($persons->name)->as('count_name'),
                  max($persons->age)->as('max_age')],
    from   => $persons,
    where  => $persons->age > 40,
  );

  # The following prints "Head count: 1 Max age:43"
  print 'Head count: '. $ans->count_name .
          ' Max age: '. $ans->max_age ."\n";


  my @items = $db->fetch(
    select    => [$persons->name, $persons->age, $addresses->city],
    from      => $persons,
    left_join => $addresses,
    on        => $addresses->id == $persons->address,
    where     => ($addresses->city == 'Springfield') & ($persons->age > 40),
    order_by  => $persons->age->desc,
    limit     => 10,
  );

  # Give me "Homer(43) lives in Springfield"
  foreach my $item (@items) {
      print $item->name, '(',$item->age,') lives in ', $item->city, "\n";
  }

=head1 DESCRIPTION

B<SQL::DB> provides a low-level interface to SQL databases, using
Perl objects and logic operators. It is NOT an Object
Relational Mapper like L<Class::DBI> and neither is it an abstraction
such as L<SQL::Abstract>. It falls somewhere inbetween.

After using define_tables() to specify your schema and creating an
B<SQL::DB> object, the typical workflow is as follows:

* connect() to the database

* deploy() the schema (CREATE TABLEs etc)

* Using one or more "abstract rows" obtained via arow() you can
do() insert, update or delete queries.

* Using one or more "abstract rows" obtained via arow() you can
fetch() (select) data to work with (and possibly modify).

* Repeat the above three steps as needed. Further queries (with a
higher level of automation) are possible with the objects returned by
fetch().

* disconnect() from the database.

B<SQL::DB> is capable of generating just about any kind of query,
including, but not limited to, JOINs, nested SELECTs, UNIONs, 
database-side operator invocations, function calls, aggregate
expressions, etc. However this package is still quite new, and nowhere
near complete. Feedback, testing, and (even better) patches are all
welcome.

For a more complete introduction see L<SQL::DB::Intro>.

=head1 CLASS SUBROUTINES

=head2 define_tables(@definitions)

Define the structure of tables, their columns, and associated indexes.
@definition is list of ARRAY references as required by
L<SQL::DB::Schema::Table>. This class subroutine can be called multiple
times. Will warn if you redefine a table.

=head1 METHODS

=head2 new(@names)

Create a new B<SQL::DB> object. The optional @names lists the tables
that this object is to know about. By default all tables defined by
define_tables() are known.

=head2 connect($dbi, $user, $pass, $attrs)

Connect to a database. The parameters are passed directly to
L<DBI>->connect. This method also informs the internal table/column
representations what type of database we are connected to, so they
can set their database-specific features accordingly.

=head2 connect_cached($dbi, $user, $pass, $attrs)

Connect to a database, potentially reusing an existing connection.
The parameters are passed directly to L<DBI>->connect_cached. Useful
when running under persistent environments.
This method also informs the internal table/column
representations what type of database we are connected to, so they
can set their database-specific features accordingly.

=head2 dbh

Returns the L<DBI> database handle we are connected with.

=head2 deploy

Runs the CREATE TABLE and CREATE INDEX statements necessary to
create the schema in the database. Will warn on any tables that
already exist. Table creation is automatically ordered based on column
references.

=head2 query(@query)

Return an L<SQL::DB::Schema::Query> object as defined by @query. This method
is useful when creating nested SELECTs, UNIONs, or you can print the
returned object if you just want to see what the SQL looks like.

=head2 query_as_string($sql, @bind_values)

An internal function for pretty printing SQL queries by inserting the
bind values into the SQL itself. Returns a string.

=head2 do(@query)

Constructs a L<SQL::DB::Schema::Query> object as defined by @query and runs
that query against the connected database.  Croaks if an error occurs.
This is the method to use for any statement that doesn't retrieve
values (eg INSERT, UPDATE and DELETE). Returns whatever value the
underlying L<DBI>->do call returns.

=head2 fetch(@query)

Constructs an L<SQL::DB::Schema::Query> object as defined by @query and runs
that query against the connected database.  Croaks if an error occurs.
This method should be used for SELECT-type statements that retrieve
rows.

When called in array context returns a list of objects based on
L<SQL::DB::Row>. The objects have accessors for each column in the
query. Be aware that this can consume large amounts of memory if there
are lots of rows retrieved.

When called in scalar context returns a query cursor (L<SQL::DB::Cursor>)
(with "next", "all" and "reset" methods) to retrieve dynamically
constructed objects one at a time.

=head2 fetch1(@query)

Similar to fetch() but always returns only the first object from
the result set. All other rows (if any) can not be retrieved.
You should only use this method if you know/expect one result.

=head2 txn(&coderef)

Runs the code in &coderef as an SQL transaction. If &coderef does not
raise any exceptions then the transaction is commited, otherwise it is
rolled back.

In scalar context returns true/undef on sucess/failure. In array context
returns (true/undef, $errstr) on success/failure.

This method can be called recursively, but any sub-transaction failure
will always result in the outer-most transaction also being rolled back.

=head2 qcount

Returns the number of successful queries that have been run.

=head2 quickrows(@objs)

Returns a string containing the column values of @objs in a tabular
format. Useful for having a quick look at what the database has returned:

    my @objs = $db->fetch(....);
    warn $db->quickrows(@objs);

=head2 create_seq($name)

This (and the seq() method below) are the only attempt that B<SQL::DB>
makes at cross-database abstraction. create_seq() creates a sequence called
$name. The sequence is actually just a row in the 'sqldb' table.

Warns if the sequence already exists, returns true if successful.

=head2 seq($name,$count)

Return the next value for the sequence $name. If $count is specified then
a list/array of $count values are returned. The uniqueness of the
returned value(s) is assured by locking the appropriate table (or rows in
the table) as required.

Note that this is not intended as a replacment for auto-incrementing primary
keys in MySQL/SQLite, or real sequences in PostgreSQL. It is simply an
ease-of-use mechanism for applications wishing to use a common sequence
api across multiple databases.

=head2 disconnect

Disconnect from the database. Effectively DBI->disconnect.

=head1 METHODS ON FETCHED OBJECTS

Although B<SQL::DB> is not an ORM system it does comes with a very
thin object layer. Objects returned by fetch() and fetch1() can be
modified using their set_* methods, just like a regular ORM system.
However, the difference here is that the objects fields may map across
multiple database tables. 

Since the objects keep track of which columns have changed, and they
also know which columns belong to which tables and which columns are
primary keys, they can also automatically generate the appropriate
commands for UPDATE or DELETE statements in order to make matching
changes in the database.

Of course, the appropriate statements only work if the primary keys have
been included as part of the fetch(). See the q_update() and q_delete()
methods in L<SQL::DB::Row> for more details.

=head2 update($sqlobject)

Nearly the same as $db->do($sqlobject->q_update).

=head2 delete($sqlobject)

Nearly the same as $db->do($sqlobject->q_delete).

=head2 insert($sqlobject)

Nearly the same as $db->do($sqlobject->q_insert).

=head1 DEBUGGING

If $SQL::DB::DEBUG is set to a true value then SQL
queries and other important actions are 'warn'ed to STDERR

=head1 SEE ALSO

L<SQL::Abstract>, L<DBIx::Class>, L<Class::DBI>, L<Tangram>

You can see B<SQL::DB> in action in the L<MySpam> application, also
by the same author.

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
