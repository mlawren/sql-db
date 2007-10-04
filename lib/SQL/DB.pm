package SQL::DB;
use 5.006;
use strict;
use warnings;
use base qw(SQL::DB::Schema);
use Carp qw(carp croak confess);
use DBI;
use UNIVERSAL qw(isa);
use SQL::DB::Schema;
use SQL::DB::Function;
use SQL::DB::Row;
use Class::Accessor::Fast;

use Data::Dumper;
$Data::Dumper::Indent = 1;

our $VERSION = '0.06';
our $DEBUG   = 0;
our @EXPORT_OK = @SQL::DB::Function::EXPORT_OK;

foreach (@EXPORT_OK) {
    no strict 'refs';
    *{$_} = *{'SQL::DB::Function::'.$_};
}


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = bless($class->SUPER::new(@_), $class);
    $self->define([
        table => 'sqldb',
        class => 'SQL::DB::Sequence',
        column => [name => 'name', type => 'VARCHAR(32)', unique => 1],
        column => [name => 'val', type => 'INTEGER'],
    ]);
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

    warn "debug: connect_cached to $dbi" if($DEBUG);
    return;
}


sub dbh {
    my $self = shift;
    return $self->{sqldb_dbh};
}


sub deploy {
    my $self = shift;

    TABLES: foreach my $table ($self->tables) {
        my $sth = $self->{sqldb_dbh}->table_info('', '', $table->name, 'TABLE');
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
            eval {$res = $self->{sqldb_dbh}->do($action);};
            if (!$res or $@) {
                die $self->{sqldb_dbh}->errstr . ' query: '. $action;
            }
        }
    }
    return 1;
}


sub create_seq {
    my $self = shift;
    my $name = shift || croak 'usage: $db->create_seq($name)';

    $self->{sqldb_dbi} || croak 'Must be connected before calling create_seq';

    my $s = SQL::DB::ARow::sqldb->new;

    if (eval {
        $self->do(
            insert  => [$s->name, $s->val],
            values  => [$name, 0],
        );
        }) {
        return 1;
    }
    warn "CreateSequence: $@";
    return;
}


sub seq {
    my $self = shift;
    my $name = shift || croak 'usage: $db->seq($name)';

    $self->{sqldb_dbi} || croak 'Must be connected before calling seq';

    $self->{sqldb_dbh}->begin_work;
    my $sqldb = SQL::DB::ARow::sqldb->new;
    my $seq;
    my $no_updates;

    eval {
        # Aparent MySQL bug - no locking with FOR UPDATE
        if ($self->{sqldb_dbi} =~ m/mysql/i) {
            $self->{sqldb_dbh}->do('LOCK TABLES sqldb WRITE, sqldb AS '.
                                    $sqldb->_alias .' WRITE');
        }

        $seq = $self->fetch1(
            select     => [$sqldb->val],
            from       => $sqldb,
            where      => $sqldb->name == $name,
            for_update => ($self->{sqldb_dbi} !~ m/sqlite/i),
        );

        if (!$seq) {
            my $q = $self->query(
                select     => [$sqldb->val],
                from       => $sqldb,
                where      => $sqldb->name == $name,
                for_update => ($self->{sqldb_dbi} !~ m/sqlite/i),
            );
            croak "Can't find sequence $name. Query was ". $q->_as_string unless($seq);
        }

        $no_updates = $self->do(
            update  => [$sqldb->val->set($seq->val + 1)],
            where   => $sqldb->name == $name,
        );

        if ($self->{sqldb_dbi} =~ m/mysql/i) {
            $self->{sqldb_dbh}->do('UNLOCK TABLES');
        }

    };

    if ($@ or !$no_updates) {
        my $tmp = $@;
        eval {$self->{sqldb_dbh}->rollback;};

        if ($self->{sqldb_dbi} =~ m/mysql/i) {
            $self->{sqldb_dbh}->do('UNLOCK TABLES');
        }

        croak "seq: $tmp";
    }


    $self->{sqldb_dbh}->commit;
    return $seq->val + 1;
}


# ------------------------------------------------------------------------
# For everything other than SELECT
# ------------------------------------------------------------------------

sub do {
    my $self        = shift;
    my $query       = $self->query(@_);

    my $rv;
    eval {
        $rv = $self->{sqldb_dbh}->do("$query", undef, $query->bind_values);
    };

    if ($@ or !defined($rv)) {
        croak "DBI::do $DBI::errstr $@: Query was:\n"
               . $self->query_as_string("$query", $query->bind_values);
    }

    $self->{sqldb_qcount}++;

    carp 'debug: '. $self->query_as_string("$query", $query->bind_values)
         ." /* Result: $rv */" if($DEBUG);
    return $rv;
}


sub execute {
    my $self = shift;
    my ($sql,$attrs,@bind) = @_;

    my $sth;
    eval {
        $sth = $self->{sqldb_dbh}->prepare($sql);
    };
    if ($@ or !$sth) {
        croak "DBI::prepare $DBI::errstr $@: Query was:\n"
              . $self->query_as_string($sql, @bind);
    }

    my $res;
    eval {
        $res = $sth->execute(@bind);
    };
    if (!$res or $@) {
        croak "DBI::execute $DBI::errstr $@: Query was:\n"
              . $self->query_as_string($sql, @bind);
    }

    carp 'debug:'. $self->query_as_string($sql, @bind) 
         ." /* RESULT: $res */" if($DEBUG);
    $self->{sqldb_qcount}++;
    return $sth;
}



# ------------------------------------------------------------------------
# SELECT
# ------------------------------------------------------------------------


sub fetch {
    my $self  = shift;
    my $query = $self->query(@_);
    my $class = SQL::DB::Row->make_class_from($query->acolumns);

    if (wantarray) {
        my $arrayref;
        eval {
            $arrayref = $self->{sqldb_dbh}->selectall_arrayref(
                        "$query", undef, $query->bind_values);
        };
        if (!$arrayref or $@) {
            croak "DBI::selectall_arrayref: $DBI::errstr $@: Query was:\n"
                . $query->_as_string;
        }

        $self->{sqldb_qcount}++;
        carp 'debug: (Rows: '. scalar @$arrayref .') '.
              $self->query_as_string("$query", $query->bind_values) if($DEBUG);
        return map {$class->new($_)->_inflate} @{$arrayref};
    }

    croak 'sorry, cursor support not yet implemented';
}


sub fetch1 {
    my $self  = shift;
    my $query = $self->query(@_);
    my $class = SQL::DB::Row->make_class_from($query->acolumns);

    my @list = $self->{sqldb_dbh}->selectrow_array("$query", undef,
                                                     $query->bind_values);
    $self->{sqldb_qcount}++;
    carp 'debug: '. $self->query_as_string("$query", $query->bind_values) if($DEBUG);

    if (@list) {
        return $class->new(\@list)->_inflate;
    }
    return;
}


sub fetcho {
    my $self = shift;
    my $query   = $self->query(@_);
    my $sth     = $self->execute("$query", undef, $query->bind_values);

    return $self->objects($query, $sth);
}


sub fetcho1 {
    my $self = shift;
    my $query   = $self->query(@_);
    my $sth     = $self->execute($query, undef, $query->bind_values);

    my @results;
    @results = $self->objects($query, $sth);
    return $results[0];
}


sub objects {
    my $self    = shift;
    my $query   = shift;
    my $sth     = shift;
    my @acols   = $query->acolumns;

    my @returns;

    while (my $row = $sth->fetchrow_arrayref) {
        my %objs;
        my $i = 0;
        my $first;
        my @references;

        foreach my $col (map {$_->_column} @acols) {
            if (!$col or !ref($col)) {
                confess "Missing column ". $col;
            }
            my $class = $col->table->class;
            if (!$class) {
                die "Missing class for table ". $col->table->name;
            }
            my $set   = 'set_' . $col->name;

            if (!$objs{$class}) {
                $objs{$class} = $class->new;
            }
            my $obj = $objs{$class};
            $first ||= $obj;

            $obj->$set($row->[$i++]);

            if ($col->primary and defined($row->[$i-1])) {
                $obj->_in_storage(1);
            }
#    warn "$set ".$row->[$i-1];
            if (my $ref = $col->references) {
                # FIXME need to check that refclass was part of the query.
                # otherwise we are creating objects that were not wanted...
                my $refclass = $ref->table->class;
                push(@references, $col);
            }
        }

        foreach my $r (@references) {
            next unless($objs{$r->table->class}->_in_storage);
            if (my $target = $objs{$r->references->table->class}) {
                my $set = 'set_' . $r->name;
                if ($target->_in_storage) {
                    $objs{$r->table->class}->$set($target);
                }
                else {
                    $objs{$r->table->class}->$set(undef); # FIXME: autoload based on key value
                }
            }
        }

        foreach my $o (values %objs) {
            $o->{_changed} = {};
        }
        push(@returns, $first);
    }
    die $self->{sqldb_dbh}->errstr if ($self->{sqldb_dbh}->errstr);

    warn 'debug: # returns: '. scalar(@returns) if($DEBUG);

    return @returns;
}


sub insert {
    my $self = shift;
    foreach my $obj (@_) {
        unless(ref($obj) and $obj->can('q_update')) {
            croak "Not an insertable object: $obj";
        }
        foreach ($obj->q_insert) {
            if (!$self->do(@$_)) {
                croak 'INSERT for '. ref($obj) . ' object failed';
            }
        }
#        $obj->_in_storage(1);
    }
    return 1;
}


sub update {
    my $self = shift;
    foreach my $obj (@_) {
        unless(ref($obj) and $obj->can('q_update')) {
            croak "Not an updatable object: $obj";
        }
        foreach ($obj->q_update) {
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
        $obj->_in_storage || croak "Can only delete items already in storage";
        foreach ($obj->q_delete) {
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


sub query_as_string {
    my $self = shift;
    my $sql  = shift || croak 'query_as_string requires an argument';
    
    foreach (@_) {
        my $quote = $self->{sqldb_dbh}->quote($_);
        $sql =~ s/\?/$quote/;
    }
    return $sql;
}


sub disconnect {
    my $self = shift;
    if ($self->{sqldb_dbh}) {
        warn 'debug: Disconnecting from DBI' if($DEBUG);
        $self->{sqldb_dbh}->disconnect;
    }
    return;
}


DESTROY {
    my $self = shift;
    $self->disconnect;
    return;
}


1;
__END__

=head1 NAME

SQL::DB - Perl interface to SQL Databases

=head1 VERSION

0.06. Development release.

=head1 SYNOPSIS

  use SQL::DB qw(max min coalesce count nextval currval setval);
  my $db = SQL::DB->new();

  $db->define([
      table  => 'addresses',
      class  => 'Address',
      column => [name => 'id',   type => 'INTEGER', primary => 1],
      column => [name => 'kind', type => 'INTEGER'],
      column => [name => 'city', type => 'INTEGER'],
#      seq    => [name => 'name', start => 1, increment => 2],
  ]);

  $db->define([
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
  ]);

  $db->connect('dbi:SQLite:/tmp/sqldbtest.db', 'user', 'pass', {});
  $db->deploy;


  my $persons  = $db->arow('persons');
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

  print 'Head count: '. $ans->count_name .' Max age:'.$ans->max_age."\n";
  # "Head count: 1 Max age:43"


  my @items = $db->fetch(
    select    => [$persons->name, $persons->age, $addresses->city],
    from      => $persons,
    left_join => $addresses,
    on        => $addresses->id == $persons->address,
    where     => ($addresses->city == 'Springfield') & ($persons->age > 40),
    order_by  => $persons->age->desc,
    limit     => 10,
  );

  foreach my $item (@items) {
      print $item->name, '(',$item->age,') lives in ', $item->city, "\n";
  }
  # "Homer(43) lives in Springfield"
  return @items # this line for automatic test

=head1 DESCRIPTION

B<SQL::DB> provides a low-level interface to SQL databases, using
Perl objects and logic operators. It is NOT an Object
Relational Mapper like L<Class::DBI> and neither is it an abstraction
such as L<SQL::Abstract>. It falls somewhere inbetween.

The typical workflow is as follows. After creating an B<SQL::DB>
object you can:

* define() the desired or existing schema (tables and columns)

* connect() to the database

* deploy() the schema (CREATE TABLEs etc)

* Create one or more "abstract row" objects using arow().

* do() insert, update or delete queries defined using the abstract
row objects.

* fetch() (select) data with queries defined using the abstract row
objects.

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

=head1 METHODS

=head2 new

Create a new B<SQL::DB> object.

=head2 define(@def)

Define the structure of the tables and indexes in the database. @def
is a list of ARRAY references as required by L<SQL::DB::Table>.

=head2 connect($dbi, $user, $pass, $attrs)

Connect to a database. The parameters are passed directly to
L<DBI>->connect.

=head2 connect_cached($dbi, $user, $pass, $attrs)

Connect to a database, potentially reusing an existing connection.
The parameters are passed directly to L<DBI>->connect_cached. Useful
when running under persistent environments.

=head2 dbh

Returns the L<DBI> database handle we are connected with.

=head2 deploy

Runs the CREATE TABLE and CREATE INDEX statements necessary to
create the schema in the database. Will warn on any tables that
already exist.

=head2 query(@query)

Return an L<SQL::DB::Query> object as defined by @query. This method
is useful when creating nested SELECTs, UNIONs, or you can print the
returned object if you just want to see what the SQL looks like.

=head2 do(@query)

Constructs a L<SQL::DB::Query> object as defined by @query and runs
that query against the connected database.  Croaks if an error occurs.
This is the method to use for any statement that doesn't retrieve
values (eg INSERT, UPDATE and DELETE). Returns whatever value the
underlying L<DBI>->do call returns.

=head2 fetch(@query)

Constructs an L<SQL::DB::Query> object as defined by @query and runs
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

=head2 qcount

Returns the number of successful queries that have been run.

The following methods are part of the _very thin_ object layer that is
part of B<SQL::DB>. The objects returned by fetch() and fetch1() can
typically be used here. See L<SQL::DB::Row> for more details.

=head2 insert($sqlobject)

A shortcut for $db->do($sqlobject->q_insert).

=head2 update($sqlobject)

A shortcut for $db->do($sqlobject->q_update).

=head2 delete($sqlobject)

A shortcut for $db->do($sqlobject->q_delete).

=head1 DEBUGGING

If $SQL::DB::DEBUG is set to a true value then SQL
queries and other important actions are 'warn'ed to STDERR

=head1 SEE ALSO

L<SQL::Abstract>, L<DBIx::Class>, L<Class::DBI>, L<Tangram>

You can see B<SQL::DB> in action in the L<MySpam> application
(disclaimer: I am also the author)

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
