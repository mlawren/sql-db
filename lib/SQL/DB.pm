package SQL::DB;
use 5.006;
use strict;
use warnings;
use Carp qw(carp croak confess);
use DBI;
use Scalar::Util qw(refaddr);
use Class::Struct qw(struct);
use SQL::DB::Schema;

use Data::Dumper;
$Data::Dumper::Indent = 1;

our $VERSION = '0.03';
our $DEBUG   = undef;


no strict 'refs';
*schema = *SQL::DB::Schema::new;
use strict;


sub connect {
    my $proto = shift;
    my $class = ref($proto) || $proto;

    my ($dbi,$user,$pass,$attrs,$schema) = @_;

    if (!$schema) {
        croak 'usage: connect($dbi,$user,$pass,$attrs,$schema)';
    }

    my $self  = {
        dbi    => $dbi,
        user   => $user,
        pass   => $pass,
        attrs  => $attrs,
        schema => $schema,
    };

    if (my $dbh = DBI->connect($dbi,$user,$pass,$attrs)) {
        $self->{dbh} = $dbh;
    }
    else {
        die $DBI::errstr;
    }

    warn "debug: Connected to $dbi" if($DEBUG);

    bless($self, $class);
    return $self;
}


sub dbh {
    my $self = shift;
    return $self->{dbh};
}


sub schema {
    my $self = shift;
    return $self->{schema};
}


sub deploy {
    my $self = shift;

    foreach my $table ($self->{schema}->tables) {
        my $sth = $self->{dbh}->table_info('', '', $table->name, 'TABLE');
        if (!$sth) {
            die $DBI::errstr;
        }

        if ($sth->fetch) {
            carp 'Table '. $table->name .' already exists - not deploying';
            next;
        }

#        print $table;

        warn "debug: $table" if($DEBUG);
        if (!$self->{dbh}->do($table->sql, undef, $table->bind_values)) {
            die $self->{dbh}->errstr;
        }
        foreach my $index ($table->sql_index) {
            warn "debug: $index" if($DEBUG);
            if (!$self->{dbh}->do($index)) {
                die $self->{dbh}->errstr;
            }
        }
    }
}


sub arow {
    my $self = shift;
    return $self->{schema}->arow(@_);
}


sub do {
    my $self = shift;

    if (!@_) {
        croak 'usage: execute($query) or execute(@query)';
    }

    my $query = shift;
    my $attrs = shift;

    warn "debug: $query" if($DEBUG);
    my $rv = $self->{dbh}->do($query->sql, $attrs, $query->bind_values);
    if (!$rv) {
        croak "DBI->do: $DBI::errstr";
    }
    return $rv;
}


sub execute {
    my $self = shift;

    if (!@_) {
        croak 'usage: execute($query) or execute(@query)';
    }

    my $query = shift;
    my $attrs = shift;

    warn "debug: $query" if($DEBUG);

    my $sth;
    eval {
        $sth = $self->{dbh}->prepare($query->sql);
    };
    if ($@) {
        croak $@;
    }

    my $res;
    eval {
        $res = $sth->execute_array($attrs, $query->bind_values);
    };
    if (!$res) {
        croak $DBI::errstr;
    }

    return $sth unless(wantarray);
    return ($sth, $query->columns);
}


sub sth_to_simple_objects {
    my $self = shift;
    my ($sth, @columns) = @_;

    if (!$sth or !@columns) {
        croak 'usage: sth_to_simple_objects($sth, @columns)';
    }
    return unless($sth);

    my $class = '_' . join('_', map {$_->table->name .'_'.$_->name} @columns);

    no strict 'refs';
    if (ref(\*{$class.'::new'}{CODE}) eq 'SCALAR') {
        struct($class => [map {$_ => '$'} map {$_->name} @columns]);
    }
    use strict;

    my @returns;
    if (wantarray) {
        while (my $row = $sth->fetchrow_arrayref) {
            my $i = 0;
            my $obj = $class->new(map {$columns[$i++]->name => $_} @$row);
            push(@returns, $obj);
        }
        die $self->{dbh}->errstr if ($self->{dbh}->errstr);
        return @returns;
    }
}


sub insert {
    my $self = shift;
    return $self->do($self->{schema}->insert(@_));
}


sub update {
    my $self = shift;
    return $self->do($self->{schema}->update(@_));
}


sub delete {
    my $self = shift;
    return $self->do($self->{schema}->delete(@_));
}


sub select {
    my $self = shift;
    my $query = $self->{schema}->select(@_);
    return $self->sth_to_simple_objects($self->execute($query));
}


sub disconnect {
    my $self = shift;
    if ($self->{dbh}) {
        warn 'debug: Disconnecting from DBI' if($DEBUG);
        $self->{dbh}->disconnect;
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

SQL::DB - Easy Perl interface to SQL Database

=head1 VERSION

0.03. Development release.

=head1 SYNOPSIS

  use SQL::DB;

  my $schema = SQL::DB->schema(<database definition>);
  my $db     = SQL::DB->connect($dbi, $user, $pass, $schema);

  if (@ARGV and $ARGV[0] eq '--install') {
      $db->deploy;
  }

  my $track   = $db->arow('tracks');
  my @objects = $db->select(
      columns  => [$track->title, $track->cd->artist->name],
      where    => !($track->length < 248) & ($track->cd->year > 1997)
      order_by => [$track->title->asc],
      limit    => 5,
  );

  foreach my $obj (@objects) {
      print $obj->title, ',', $obj->name, "\n";
  }

=head1 DESCRIPTION

B<SQL::DB> provides an abstraction layer to SQL databases. It allows
you to generate and run queries using Perl constructs such as objects
and logic operators. It is not an Object Mapping Layer
(such as Class::DBI) but is also more than a pure SQL abstraction (such
as SQL::Abstract). It falls somewhere inbetween.

Because B<SQL::DB> (or rather the schema class L<SQL::DB::Schema>) makes use
of foreign key information, powerful queries can be created with minimal
effort, requiring fewer statements than if you were to write the SQL
yourself.

=head1 TUTORIAL

=head2 Schema Definition

B<SQL::DB> needs to know the structure of the database tables and
columns, and their inter-relationships (eg primary & foreign keys).
The schema is built (as defined by L<SQL::DB::Schema>) as follows.
We will use the age-old Music Album example consisting of Artists,
their CDs, and the Tracks on the CDs.
 
  my $schema = SQL::DB->schema(
    [   
        table   => 'artists',
        columns => [
            [
                name    => 'id',
                type    => 'INTEGER',      # mandatory, any SQL type
                primary => 1,              # optional
            ],
            [
                name    => 'name',
                type    => 'VARCHAR(255)',
                unique  => 1,              # optional
            ],
        ],
    ],
    [
        table   => 'cds',
        columns => [
            [
                name    => 'id',
                type    => 'INTEGER',
                primary => 1,
            ],
            [
                name    => 'artist',
                type    => 'INTEGER',
                references => 'artists(id)',
            ],
            [
                name    => 'title',
                type    => 'VARCHAR(255)',
            ],
        ],
        unique => [
            ['artist,title'],
        ],
        index => [
            columns => ['artist'],
        ],
    ],
    [
        table   => 'tracks',
        columns => [
            [
                name    => 'id',
                type    => 'INTEGER',
                primary => 1,
            ],
            [
                name    => 'cd',
                type    => 'INTEGER',
                references => 'artists(id)',
            ],
            [
                name    => 'title',
                type    => 'VARCHAR(255)',
            ],
            [
                name    => 'length',
                type    => 'INTEGER',
            ],
        ],
        unique => [
            ['cd,title'],
        ],
        index => [
            columns => ['cd'],
        ],
    ],
  );

Column definitions may also include 'null', 'unique' and 'default'
values, which which will be used at table creation time. If you want
to see the SQL generated for creating the tables you can simply
"print $schema->tables;".

The order in which the tables are defined is important, just as
if you were creating the tables in SQL. Tables with foreign
key definitions should come _after_ the table definitions they refer
to. This restriction may not be necessary in future versions.

=head2 Database Connection

Connecting to a database is basically the same as for L<DBI> with
an additional schema argument. The object returned from the connect
call is the handle to be used for all queries against the database.

  my $db = SQL::DB->connect($dbi, $user, $pass, $attrs, $schema)

=head2 Table Creation

If your tables do not already exist in the database B<SQL::DB> can
create them for you with a simple call to the deploy() method.

  $db->deploy();

It is safe to call this even if the tables do already exist. B<SQL::DB>
will emit a warning and continue.

=head2 Abstract Rows

All queries with B<SQL::DB> depend on abstract representations
of table rows. An abstract row is obtained using the arow()
method. The object returned has methods that match the columns of
a table, plus some extra methods to compare columns in an SQL-like
way.

So we obtain an object that could represent any CD and use it expressions
like this:

  my $cd    = $db->arow('cds');
  my $expr1 = ($cd->id == 1);
  my $expr2 = ($cd->title->like('%Kind of Magic%'));
  my $expr3 = ($cd->id != 1) & ($cd->artist->in(1,2,5));

Very powerful expressions can be created using this combination
of abstract rows and the Perl logic operators. More details on
this in the "EXPRESSIONS" section below.

If a table column (such as the 'cds.artist' column) references a foreign
key then you can "follow through" to reach the columns of that table
as well. So to refer to the 'artists.name' column connected to the
abstract CD row we can use "$cd->artist->name" in any expression.

  my $expr4 = ($cd->artist->name == 'Queen');

On the SQL side B<SQL::DB> automatically matches up the foreign
keys for you, so there is no need to go comparing $cd->artist
with $cd->artist->id. There are more examples of this in the
"ADVANCED EXAMPLES" section below.

=head2 Row Insertion

  my $artist = $db->arow('artists');
  $db->insert(
      columns => [$artist->id, $artist->name],
      values  => [1, 'Queens'],
  );

You do not have to specify every column for an insertion provided
of course that the table definition has appropriate DEFAULTs
or allows NULLs.

=head2 Row Updates

Updating existing rows is similar to row insertion with the
additional possibility of filtering  - ie the WHERE clause.

  my $artist = $db->arow('artists');
  $db->update(
      columns => [$artist->name],
      set     => ['Queen'],
      where   => ($artist->name == 'Queens'),
  );

=head2 Row Deletion

Row deletion works the same way although you still have to specify
a column in the 'columns' field, and SQL::DB works out which row/table
it is.

  my $artist = $db->arow('artists');
  $db->delete(
      columns => [$artist->id],
      where   => $artist->name->like('Q%')
  );

=head2 Row Selection

Selection is a slightly different case because we expect data to
be returned. A successful "select" call returns a list of objects,
whose methods match the columns retrieved from the database.

  my $artist = $db->arow('artists');
  my @objs   = $db->select(
      columns => [$artist->id, $artist->name],
      where   => ($artist->id < 3)
  );
  
  foreach my $obj (@objs) {
      print $obj->id .'='. $obj->name ."\n";
  }

=head2 Disconnection

When you are finished with the database you can disconnect.
Disconnection also happens automatically if the $db object goes
out of scope and is destroyed.

  $db->disconnect;

There are lower-level methods available for creating queries
or accessing the DBI handle directly, as described in the METHODS
section below.

=head1 ADVANCED EXAMPLES

The above is all quite ordinary and not much different from writing
the SQL statements directly. However, given that B<SQL::DB> is aware
of of inter-table relationships we can make much more powerful queries.

First of all, lets give our artist a CD and some Tracks to work with.

  my $cd = $db->arow('cds');
  $db->insert(
      columns => [$cd->id, $cd->artist, $cd->name],
      values  => [1, 1, 'A Kind of Magic'],
  );

  my $track = $db->arow('tracks');
  $db->insert(
      columns => [$track->id, $track->cd, $track->title, $track->length],
      values  => [1, 1, "Don't Lose Your Head", 278],
  );
  $db->insert(
      columns => [$track->id, $track->cd, $track->title, $track->length],
      values  => [2, 1, "Gimme the Prize", 274],
  );

Now lets do a search to find all the track titles for our Artist 'Queen',
limited to the first 5, unique tracks ordered by reverse name.

  my $track  = $db->arow('tracks');
  my @tracks = $db->select(
      columns  => [$track->id, $track->title],
      distinct => 1,
      where    => ($track->cd->artist->name == 'Queen')
      order_by => [$track->name->desc],
      limit    => 5,
  );

What happens here is that B<SQL::DB> understands the relationships
inside $track->cd->artist and builds the appropriate statements
to link those tables together based on the primary and foreign keys.

Columns that are not in the 'columns' list are simply not retrieved
and do not exist as methods in the returned object. So for the above
query trying to call 'length' on a returned object will die.

If you want to retrieve the whole row you don't have to specify every
column. Use the abstract row's _columns() method.

      columns  => [$track->_columns],

There is nothing to stop us selecting columns from different tables
in the same query. Show me the Artist names and their Albumn titles
where the tracks are longer than 276 seconds:

  my $track  = $db->arow('tracks');
  my @objs = $db->select(
      columns  => [$track->cd->artist->name, $track->cd->title],
      distinct => 1,
      where    => ($track->length > 276)
  );
  
  foreach my $obj (@objs) {
      print $obj->name, $obj->title,"\n"; # OK
      print $obj->length, "\n";           # dies - column not retrieved
  }

The limitation with this is of course that all of the column names
retrieved must be unique. It is no good selecting the 'artists.id'
and 'cds.id' columns - there is no way to differentiate between
the two using B<SQL::DB> this way. Take a look at the execute() method
to get around this.

It is possible to perform subselects by defining a query (without
running it) via the schema object, and using that query as an
expression inside another one.

  my $track = $db->arow('tracks');
  my $query = $db->schema->select(
      columns => [$track->cd->artist->id],
      where   => ($track->title == 'Gimme the Prize'),
  );

  my $artist = $db->arow('artists');
  $db->select(
      columns => [$artist->name],
      where   => ($artist->id->not_in($query)),
  );

Notice that we used two abstract rows instead of following through,
because the two queries are in fact independent from each other.

BTW this is probably not good SQL as I'm not an SQL expert, but the point
is B<SQL::DB> is powerful enough to produce what you want if you
know what you are doing. It is also powerful enough for you to shoot
yourself in the foot if you don't know what you are doing.

The final thing to remember is that using B<SQL::DB> you can only get
to foreign tables through the reference, not the other way around. Ie
there is no $cd->tracks method. I'm still having a think about if/how
this should be implemented or left to higher layers.

=head1 EXPRESSIONS

The real power of B<SQL::DB> lies in the way that WHERE
$expressions are constructed.  Abstract columns and queries are derived
from an expression class. Using Perl's overload feature they can be
combined and nested any way to very closely map Perl logic to SQL logic.

  Perl          SQL             Applies to
  ---------     -------         ------------
  &             AND             Expressions
  |             OR              Expressions
  !             NOT             Expressions
  ==            ==              Column
  like          LIKE            Column
  in            IN              Column
  not_in        NOT IN          Column
  is_null       IS NULL         Column
  is_not_null   IS NOT NULL     Column
  exists        EXISTS          Expressions
  asc           ASC             Column (ORDER BY)
  desc          DESC            Column (ORDER BY)

See L<To::Be::Written> for more details.

=head1 METHODS

=head2 schema($table1, $table2, ...)

Create a schema definition for a database containing table1, table2.
The object returned is used when connecting to a database with the
'connect' method below. The arguments $table1, $table2, etc are
references to ARRAYs as specified by L<SQL::DB::Schema>.

=head2 connect($dbi, $user, $pass, $attrs, $schema)

Connect to a database. The first four parameters are the same as for
the L<DBI>->connect call, the fifth parameter is a previously created
L<SQL::DB::Schema> object. This returns an instance of B<SQL::DB>
which you can run queries with.

=head2 dbh

Returns the L<DBI> database handle we are connected with.

=head2 schema

Returns the $schema used in the connect() call. Through this method
you can get access to the schema object (which you may not want to
keep track of yourself).

=head2 deploy

Runs the CREATE TABLE statements necessary to create the
$schema in the database. Will warn for any tables that already exist.

=head2 arow($table_name)

Returns an abstract representation of a row from table $table_name.
The methods of the abstract row are the same as the columns in the
table. The abstract row can be used in any of the insert, update,
delete or select statements below.

This is actually the arow method from L<SQL::DB::Schema>

=head2 insert(...)

Will perform an INSERT according to the arguments given and return
the number of rows affected. The arguments are the same as for
L<SQL::DB::Query::Insert>.

  columns  => [@columns],       # mandatory
  values   => [@values]         # mandatory

=head2 update(...)

Will perform an UPDATE according to the arguments given and return
the number of rows affected. The arguments are the same as for
L<SQL::DB::Query::Update>.

  columns  => [@columns],       # mandatory
  set      => [@values]         # mandatory
  where    => $expression,      # optional (but probably necessary)

=head2 select(...)

Will perform an SELECT according to the arguments given, and return
an array of objects. The arguments are the same as for L<SQL::DB::Query::Update>.

  select          => [@columns],       # mandatory
  distinct        => 1 | [@columns],   # optional
  where           => $expression,      # optional
  order_by        => [@columns],       # optional
  having          => [@columns]        # optional
  limit           => [$count, $offset] # optional

=head2 delete(...)

Will perform a DELETE according to the arguments given and return
the number of rows affected. The arguments are the same as
for L<SQL::DB::Query::Update>.

  columns  => [@arows],          # mandatory
  where    => $expression       # optional (but probably necessary)

=head1 INTERNAL METHODS

Documented here for completness.

=head2 do($query)

Takes a query object (one of SQL::DB::Query::Insert, SQL::DB::Query::Update,
or SQL::DB::Query::Delete) and runs the query against the database returning
the result. Dies if an error occurs.

=head2 execute($query)

Takes a SQL::DB::Query::Select object, runs it against the database and
returns the DBI statement handle and the list of SQL::DB::Column objects
that were retrieved.

=head2 sth_to_simple_objects($sth, @columns)

Creates an object class specific to this query, with getter/setter
methods set to the names of the columns. Returns a list of those objects.

=head1 CAVEATS

Because L<SQL::DB::Schema> is very simple it will create what it
is asked to without knowing or caring if the statements are suitable for
the target database. If you need to produce SQL which makes use of
non-portable database specific statements you will need to create your
own layer above B<SQL::DB> for that purpose.

=head1 TODO

COUNT() statements. This module needs more exposure/use to find
out what is missing.

=head1 DEBUGGING

If $SQL::DB::DEBUG is set to a true value then SQL
queries and other important actions are 'warn'ed to STDERR

=head1 SEE ALSO

L<SQL::Abstract>, L<SQL::Builder>, L<Class::DBI>, L<Tangram>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

=cut

