package SQL::DB::Schema;
use strict;
use warnings;
use base qw(Exporter);
use Carp qw(carp croak confess);

use SQL::DB::Table;
use SQL::DB::Query;
use SQL::DB::Function;

our $VERSION = '0.05';
our $DEBUG;
our @EXPORT_OK = @SQL::DB::Function::EXPORT_OK;

foreach (@EXPORT_OK) {
    no strict 'refs';
    *{$_} = *{'SQL::DB::Function::'.$_};
}

use Data::Dumper;
$Data::Dumper::Indent = 1;


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        sqldbs_tables      => [],
        sqldbs_table_names => {},
    };
    bless($self,$class);

    foreach (@_) {
        unless (ref($_) and ref($_) eq 'ARRAY') {
            croak 'usage: new($arrayref, $arrayref,...)'
        }
        $self->define($_);
    }
    return $self;
}


sub define {
    my $self = shift;

    foreach my $def (@_) {
        unless (ref($def) and ref($def) eq 'ARRAY') {
            croak 'usage: define($arrayref,...)';
        }

        my $table = SQL::DB::Table->new(schema => $self, @{$def});

        if (exists($self->{sqldbs_table_names}->{$table->name})) {
            croak "Table ". $table->name ." already defined";
        }

        push(@{$self->{sqldbs_tables}}, $table);
        $self->{sqldbs_table_names}->{$table->name} = $table;
    }
    return;
}


sub table {
    my $self = shift;
    my $name  = shift;

    if ($name) {
        if (!exists($self->{sqldbs_table_names}->{$name})) {
            confess "Table '$name' has not been defined";
        }
        return $self->{sqldbs_table_names}->{$name};
    }
    confess 'usage: table($name)';
}


sub tables {
    my $self = shift;
    my $name  = shift;

    return @{$self->{sqldbs_tables}};
}


sub query {
    my $self = shift;
    return SQL::DB::Query->new(@_);
}

1;
__END__

=head1 NAME

SQL::DB::Schema - Generate SQL using Perl logic and objects

=head1 VERSION

0.05. Development release.

=head1 SYNOPSIS

  use SQL::DB::Schema;
  use DBI;

  my $dbh = DBI->connect("dbi:SQLite:/tmp/sqlite$$.db");

  my $schema = SQL::DB::Schema->new(
    [
        table => 'artists',
        class => 'Artist',
        columns => [
            [name => 'id',  type => 'INTEGER', primary => 1],
            [name => 'name',type => 'VARCHAR(255)',unique => 1],
        ],
        unique => 'name',
        index  => [
            columns => 'name',
            unique  => 1,
        ],
    ],
    [
        table => 'cds',
        class => 'CD',
        columns => [
            [name => 'id', type => 'INTEGER', primary => 1],
            [name => 'title', type => 'VARCHAR(255)'],
            [name => 'year', type => 'INTEGER'],
            [name => 'artist', type => 'INTEGER', references => 'artists(id)'],
        ],
        unique  => 'title,artist',
        index   => [
            columns => 'title',
        ],
        index  => [
            columns => 'artist',
        ],
    ],
  );


  foreach my $t ($schema->tables) {
    $dbh->do($t->sql);
    foreach my $index ($t->sql_index) {
        $dbh->do($index);
    }
  }

  # CREATE TABLE artists (
  #     id              INTEGER        NOT NULL,
  #     name            VARCHAR(255)   NOT NULL UNIQUE,
  #     PRIMARY KEY(id),
  #     UNIQUE (name)
  # )
  # CREATE TABLE cds (
  #     id              INTEGER        NOT NULL,
  #     title           VARCHAR(255)   NOT NULL,
  #     year            INTEGER        NOT NULL,
  #     artist          INTEGER        NOT NULL REFERENCES artists(id),
  #     PRIMARY KEY(id),
  #     UNIQUE (title, artist)
  # )
  # CREATE INDEX cds_title ON cds (title)
  # CREATE INDEX cds_artist ON cds (artist)

  my $artist = Artist->arow; # or Artist::Abstract->new;
  my $cd     = CD->arow;     # or CD::Abstract->new;

  my $query  = $schema->query(
    insert => [$artist->id, $artist->name],
    values => [1, 'Queen'],
  );

  $dbh->do($query->sql, undef, $query->bind_values);

  my $query = $schema->select(
      columns  => [ $track->cd->title, $track->cd->artist->name ],
      distinct => 1,
      where    => ( $track->length > 248 ) & ! ($track->cd->year > 1997),
      order_by => [ $track->cd->year->desc ],
  );

  print $query,"\n";

  my $sth = $dbi->prepare($query->sql);
  $sth->execute($query->bind_values);

  foreach ($sth->rows) {
    ...
  }

=head1 DESCRIPTION

B<SQL::DB::Schema> is a module for producing SQL statements using a
combination of Perl objects, methods and logic operators such as '!',
'&' and '|'.  You can think of B<SQL::DB::Schema> in the same
category as L<SQL::Builder> and L<SQL::Abstract> but with extra
abilities.

B<SQL::DB::Schema> doesn't actually do much of the work itself, but
glues together various other SQL::DB::* modules.

Because B<SQL::DB::Schema> is very simple it will create what it is asked
to without knowing or caring if the statements are suitable for the
target database. If you need to produce SQL which makes use of
non-portable database specific statements you will need to create your
own layer above B<SQL::DB::Schema> for that purpose.

You probably don't want to B<SQL::DB::Schema> directly unless you
are writing an Object Mapping Layer or need to produce SQL offline.
If you need to talk to a real database you are much better off
interfacing with L<SQL::DB>.

=head1 METHODS

=head2 new(\@schema)

Create a new SQL::DB::Schema object to hold the table schema.
\@schema (a reference to an ARRAY) must be a list of
('Table' => {...}) pairs representing tables and their
column definitions.

    my $def    = ['Users' => {columns => [{name => 'id'}]}];
    my $schema = SQL::DB::Schema->new($def);

The table definition can include almost anything you can think of
using when creating a table. The following example (while overkill and
not accepted by any database) gives a good overview of what is possible.

Note that there is more than one place to define some items (for example
PRIMARY KEY and UNIQUE). Which you should use is up to you and
your database backend.

    'Artist' => {
        columns => [
            {   name           => 'id',
                type           => 'INTEGER',
                auto_increment => 1,
                primary        => 1,
            },
            {   name => 'name',
                type => 'VARCHAR(255)',
                unique => 1,
            },
            {   name => 'age',
                type => 'INTEGER',
            },
            {   name => 'label',
                type => 'VARCHAR(255)',
            },
            {   name => 'wife',
                type => 'INTEGER',
            },
        ],
        primary =>  [qw(id)],
        unique  =>  [qw(name age)],
        indexes => [
            {
                columns => ['name 10 ASC'],
            },
            {
                columns => [qw(name age)],
                unique => 1,
                using => 'BTREE',
            },
        ],
        foreign => [
            {
                columns  => [qw(wife)],
                references  => ['Wives(id)'],
            },
            {
                columns  => [qw(name label)],
                references  => ['Labels(id,label)'],
            },
        ],
        engine          => 'InnoDB',
        default_charset => 'utf8',
    }

Also note that the order in which the tables are defined matters
when it comes to foreign keys. See a good SQL book or Google for why.

=head2 tables( )

Return a list of objects representing the database
tables. The CREATE TABLE statements are available via the 'sql' and
'sql_index' methods, and the bind values (usually only from DEFAULT
parameters) are returned in a list by the 'bind_values' method. These are
suitable for using directly in L<DBI> calls.

So a typical database installation might go like this:

    my $schema = SQL::DB::Schema->new(@schema);
    my $dbi    = DBI->connect(...);

    foreach my $t ($schema->table) {
        $dbi->do($t->sql, {}, $t->bind_values);    
        foreach my $i ($t->sql_index) {
            $dbi->do($i);    
        }
    }

The returned objects can also be queried for details about the names
of the columns but is otherwise not very useful. See L<SQL::DB::Table>
for more details.

=head2 table('Table')

Returns an object representing the database table 'Table'. Also see
L<SQL::DB::Table> for more details.

=head2 arow('Table')

Returns an abstract representation of a row from 'Table' for
use in all query types. This object has methods for each column
plus a '_columns' method which returns all columns. These objects
are the workhorse of the whole system.

As an example, if a table 'DVDs' has been defined with columns 'id',
'title' and 'director' and you create an abstract row using

    my $dvd = $schema->arow('DVDs')
    
then the following are equivalent:

    my $q  = $schema->query(
        select => [$dvd->_columns]
    );
    my $q  = $schema->query(
        select => [$dvd->id, $dvd->title, $dvd->director]
    );

Now if 'director' happens to have been defined as a foreign key
for the 'id' column of a 'Directors' table ('id','name') then you can also
do the following:

    my $q  = $schema->query(
        select => [$dvd->title],
        where  => $dvd->director->name == 'Spielberg'
    );

See L<SQL::DB::ARow> for more details.

=head2 query(key => value, key => value, key => value, ...)

Returns an object representing an SQL query and
its associated bind values. The SQL text is available via the 'sql'
method, and the bind values are returned in a list by the 'bind_values'
method. These are then suitable for using directly in L<DBI> methods.

The type of query and its parameters are defined according to the
key/value pairs as follows.

=head3 INSERT

  insert   => [@columns],       # mandatory
  values   => [@values]         # mandatory

=head3 SELECT

  select          => [@columns],       # mandatory
  distinct        => 1 | [@columns],   # optional
  join            => $arow,             # optional
  where           => $expression,      # optional
  order_by        => [@columns],       # optional
  having          => [@columns]        # optional
  limit           => [$count, $offset] # optional

=head3 UPDATE

  update   => [@columns],       # mandatory
  where    => $expression,      # optional (but probably necessary)
  values   => [@values]         # mandatory

=head3 DELETE

  delete   => [@arows],          # mandatory
  where    => $expression       # optional (but probably necessary)

Note: 'from' is not needed because the table information is already
associated with the columns.

See L<SQL::DB::Query>, L<SQL::DB::Query::Insert>, L<SQL::DB::Query::Select>,...

=head1 EXPRESSIONS

The real power of B<SQL::DB::Schema> lies in the way that the WHERE
$expression is constructed.  Abstract columns and queries are derived
from an expression class. Using Perl's overload feature they can be
combined and nested any way to directly map Perl logic to SQL logic.

See L<SQL::DB::Query> for more details.

=head1 INTERNAL METHODS

These are used internally but are documented here for completeness.

=head2 define('Table' => {..definition...})

Create the representation of table 'Table' according to the schema
in {...definition...}. Each table can only be defined once.

=head1 SEE ALSO

L<SQL::Builder>, L<SQL::Abstract>

L<Tangram> has some good examples of the query syntax possible using
Perl logic operators.

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

Feel free to let me know if you find this module useful.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
