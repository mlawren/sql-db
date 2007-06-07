package SQL::API;
use 5.006;
use strict;
use warnings;
use Carp qw(carp croak);

use SQL::API::Table;
use SQL::API::Insert;
use SQL::API::Select;
#use SQL::API::Update;
#use SQL::API::Delete;

our $VERSION = '0.01';


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        tables => {},
    };
    bless($self,$class);

    my @items;
    if (!ref($_[0])) {
         @items = @_;
    }
    elsif (ref($_[0]) eq 'ARRAY') {
         my $ref = shift;
         @items = @{$ref};
    }
    else {
        die 'new requires an array or arrayref'
    }

    while (@items) {
        my $name = shift @items;
        my $def  = shift @items;
        $self->define($name, $def);
    }
    return $self;
}


sub define {
    my $self       = shift;
    my $name       = shift;
    my $definition = shift;

    if (!$name or ref($definition) ne 'HASH') {
        croak 'usage: define($name, $hashref)';
    }

    if (exists($self->{tables}->{$name})) {
        warn "Redefining table '$name'";
    }

    $self->{tables}->{$name} = SQL::API::Table->new($name, $definition, $self);
    return $self->{tables}->{$name};
}


sub table {
    my $self = shift;
    my $name  = shift;

    if (!$name) {
        croak 'usage: table($name)';
    }

    if (!exists($self->{tables}->{$name})) {
        croak "Table '$name' has not been defined";
    }
    return $self->{tables}->{$name};
}


sub row {
    my $self = shift;
    my $name = shift;
    if (!$name) {
        croak 'usage: row($name)';
    }
    if (!exists($self->{tables}->{$name})) {
        croak "Table '$name' has not been defined";
    }
    
    return $self->{tables}->{$name}->abstract_row;
}


sub query {
    my $self = shift;

    my $def;
    if (ref($_[0]) and ref($_[0]) eq 'HASH') {
        $def = shift;
    }
    else {
        my %defs = @_;
        $def = \%defs;        
    }
#use Data::Dumper;
#$Data::Dumper::Indent=1;

#die ref($def);
    if (exists($def->{insert})) {
        return SQL::API::Insert->new($def);
    }
    elsif (exists($def->{select})) {
        return SQL::API::Select->new($def);
    }
#    elsif (exists($def->{update})) {
#        return SQL::API::Update->new($def);
#    }
#    elsif (exists($def->{delete})) {
#        return SQL::API::Delete->new($def);
#    }

    croak 'query badly defined (missing select,insert,update etc)';
}


1;
__END__

=head1 NAME

SQL::API - Create SQL statements using Perl objects and logic

=head1 SYNOPSIS

  use SQL::API;

  my $schema = SQL::API->new('CD' => {...}, 'Artist' => {...});
  my $cd     = $schema->row('CD');
  my $artist = $schema->row('Artist');

  my $query = $schema->query(
      select => [
          $cd->_columns,
          $artist->id,
          $artist->name
      ],
      where => (
          ($cd->artist == $artist->id) &
          ($artist->name == 'Queen') &
          ($cd->year > 1997)
      ),
      order_by => [
          $cd->year->desc,
          $cd->title->asc
      ],
  );

  my $sth = $dbi->prepare($query->sql);
  $sth->execute($query->bind_values);

  foreach ($sth->rows) {
    ...
  }

=head1 DESCRIPTION

B<SQL::API> is a module for producing SQL statements using a combination
of Perl objects, methods and logic operators such as '!', '&' and '|'.

B<SQL::API> is not an object <-> database mapping tool as it only
generates SQL, but it would make a good base for such a system. You can
think of B<SQL::API> in the same category as L<SQL::Builder>
and L<SQL::Abstract> with the ability to easily create complex/nested
queries.

As B<SQL::API> is very simple it will create what it is asked
to without knowing or caring if the statements are suitable for the
target database. If you need to produce SQL which makes use of
database specific statements which are not portable I suggest you
create your own layer above this module for that purpose.

If you actually want to do something with the SQL generated you need
to have L<DBI> and the appropriate DBD:* module for your database installed.

=head1 METHODS

=head2 new(\@schema)

Create a new SQL::API object to hold the table schema.
\@schema (a reference to an ARRAY) must be a list of
('Table' => {...}) pairs representing tables and their
column definitions.

    my $def    = ['Users' => {columns => [{name => 'id'}]}];
    my $schema = SQL::API->new($def);

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
    }

L<SQL::API::Schema> for further details.

=head2 table( )

Return a list of SQL::API::Table objects representing the database
tables. The CREATE TABLE statements are available via the 'sql' method,
and the bind values (usually only from DEFAULT parameters) are returned
by the 'bind_values' method. These are then suitable for using directly in
L<DBI> calls.

So a typical database installation would go like this:

    my $schema = SQL::API->new(@schema);
    my $dbi    = DBI->connect(...);

    foreach my $t ($schema->table) {
        $dbi->do($t->sql, {}, $t->bind_values);    
    }

You can also call table with a single 'Table' argument to return
a single object.

The object can also be queried for details about the names of the columns
but is otherwise not very useful. See L<SQL::API::Table>
for more details.

=head2 row('Table')

Returns an abstract representation of a row from 'Table' for
use in all query types. This object has methods for each column
plus a '_columns' method which returns all columns. These objects
are the workhorse of the whole system.

As an example, if a table 'DVDs' has been defined with columns 'id',
'title' and 'director' and you create an abstract row using

    my $dvd = $schema->row('DVDs')
    
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

See L<SQL::API::ARow> for more details.

=head2 query(key => value, key => value, key => value, ...)

Returns an SQL::API::Query based object representing an SQL query and
its associated bind values. The SQL text is available via the 'sql'
method, and the bind values are returned in a list by the 'bind_values'
method. These are then suitable for using directly in L<DBI> methods.

The type of query and its parameters are defined according to the
key/value pairs as follows.

=head3 INSERT

  insert   => [@columns],       # mandatory
  values   => [@values]         # mandatory

=head3 SELECT

  select   => [@columns],       # mandatory
  distinct => 1 | [@columns],   # optional
  where    => $expression,      # optional
  order_by => [@columns],       # optional
  having   => [@columns]        # optional
  limit    => [$count, $offset] # optional

=head3 UPDATE

  update   => [@columns],       # mandatory
  where    => $expression,      # optional (but probably necessary)
  values   => [@values]         # mandatory

=head3 DELETE

  delete   => [@rows],          # mandatory
  where    => $expression       # optional (but probably necessary)

=head1 EXPRESSIONS

The real power of B<SQL::API> lies in the way that the WHERE
$expression is constructed.  Abstract columns and queries are derived
from an expression class. Using Perl's overload feature they can be
combined and nested any way to directly map Perl logic to SQL logic.

See L<SQL::API::Query> for more details.

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

