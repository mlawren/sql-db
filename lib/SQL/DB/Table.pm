package SQL::DB::Table;
use Mouse;
use Carp qw(carp croak);
use SQL::DB::Column;
use SQL::DB::Index;
use SQL::DB::Row;
use SQL::DB::ARow;

our $VERSION = '0.18';


has 'schema' => (
    is => 'ro',
    isa => 'SQL::DB::Schema',
    required => 1,
    weak_ref => 1,
    handles => [qw/dbd/],
);

has 'name' => (
    is => 'rw',
    isa => 'Str',
    required => 1,
);

has 'class' => (
    is => 'rw',
    isa => 'Str',
    lazy => 1,
    builder => sub { SQL::DB::Row->make_class_from( shift->columns ) },
    init_arg => undef,
);

has 'aclass' => (
    is => 'rw',
    isa => 'Str',
    lazy => 1,
    builder => sub { SQL::DB::ARow->make_class_from( shift->columns ) },
    init_arg => undef,
);

has 'columns' => (
    is => 'ro',
#    isa => 'ArrayRef[SQL::DB::Column]',
    isa => 'ArrayRef',
    required => 1,
    auto_deref => 1,
);

has '_columns' => (
    is => 'ro',
#    isa => 'HashRef[SQL::DB::Column]',
    isa => 'HashRef',
    required => 1,
    auto_deref => 1,
);

has 'primary' => (
    is => 'ro',
#    isa => 'Str|ArrayRef[SQL::DB::Column]',
    isa => 'ArrayRef',
    default => sub { [] },
    auto_deref => 1,
);

has '_primary' => (
    is => 'ro',
#    isa => 'HashRef[SQL::DB::Column]',
    isa => 'HashRef',
    default => sub { {} },
    auto_deref => 1,
);

has 'unique' => (
    is => 'rw',
#    isa => 'ArrayRef[SQL::DB::Column]',
    isa => 'ArrayRef',
    default => sub { [] },
    auto_deref => 1,
);

has 'indexes' => (
    is => 'rw',
#    isa => 'ArrayRef[SQL::DB::Index]',
    isa => 'ArrayRef',
    default => sub { [] },
    auto_deref => 1,
);

has 'sequences' => (
    is => 'rw',
    isa => 'ArrayRef[SQL::DB::Sequence]',
#    isa => 'ArrayRef',
    default => sub { [] },
    auto_deref => 1,
);

has 'ref_by' => (
    is => 'ro',
#    isa => 'HashRef[SQL::DB::Table]', # don't forget to weaken this??
    isa => 'HashRef',
    default => sub { {} },
    auto_deref => 1,
    init_arg => undef,
);

# A kind of a holding/waiting area for references that we can't resolve
# until all tables have been defined.
has '_foreign' => (
    is => 'rw',
    isa => 'ArrayRef[HashRef]',
    default => sub { [] },
    auto_deref => 1,
    init_arg => undef,
);

has 'foreign' => (
    is => 'rw',
#    isa => 'ArrayRef[SQL::DB::FK]',
    isa => 'ArrayRef',
    default => sub { [] },
    auto_deref => 1,
);

has '_type' => (
    is => 'ro',
    isa => 'Str|HashRef',
    init_arg => 'type',
);
sub type            { _multival(shift, 'type', @_ ) };

has '_engine' => (
    is => 'ro',
    isa => 'Str|HashRef',
    init_arg => 'engine',
);
sub engine          { _multival(shift, 'engine', @_ ) };

has '_default_charset' => (
    is => 'ro',
    isa => 'Str|HashRef',
    init_arg => 'default_charset',
);
sub default_charset { _multival(shift, 'default_charset', @_ ) };

has '_triggers' => (
    is => 'ro',
    isa => 'ArrayRef',
    default => sub { [] },
    init_arg => 'triggers',
    auto_deref => 1,
);

sub triggers {
    my $self = shift;

    if ( @_ ) {
        $self->_triggers( @_ );
    }

    if ( wantarray ) {
        my $dbd = $self->schema->dbd;

        return map {
            if ( ref( $_) eq 'HASH' ) {
                return exists $_->{$dbd} ? $_->{$dbd} : ();
            }
            return $_;
        } $self->_triggers;
    }

    return $self->_triggers;
}


sub _multival {
    my $self = shift;
    my $key  = '_'. shift;

    return $self->$key(shift) if ( @_ );

    my $val  = $self->$key;
    my $type = $self->schema->dbd;

    if ( ref $val eq 'HASH' ) {
        if ( exists $val->{$type} ) {
            return $val->{$type};
        }
        else {
            warn "No $type definition for $key";
            return;
        }
    }

    return $val;
}


sub column_names {
    my $self = shift;
    return values %{ $self->_columns };
}


sub column {
    my $self = shift;
    my $name = shift;
    if ( ! exists $self->_columns->{$name} ) {
        confess "Column '$name' not defined in table: ".$self->name;
    }
    return $self->_columns->{$name};
}


sub primary_names {
    my $self = shift;
    return keys %{ $self->_primary };
}


sub row {
    my $self   = shift;
    my $class  = $self->class;
    return $class->new( @_ );
}


sub arow {
    my $self   = shift;
    my $aclass = $self->aclass;
    return $aclass->new;
}


sub _sql_primary {
    my $self = shift;
    my @cols = $self->primary;

    return unless( @cols );
    return 'PRIMARY KEY(' . join( ', ', map { $_->name } @cols ) .')';
}


sub _sql_unique {
    my $self = shift;
    my @unique = $self->unique;

    return unless( @unique );

    my @sql = ();

    # a list of arrays
    foreach my $u ( @unique ) {
        push( @sql, 'UNIQUE (' . join( ', ', map { $_->name } @$u ) . ')' );
    }

    return @sql;
}


sub _sql_engine {
    my $self = shift;
    my $engine = $self->engine;
    return '' unless( $engine );
    return ' ENGINE='.$engine;
}


sub _sql_default_charset {
    my $self = shift;
    my $charset = $self->default_charset;
    return '' unless( $charset );
    if ( $self->schema->dbd eq 'mysql') {
        return ' DEFAULT CHARACTER SET '.$charset;
    }
    else {
        return ' DEFAULT_CHARSET '.$charset;
    }
}


sub _sql_create_table {
    my $self = shift;

    if ( @{ $self->_foreign } ) {
        require Data::Dumper;
        local $Data::Dumper::Indent = 1;
        local $Data::Dumper::Maxdepth=2;
        croak "Schema(".$self->schema->name .
            ").".$self->name .
            " cannot generate SQL unless foreign keys resolved".
            Data::Dumper::Dumper($self->_foreign);
    }

    my @vals = map { $_->as_sql } $self->columns;
    push(@vals, $self->_sql_primary);
    push(@vals, $self->_sql_unique);
    push(@vals, map { $_->as_sql } $self->foreign);

    return 'CREATE TABLE '
           . $self->name
           . " (\n    " . join(",\n    ", @vals) . "\n)"
           . $self->_sql_engine
           . $self->_sql_default_charset
    ;
}


sub _sql_triggers {
    my $self = shift;
    return () unless($self->{triggers});

    my $dbd = $self->schema->dbd;
    my @triggers;

    foreach my $trigger (@{$self->{triggers}}) {
        next unless(exists $trigger->{$dbd});
        push(@triggers, $trigger->{$dbd});
    }

    return @triggers;
}


sub as_sql {
    my $self = shift;

    if ( wantarray ) {
        return (
            $self->_sql_create_table,
            (map { $_->as_sql } $self->indexes),
            $self->triggers,
        );
    }

    return join( ";\n",
        $self->_sql_create_table,
        (map { $_->as_sql } $self->indexes),
        $self->triggers,
    ) . ";\n";
}


my @reserved = qw(
    sql
    sql_index
    asc
    desc
    is_null
    not_null
    is_not_null
    exists
); 



1;
__END__

=head1 NAME

SQL::DB::Table - Perl representation of an SQL database table

=head1 SYNOPSIS

  use SQL::DB::Table;

  my $table = SQL::DB::Table->new(
    schema => $schema,
    table => 'cities',
    columns => [
        { name => 'id',   type => 'int', },
        { name => 'name', type => 'text', },
        { name => 'countrycode',  type => 'char(3)' },
    ],
    primary => 'id',
    indexes => [
        { columns => 'countrycode', using => 'btree' },
    ],
    foreign => [
        { columns => 'countrycode', references => 'countries(code)' },
    ],
    unique => [
        { columns => 'countrycode,name' },
    ],
  );

  print $table->as_sql;
  # CREATE TABLE cities (
  #     id              int            NOT NULL,
  #     name            text           NOT NULL,
  #     countrycode     char(3)        NOT NULL,
  #     UNIQUE (countrycode, name),
  #     FOREIGN KEY (countrycode) REFERENCES countries(code)
  # );
  # CREATE INDEX cities_countrycode ON cities (countrycode);

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::Table> is used by L<SQL::DB>
to represent a database table definition.  Most users will only use
this module indirectly through L<SQL::DB>.

Some of the attributes are multivariate, allowing you to specify
different values depending on what DBD driver is used. If set
be a scalar then that value will be used for all driver types. If set
to a hashref, then the keys inside the hash determine which value is
used for which driver.

=head1 CONSTRUCTOR

The new() constructor takes key/value pairs matching the attribute
names. The following are required: schema, table (maps to 'name'), columns.

=head1 ATTRIBUTES

=over 4

=item schema

A weak reference to the L<SQL::DB::Schema> object 'containing' this
table.

=item name

The table name. Taken from the 'table' constructor value.

=item columns

An array of L<SQL:::DB::Column> objects, based on the array of
hashrefs passed in to the new() constructor.

=item primary

An array of L<SQL:::DB::Column> objects which are primary keys of
the table.

=item unique

An array of an array of L<SQL:::DB::Column> objects, matching the
defined unique contraints.

=item indexes

An array of L<SQL:::DB::Index> objects, based on the array of
hashrefs passed in to the new() constructor.

=item sequences

An array of L<SQL:::DB::Sequence> objects, based on the array of
hashrefs passed in to the new() constructor.

=item ref_by

In array context returns a list of L<SQL::DB::Table> objects which have
columns with foreign keys referencing this table. In scalar context a
hashref of L<SQL:::DB::Table> objects that have foreign keys pointing
to this table.

=item foreign

An array of L<SQL:::DB::FK> foreign key objects. Specified as an array
of hashrefs passed in to the constructor, and also derived from
individual column definitions.

=item type => $type

SQL table type. Multivariate - typically only specified for PostgreSQL.

    type => {Pg => $type}

=item engine => $engine

The SQL backend engine type. Multivariate - typically only specified
for MySQL.

    engine => {mysql => $engine}

=item default_charset => $charset

The SQL default character set. Multivariate - typically only specified
for MySQL.

    default_charset => {mysql => $charset}

=item tablespace => $tspace

The tablespace definition. Typically only specified for PostgreSQL.

    tablespace => {Pg => $tspace}

=item triggers => @triggers

In array context is a list of SQL statements run against the database
after the CREATE TABLE and CREATE INDEX statements. Do not necessarily
have to be CREATE TRIGGER statements - in fact any valid SQL will run.

This attribute is multivariate. Eg, the following trigger specification
will provide a single trigger for each of the SQLite, Pg, and mysql DBD
types:

    triggers => [
        { SQLite => $sql1, Pg => $sql2 },
        { mysql => $sql3 },
    ]

=back

=head1 METHODS

=over 4

=item column_names -> @names

Returns the list of column names.

=item column($name) -> SQL::DB::Column

Returns the L<SQL::DB::Column> object matching $name.

=item primary_names -> @names

Returns the list of primary column names.

=item row -> SQL::DB::Row

Returns a new empty/default object representing a single row of the
table. Such an object can be inserted into a database by L<SQL::DB>.

=item arow -> SQL::DB::ARow

Returns an object representing any row of the table. This abstraction
object can be used with the L<SQL::DB> 'fetch' and 'do' methods.

=item as_sql -> @list | $str

In array context returns a list of SQL CREATE TABLE / INDEX / TRIGGER /
SEQUENCE statements. In scalar context returns a string containing the
same list of statements joined together by ";\n";

=back

=head1 SEE ALSO

L<SQL::DB::Column>, L<SQL::DB::Index>, L<SQL::DB::FK>, L<SQL::DB>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2007-2009 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.


=cut

# vim: set tabstop=4 expandtab:

