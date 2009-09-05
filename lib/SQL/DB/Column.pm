package SQL::DB::Column;
use Mouse;
use Carp qw(carp croak);
#use overload '""' => 'name', fallback => 1;

has 'table' => (
    is => 'rw',
    isa => 'SQL::DB::Table',
    weak_ref => 1,
);

has 'name' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has '_type' => (
    is => 'ro',
#    isa => 'Str|HashRef[Str]',
    isa => 'Str|HashRef',
    default => 0,
    init_arg => 'type',
);
sub type            { _multival(shift, 'type', @_ ? @_ : ()) };

has '_bind_type' => (
    is => 'ro',
#    isa => 'Str|HashRef[Str]',
    isa => 'Str|HashRef',
    default => 0,
    init_arg => 'bind_type',
);
sub bind_type       { _multival(shift, 'bind_type', @_) };

has 'primary' => (
    is => 'rw',
    isa => 'Bool',
    default => 0,
);

has 'null' => (
    is => 'ro',
    isa => 'Bool',
    default => 0,
);

has 'default' => (
    is => 'ro',
    isa => 'Any',
);

has '_references' => (
    is => 'rw',
    isa => 'Undef|Str',
);

has 'references' => (
    is => 'rw',
    isa => 'SQL::DB::Column',
    weak_ref => 1,
);

has '_deferrable' => (
    is => 'ro',
#    isa => 'Str|HashRef[Bool]',
    isa => 'Bool|HashRef',
    default => 0,
    init_arg => 'deferrable',
);
sub deferrable      { _multival(shift, 'deferrable', @_) };

has 'unique' => (
    is => 'ro',
    isa => 'Bool',
    default => 0,
);

has '_auto_increment' => (
    is => 'ro',
#    isa => 'Bool|HashRef[Bool]',
    isa => 'Bool|HashRef',
    default => 0,
    init_arg => 'auto_increment',
);
sub auto_increment  { _multival(shift, 'auto_increment', @_) };

has 'set' => (
    is => 'ro',
    isa => 'CodeRef',
);

has 'inflate' => (
    is => 'ro',
    isa => 'CodeRef',
);

has 'deflate' => (
    is => 'ro',
    isa => 'CodeRef',
);


sub _multival {
    my $self = shift;
    my $key  = '_'. shift;

    if (@_) {
        $self->$key(shift);
        return;
    }

    my $val = $self->$key;

    my $type = $self->table->dbd || confess "Table DBD type not yet defined";
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


sub _sql_default {
    my $self = shift;
    my $default = $self->default;

    return '' if ( ! defined $default || ref $default eq 'CODE' );

    if ($self->type =~ m/(int)|(real)|(float)|(double)|(num)/i) {
        return ' DEFAULT ' . $default
    }
    return " DEFAULT '" . $default ."'";
}


sub _sql_deferrable {
    my $self = shift;
    my $def  = $self->_multival('deferrable');
    return '' unless ( defined $def );

    return $def ? ' DEFERRABLE' : ' NOT DEFERRABLE';
}


sub as_sql {
    my $self = shift;

    return sprintf('%-15s %-15s', $self->name, $self->type)
           . ($self->null ? 'NULL' : 'NOT NULL')
           . $self->_sql_default
           . ($self->auto_increment ? ' AUTO_INCREMENT' : '')
    ;
}


1;
__END__

=head1 NAME

SQL::DB::Column - Perl representation of an SQL database column

=head1 SYNOPSIS

  use SQL::DB::Column;
  use DBI qw(SQL_BLOB);

  my $col = SQL::DB::Column->new(
      table => $table,
      name  => 'bincol',
      type  => {
          SQLite => 'BLOB',
          Pg     => 'BYTEA' 
      },
      bind_type => {
          SQLite => SQL_BLOB,
          Pg => {
              pg_type => eval { require DBD::Pg; DBD::Pg::BYTEA };
          },
      },
      null => 0,
      ...
  );

  print $col->as_sql;
  # bincol BLOB - when using DBD::SQLite
  # bincol BYTEA - when using DBD::Pg

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::Column> is used by L<SQL::DB>
to represent a database column definition.  Most users will only use
this module indirectly through L<SQL::DB>.

Several of the attributes are multivariate, allowing you to specify
different values depending on what DBD driver is used. If set
be a scalar then that value will be used for all driver types. If set
to a hashref, then the keys inside the hash determine which value is
used for which driver.

=head1 CONSTRUCTOR

The new() constructor takes key/value pairs matching the attribute
names. The following are required: table, name, type.

=head1 ATTRIBUTES

=over 4

=item table

A weak reference to the L<SQL::DB::Table> object 'containing' this column.

=item name

The column name.

=item type

The column name. Multivariate.

=item bind_type

The bind type DBI will use for placeholders. Multivariate.

=item primary

A boolean indicating if the column is primary.

=item null

A boolean indicating if the column can hold NULL values.

=item default

The default value for the column.

=item references

A foreign key constraint.

=item deferrable

A boolean indicating whether the foreign key constraint is deferrable.

=item unique

A boolean indicating if the column is unique.

=item auto_increment

A boolean indicating if the column is auto incrementing. Multivariate.

=item set

A code reference to be run when SQL::DB::Row->set_column($val) is
called. The subref has access to the row object and all of its columns,
but must return the value for the column and not set it.

=item inflate

A code reference to run after SELECT.

=item defalte

A code reference to run before INSERT or UPDATE.

=back

=head1 METHODS

=over 4

=item as_sql

Returns the SQL representation of this column.

=back

=head1 SEE ALSO

L<SQL::DB::Table>, L<SQL::DB>

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
