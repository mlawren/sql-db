package SQL::DB::Index;
use Mouse;

our $VERSION = '0.18';

has 'table' => (
    is => 'ro',
    isa => 'SQL::DB::Table',
    required => 1,
    weak_ref => 1,
);

has 'columns' => (
    is => 'ro',
#    isa => 'Str|ArrayRef[SQL::DB::Column]',
    isa => 'ArrayRef',
    required => 1,
    auto_deref => 1,
);

has 'unique' => (
    is => 'ro',
    isa => 'Bool',
    default => 0,
);

has '_using' => (
    is => 'ro',
#    isa => 'Str|HashRef[Str]',
    isa => 'Str|HashRef',
    init_arg => 'using',
);

has '_tablespace' => (
    is => 'ro',
#    isa => 'Str|HashRef[Str]',
    isa => 'Str|HashRef',
    init_arg => 'tablespace',
);


sub tablespace { _multival(shift, 'tablespace', @_) };
sub using { _multival(shift, 'using', @_) };

sub _multival {
    my $self = shift;
    my $key  = '_'. shift;

    if (@_) {
        $self->$key(shift);
        return;
    }

    my $val = $self->$key;

    my $dbd = $self->table->dbd;
    if ( ref $val eq 'HASH' ) {
        if ( exists $val->{$dbd} ) {
            return $val->{$dbd};
        }
        else {
            warn "No $dbd definition for $key";
            return;
        }
    }

    return $val;
}


sub as_sql {
    my $self = shift;
    my @names = map { $_->name } $self->columns;
    return 'CREATE'
        . ($self->unique ? ' UNIQUE' : '')
        . ' INDEX '
        . $self->table->name .'_'
        . join( '_', @names )
        . ' ON ' . $self->table->name
        . ( $self->using ? ' USING '. $self->using : '' )
        . ' (' . join(',', @names ) . ')'
    ;
}


1;
__END__

=head1 NAME

SQL::DB::Index - Perl model of a database index

=head1 SYNOPSIS

  use SQL::DB::Index;

  my $index = SQL::DB::Index->new(
      table      => $table,
      columns    => $str|\@array,
      unique     => 0|1,
      using      => { Pg => 'btree' },
      tablespace => { Pg => 'btree' },
  );

  print $index->as_sql;
  # CREATE UNIQUE INDEX table_columns ON table(columns) USING btree

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::Index> is used by L<SQL::DB>
to represent a database index definition.  Most users will only access
this module through L<SQL::DB>.

Some attributes are multivariate, allowing you to specify different
values depending on what DBD driver will be used. If set be a scalar
then that value will be used for all driver types. If set to a hashref,
then the keys inside the hash determine which value is used for which
driver.

=head1 CONSTRUCTOR

The new() constructor takes key/value pairs matching the attribute
names. The following are required: table, columns.

=head1 ATTRIBUTES

=over 4

=item table

A weak reference to the L<SQL::DB::Table> object having this
index.

=item columns

An arrayref of L<SQL::DB::Column> objects.

=item unique

A boolean indicating if the index is unique.

=item using

The index generation method. Multivariate.

=item tablespace

Tablespace (eg in PostgreSQL). Multivariate.

=back

=head1 METHODS

=over 4

=item as_sql

Returns the SQL representation of this index.

=back

=head1 SEE ALSO

L<SQL::DB::Table>, L<SQL::DB>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2009 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

=cut
# vim: set tabstop=4 expandtab:
