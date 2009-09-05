package SQL::DB::FK;
use Mouse;
use Mouse::Util::TypeConstraints;

our $VERSION = '0.18';

subtype 'SQL::DB::FK::Types::Initially'
    => as 'Str',
    => where { $_ =~ /^(deferred)|(immediate)$/i };

has 'columns' => (
    is => 'ro',
    isa => 'ArrayRef',
    required => 1,
    auto_deref => 1,
);

has 'references' => (
    is => 'ro',
    isa => 'ArrayRef',
    required => 1,
    auto_deref => 1,
    weaken => 1,
);

has 'deferrable' => (
    is => 'ro',
    isa => 'Bool',
);

has 'initially' => (
    is => 'ro',
    isa => 'SQL::DB::FK::Types::Initially',
    default => 'immediate',
);


sub as_sql {
    my $self = shift;

    return 'FOREIGN KEY ('
        . join( ',', map { $_->name } $self->columns ) . ') REFERENCES '
        . $self->references->[0]->table->name .'('
        . join( ',', map { $_->name } $self->references ) . ')'
        . ( $self->deferrable
                ? ' DEFERRABLE INITIALLY '. uc( $self->initially ) 
                : ' NOT DEFERRABLE'
          )
    ;
}


1;
__END__

=head1 NAME

SQL::DB::FK - Perl representation of database foreign key definitions

=head1 SYNOPSIS

  use SQL::DB::FK;

  my $fk = SQL::DB::FK->new(
      columns    => [$c1,$c2],
      references => [$c3,$c4],
      deferrable => 1,
      initially  => 'immediate',
  );

  print $fk->as_sql;
  # FOREIGN KEY ($c1,$c2) REFERENCES $c3->table($c3,$c4)
  #     DEFERRABLE INITIALLY IMMEDIATE

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::FK> is used by L<SQL::DB> to
represent a database foreign key definition.  Most users will only
use this module indirectly through L<SQL::DB>.

=head1 CONSTRUCTOR

The new() constructor takes key/value pairs matching the attribute
names. The following are required: columns, references.

=head1 ATTRIBUTES

=over 4

=item columns

An arrayref of L<SQL::DB::Column> objects.

=item references

An arrayref of L<SQL::DB::Column> objects. These must be in the same
order as the 'columns' arrayref.

=item deferrable

Boolean whether the foreign key is deferrable or not.

=item initially

Must be one of 'immediate' or 'deferred'.

=back

=head1 METHODS

=over 4

=item as_sql

Returns the SQL representation of this foreign key relationship.

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
