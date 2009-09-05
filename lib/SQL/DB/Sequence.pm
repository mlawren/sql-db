package SQL::DB::Sequence;
use Mouse;

our $VERSION = '0.18';

has 'table' => (
    is => 'ro',
    isa => 'SQL::DB::Table',
    weaken => 1,
);

has 'name' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has 'inc' => (
    is => 'ro',
    isa => 'Int',
    default => 1,
);

has 'start' => (
    is => 'ro',
    isa => 'Int',
    default => 1,
);


sub as_sql {
    my $self = shift;
    if ( $self->table && ($self->table->dbd || '') eq 'Pg' ) {
        return 'CREATE SEQUENCE '. $self->name
            . ' INCREMENT BY '. $self->increment;
    }
}


1;
__END__

=head1 NAME

SQL::DB::Sequence - Perl representation of database sequence

=head1 SYNOPSIS

  use SQL::DB::Sequence;

  my $seq = SQL::DB::Sequence->new(
      name   => 'myseq',
      inc    => 2,
      start  => 1,
  );

  print $seq->as_sql; # only for DBD = 'Pg'
  # CREATE SEQUENCE myseq INCREMENT BY 2

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::Sequence> is used by L<SQL::DB>
to represent a database sequence. Most users will only use this module
indirectly through L<SQL::DB>.

=head1 CONSTRUCTOR

The new() constructor takes key/value pairs matching the attribute
names. The following are required: name.

=head1 ATTRIBUTES

=over 4

=item table

A weakened reference to a L<SQL::DB::Table>.

=item name

The sequence name.

=item inc

An integer amount by which the sequence is incremented.

=item start

The first value returned by the sequence.

=back

=head1 METHODS

=over

=item as_sql -> Str

Returns the SQL representation of this index.

=back

=head1 SEE ALSO

L<SQL::DB>

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
