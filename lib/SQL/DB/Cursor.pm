package SQL::DB::Cursor;
use strict;
use warnings;
use Carp qw(croak);


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;

    my ($sth,$cl) = @_;
    ($sth && $cl) || croak 'usage: new($sth,$class)';

    my $self = {
        sth   => $sth,
        class => $cl,
    };

    bless($self, $class);
    return $self;
}


sub next {
    my $self = shift;

    my @list;
    eval {@list = $self->{sth}->fetchrow_array;};

    if (!@list) {
        if (!$@) {
            return;
        }
        croak "DBI:fetchrow_array $DBI::errstr. $@";
    }

    my $class = $self->{class};
    return $class->new_from_arrayref(\@list)->_inflate;
}


1;
__END__


=head1 NAME

SQL::DB::Cursor - SQL::DB database cursor

=head1 SYNOPSIS

  use SQL::DB::Cursor;
  my $cursor = SQL::DB::Cursor->new($sth, $class);

  while (my $next = $cursor->next) {
    print $next->column()
  }

=head1 DESCRIPTION

B<SQL::DB::Cursor> is a cursor interface to DBI. It is typically only
used by the L<SQL::DB> fetch() method, and in user code.

=head1 METHODS

=head2 new($sth)

Create a new cursor object. $sth is a DBI statement handle (ie the result
of a DBI->execute call). $class must be the result of a L<SQL::DB::Row>
make_class_from() method call.

=head2 next

Returns the next row from the database as an object of type $class. Returns
undef when no data is left. Croaks on failure.

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
