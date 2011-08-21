package SQL::DB::Cursor;
use Moo;
use Sub::Install qw/install_sub/;
use Carp qw(croak);

our $VERSION = '0.19_3';

has 'sth' => (
    is       => 'ro',
    required => 1,
);

has 'db' => (
    is       => 'ro',
    required => 1,
    weak_ref => 1,
);

has 'query' => (
    is       => 'ro',
    required => 1,
);

has 'class' => (
    is       => 'rw',
    init_arg => undef,
);

has 'first' => (
    is       => 'rw',
    init_arg => undef,
);

has 'done' => (
    is       => 'rw',
    init_arg => undef,
);

my %classes;

sub _make_class {
    my $ref   = shift;
    my @cols  = sort keys %$ref;
    my $class = 'SQL::DB::Row::' . join( '_', @cols );
    return $class if ( $classes{$class} );

    foreach my $col (@cols) {
        install_sub(
            {
                code => sub {
                    return $_[0]->{$col} if @_ == 1;
                    my $caller = caller;
                    croak "'$caller' cannot alter the value of '$col' "
                      . "on objects of class '$class'";
                },
                into => $class,
                as   => $col,
            }
        );
    }

    return $classes{$class} = $class;
}

sub next {
    my $self = shift;
    return if ( $self->done );

    my $ref;

    #    try {
    $ref = $self->sth->fetchrow_hashref;

    #    } catch {
    #        croak $self->db->query_as_string($self->query,
    #            @{ $self->query->_bvalues } )
    #            . "DBI:fetchrow_hashref: $_";
    #    };

    if ( !$ref ) {
        $self->done(1);
        return;
    }
    if ( !$self->class ) {
        $self->class( _make_class($ref) );
    }
    return bless \%{$ref}, $self->class;
}

sub all {
    my $self = shift;

    my @all;
    while ( !$self->done ) {
        my $ref;

        #        try {
        $ref = $self->sth->fetchrow_hashref;

        #        } catch {
        #            croak $self->db->query_as_string($self->query,
        #                @{ $self->query->_bvalues } ) ;
        #                . "DBI:fetchrow_hashref: $_";
        #        };

        if ( !$ref ) {
            $self->done(1);
            last;
        }
        if ( !$self->class ) {
            $self->class( _make_class($ref) );
        }
        push( @all, bless \%{$ref}, $self->class );
    }
    return @all;
}

sub finish {
    my $self = shift;
    $self->sth->finish;
    $self->done(1);
}

sub DESTROY {
    my $self = shift;
    $self->sth->finish;
}

1;
__END__


=head1 NAME

SQL::DB::Cursor - SQL::DB database cursor

=head1 SYNOPSIS

  use SQL::DB::Cursor;
  my $cursor = SQL::DB::Cursor->new(
    sth => $sth,
    db  => $sqldb,
    query => $expr,
  );

  while (my $next = $cursor->next) {
    print $next->column()
  }

  # Or if you want you can get all at once:
  my @objects = $cursor->all;

=head1 DESCRIPTION

B<SQL::DB::Cursor> is a Perl-side cursor/read-only ORM interface to
DBI. It is used by L<SQL::DB> to create objects from rows. See the
SQL::DB "fetch()" method for details.

=head1 CONSTRUCTOR

=over 4

=item new(sth => $sth, db => $db, query => $expr)

Create a new cursor object. $sth is a DBI statement handle (ie the
result of a DBI->execute call). $db is the SQL::DB object creating the
cursor. $expr must be the SQL::DB::Expr which was used to create $sth.

=back

=head1 ATTRIBUTES

=over 4

=item sth

The DBI statement handle.

=item db

The SQL::DB object that created the statement handle and query.

=item query

The SQL::DB::Expr used to create the statement handle.

=item class

The class into which retrieved objects are blessed into. This is
automatically created from the first row retrieved.

=item done

Boolean value that is set to true when there are no more rows to be
returned.

=back

=head1 METHODS

=over 4

=item next

Returns the next row from the statement handle as an object. Returns
undef when no data is left. Croaks on failure.

=item all

Returns all rows from the statement handle as a list of objects.
Returns the empty list if no data is available (or has already been
fetch with 'next' for example). Croaks on failure.

=item finish

Calls finish() on the DBI statement handle.

=back

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007,2008 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

=cut

# vim: set tabstop=4 expandtab:
