package SQL::DBx::Pg;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;

our $VERSION = '0.971.0';

sub nextval {
    my $self = shift;
    my $name = shift || die 'nextval($name)';

    $log->debugf( 'SELECT nextval(%s)', $name );
    return $self->conn->dbh->selectrow_array(
        "SELECT
    nextval('$name')"
    );
    return $self->conn->dbh->selectrow_array( 'SELECT nextval(?)', undef,
        $name );
}

sub currval {
    my $self = shift;
    my $name = shift || die 'currval($name)';

    $log->debugf( 'SELECT currval(%s)', $name );
    return $self->conn->dbh->selectrow_array( 'SELECT currval(?)', undef,
        $name );
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;
