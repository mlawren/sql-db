package SQL::DBx::Sequence;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;

our $VERSION = '0.19_11';

my $seq_inc = q{UPDATE
    sqlite_sequence
SET
    seq = seq + 1
WHERE
    name = ?};

my $seq_get = q{SELECT
    seq
FROM
    sqlite_sequence
WHERE
    name = ?};

sub _nextval {
    my $dbh = shift;
    my $name = shift || die 'nextval($name)';

    if ( $dbh->do( $seq_inc, undef, $name ) ) {
        my $val = ( $dbh->selectrow_array( $seq_get, undef, $name ) )[0];
        defined $val || croak "nextval: unknown sequence: $name";
        return $val;
    }
    else {
        croak "nextval1: unknown sequence: $name";
    }
}

sub _currval {
    my $dbh = shift;
    my $name = shift || die 'currval($name)';

    my $val = ( $dbh->selectrow_array( $seq_get, undef, $name ) )[0];

    defined $val || croak "currval: unknown sequence: $name";
    return $val;
}

after BUILD => sub {
    my $self = shift;
    return unless $self->dbd eq 'SQLite';

    my $dbh = $self->conn->dbh;
    $dbh->sqlite_create_function( 'nextval', 1, sub { _nextval( $dbh, $_[0] ) },
    );

    $dbh->sqlite_create_function( 'currval', 1, sub { _currval( $dbh, $_[0] ) },
    );

    return;
};

sub create_sequence {
    my $self = shift;
    my $name = shift;

    if ( $self->dbd eq 'SQLite' ) {
        my $dbh = $self->conn->dbh;

        # The sqlite_sequence table doesn't exist until an
        # autoincrement table has been created.
        # IF NOT EXISTS is used because table_info may not return any
        # information if we are inside a transaction where the first
        # sequence was created
        if ( !$dbh->selectrow_array('PRAGMA table_info(sqlite_sequence)') ) {
            $dbh->do( 'CREATE TABLE IF NOT EXISTS '
                  . 'Ekag4iiB(x integer primary key autoincrement)' );
            $dbh->do('DROP TABLE IF EXISTS Ekag4iiB');
        }
        $dbh->do( 'INSERT INTO sqlite_sequence(name,seq) VALUES(?,?)',
            undef, $name, 0 );
    }
    elsif ( $self->dbd eq 'Pg' ) {
        $self->conn->run(
            sub {
                $_->do( 'CREATE SEQUENCE seq_' . $name );
            }
        );
    }
    else {
        die "Sequence support not implemented for " . $self->dbd;
    }
}

sub nextval {
    my $self = shift;
    my $name = shift;

    if ( $self->dbd eq 'SQLite' ) {
        _nextval( $self->conn->dbh, $name );
    }
    elsif ( $self->dbd eq 'Pg' ) {
        $log->debug( "SELECT nextval('seq_" . $name . "')" );
        my $val = $self->conn->run(
            sub {
                $_->selectrow_array( "SELECT nextval('seq_" . $name . "')" );
            }
        );
    }
    else {
        die "Sequence support not implemented for " . $self->dbd;
    }
}

sub currval {
    my $self = shift;
    my $name = shift;

    if ( $self->dbd eq 'SQLite' ) {
        _currval( $self->conn->dbh, $name );
    }
    elsif ( $self->dbd eq 'Pg' ) {
        $log->debug( "SELECT currval('seq_" . $name . "')" );
        my $val = $self->conn->run(
            sub {
                $_->selectrow_array( "SELECT currval('seq_" . $name . "')" );
            }
        );
    }
    else {
        die "Sequence support not implemented for " . $self->dbd;
    }
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;

