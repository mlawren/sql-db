package SQL::DBx::Sequence;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;

our $VERSION = '0.97_3';

has '_sqlite_seq_dbh' => ( is => 'ro' );

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    my %args  = @_;

    $args{dsn} || confess 'Missing argument: dsn';
    my ( $dbi, $dbd, @rest ) = DBI->parse_dsn( $args{dsn} );

    if ( $dbd eq 'SQLite' and $args{dsn} !~ m/\.seq$/ ) {
        my $dsn = $args{dsn} . '.seq';
        my $dbh = DBI->connect(
            $dsn, '', '',
            {
                RaiseError => 1,
                PrintError => 0,
            }
        );
        $args{_sqlite_seq_dbh} = $dbh;
    }

    return $class->$orig(%args);
};

sub create_sequence {
    my $self = shift;
    my $name = shift;

    if ( $self->dbd eq 'SQLite' ) {
        $self->_sqlite_seq_dbh->do( 'CREATE TABLE sequence_' 
              . $name . ' ('
              . 'seq INTEGER PRIMARY KEY, mtime TIMESTAMP )' );
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
        $log->debug( 'INSERT INTO sequence_' 
              . $name
              . "(mtime) VALUES(CURRENT_TIMESTAMP)" );

        $self->_sqlite_seq_dbh->do( 'INSERT INTO sequence_' 
              . $name
              . '(mtime) VALUES(CURRENT_TIMESTAMP)' );

        return $self->_sqlite_seq_dbh->sqlite_last_insert_rowid();
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

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;

