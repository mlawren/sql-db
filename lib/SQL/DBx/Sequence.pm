package SQL::DBx::Sequence;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;

our $VERSION = '0.19_11';

after BUILD => sub {
    my $self = shift;
    return unless $self->dbd eq 'SQLite';

    my $rows = $self->conn->dbh->selectall_arrayref('PRAGMA database_list');

    foreach my $row (@$rows) {
        if ( $row->[1] eq 'main' ) {
            $self->conn->dbh->do("ATTACH '$row->[2].seq' AS seq");
            last;
        }
    }

};

sub create_sequence {
    my $self = shift;
    my $name = shift;

    if ( $self->dbd eq 'SQLite' ) {
        $self->conn->dbh->do( 'CREATE TABLE seq.' 
              . $name . ' ('
              . 'seq INTEGER PRIMARY KEY AUTOINCREMENT, x INTEGER )' );

        $self->conn->dbh->do( "
            CREATE TRIGGER seq.ai_$name AFTER INSERT ON $name
            BEGIN
                DELETE FROM $name WHERE seq != NEW.seq;
            END;"
        );

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
        $log->debug( 'INSERT INTO seq.' . $name . "(x) VALUES(NULL)" );

        $self->conn->dbh->do( 'INSERT INTO seq.' . $name . '(x) VALUES(NULL)' );

        return $self->conn->dbh->sqlite_last_insert_rowid();
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

