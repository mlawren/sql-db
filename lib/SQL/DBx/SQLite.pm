package SQL::DBx::SQLite;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;

our $VERSION = '0.19_13';

sub sqlite_create_function_debug {
    my $self = shift;

    return unless $self->dbd eq 'SQLite';

    my $dbh = $self->conn->dbh;

    $dbh->sqlite_create_function(
        'debug', -1,
        sub {
            if ( @_ && defined $_[0] && $_[0] =~ m/^select/i ) {
                my $sth = $dbh->prepare("@_");
                $sth->execute;
                use Data::Dumper;
                while ( my $ref = $sth->fetchrow_hashref ) {
                    $log->debug( Dumper($ref) );
                }

              #                map {$log->debug(@$_)}@{$sth->fetchall_arrayref};
              #                $sth->dump_results;
            }
            else {
                $log->debug( map { defined $_ ? $_ : 'NULL' } @_ );
            }
        }
    );

    $log->debugf( 'SQL debug() function added by ' . __PACKAGE__ );
}

sub sqlite_create_function_sha1 {
    my $self = shift;

    return unless $self->dbd eq 'SQLite';

    require Digest::SHA1;
    my $dbh = $self->conn->dbh;

    $dbh->sqlite_create_function(
        'sha1', -1,
        sub {
            Digest::SHA1::sha1( grep { defined $_ } @_ );
        }
    );

    $dbh->sqlite_create_function(
        'sha1_hex',
        -1,
        sub {
            Digest::SHA1::sha1_hex( grep { defined $_ } @_ );
        }
    );

    $dbh->sqlite_create_function(
        'sha1_base64',
        -1,
        sub {
            Digest::SHA1::sha1_base64( grep { defined $_ } @_ );
        }
    );

    $dbh->sqlite_create_aggregate( 'agg_sha1', -1,
        'SQL::DBx::SQLite::agg_sha1' );

    $dbh->sqlite_create_aggregate( 'agg_sha1_hex', -1,
        'SQL::DBx::SQLite::agg_sha1_hex' );

    $dbh->sqlite_create_aggregate( 'agg_sha1_base64', -1,
        'SQL::DBx::SQLite::agg_sha1_base64' );

    $log->debugf( 'SQL sha1*() functions added by ' . __PACKAGE__ );
    return;
}

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

sub sqlite_create_function_nextval {
    my $self = shift;

    return unless $self->dbd eq 'SQLite';

    my $dbh = $self->conn->dbh;
    $dbh->sqlite_create_function( 'nextval', 1, sub { _nextval( $dbh, $_[0] ) },
    );

    $log->debug( 'SQL nextval() function added by ' . __PACKAGE__ );
}

sub sqlite_create_function_currval {
    my $self = shift;

    return unless $self->dbd eq 'SQLite';

    my $dbh = $self->conn->dbh;
    $dbh->sqlite_create_function( 'currval', 1, sub { _currval( $dbh, $_[0] ) },
    );

    $log->debug( 'SQL currval() function added by ' . __PACKAGE__ );
}

sub sqlite_create_sequence {
    my $self = shift;
    my $name = shift || confess 'sqlite_create_sequence($name)';

    return unless $self->dbd eq 'SQLite';

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

    # the sqlite_sequence table doesn't have any constraints so it
    # would be possible to insert the same sequence twice. Check if
    # one already exists
    my $val = ( $dbh->selectrow_array( $seq_get, undef, $name ) )[0];
    $val && croak "create_sequence: sequence already exists: $name";
    $dbh->do( 'INSERT INTO sqlite_sequence(name,seq) VALUES(?,?)',
        undef, $name, 0 );
}

sub nextval {
    my $self = shift;
    my $name = shift;

    return _nextval( $self->conn->dbh, $name );
}

sub currval {
    my $self = shift;
    my $name = shift;

    return _currval( $self->conn->dbh, $name );
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

package SQL::DBx::SQLite::agg_sha1;

sub new {
    bless { d => Digest::SHA1->new }, shift;
}

sub step {
    my $self = shift;
    $self->{d}->add( grep { defined $_ } @_ );
}

sub finalize {
    $_[0]->{d}->digest;
}

package SQL::DBx::SQLite::agg_sha1_hex;
our @ISA = ('SQL::DBx::SQLite::agg_sha1');

sub finalize {
    $_[0]->{d}->hexdigest;
}

package SQL::DBx::SQLite::agg_sha1_base64;
our @ISA = ('SQL::DBx::SQLite::agg_sha1');

sub finalize {
    $_[0]->{d}->b64digest;
}

1;
