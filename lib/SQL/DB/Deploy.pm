package SQL::DB::Deploy;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;

our $VERSION = '0.97_2';

sub last_deploy_id {
    my $self = shift;
    my $app = shift || croak 'deploy_id($app)';

    return eval {
        $self->conn->dbh->selectrow_array(
            'SELECT count(id) FROM _sqldb WHERE app=?',
            undef, $app );
    } || 0;
}

sub deploy {
    my $self = shift;
    my $app  = shift || croak 'deploy($app,$ref)';
    my $ref  = shift || croak 'deploy($app,$ref)';

    unless ( ref $ref ) {
        eval { require YAML };
        die "Deploy YAML feature not enabled" if $@;
        if ( $ref =~ /^---/ ) {
            $ref = YAML::Load($ref);
        }
        else {
            $ref = YAML::LoadFile($ref);
        }
    }

    if ( !grep { $_ eq $self->dbd } keys %$ref ) {
        die "Missing key for deploy: "
          . $self->dbd
          . ' (have '
          . join( ',', keys %$ref ) . ')';
    }

    return $self->conn->txn(
        sub {
            my $dbh = $_;

            my $sth = $dbh->table_info( '%', '%', '_sqldb' );
            my $_sqldb = $dbh->selectall_arrayref($sth);

            unless (@$_sqldb) {
                $log->debug('Creating _sqldb');
                $dbh->do( '
            CREATE TABLE _sqldb (
                id INTEGER,
                app VARCHAR(40) NOT NULL,
                ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                type VARCHAR(20),
                data VARCHAR,
                PRIMARY KEY (id,app)
            )' );
            }

            my $latest_change_id = $self->last_deploy_id($app);
            $log->debug( 'Latest Change ID:', $latest_change_id );

            my $count = 0;
            foreach my $cmd ( @{ $ref->{ $self->dbd } } ) {
                $count++;
                next unless ( $count > $latest_change_id );

                exists $cmd->{sql}
                  || exists $cmd->{perl}
                  || die "Missing 'sql' or 'perl' key for id " . $count;

                if ( exists $cmd->{sql} ) {
                    $log->debug( $cmd->{sql} );
                    $dbh->do( $cmd->{sql} );
                    $dbh->do(
                        'INSERT INTO _sqldb(id,app,type,data) VALUES(?,?,?,?)',
                        undef, $count, $app, 'sql', $cmd->{sql}
                    );
                }

                if ( exists $cmd->{perl} ) {
                    $log->debug( $cmd->{perl} );
                    eval "$cmd->{perl}";
                    die $@ if $@;
                    $dbh->do(
                        'INSERT INTO _sqldb(id,app,type,data) VALUES(?,?,?,?)',
                        undef, $count, $app, 'perl', $cmd->{perl}
                    );
                }
            }
            $log->debug( 'Deployed to Change ID:', $count );
            return ( $latest_change_id, $count );
        }
    );
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;
