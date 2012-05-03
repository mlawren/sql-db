package SQL::DBx::Deploy;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;
use YAML;

our $VERSION      = '0.19_14';
our $DEPLOY_TABLE = '_deploy';

sub last_deploy_id {
    my $self = shift;
    my $app  = shift || croak 'deploy_id($app)';
    my $dbh  = $self->conn->dbh;

    my $sth = $dbh->table_info( '%', '%', $DEPLOY_TABLE );
    return 0 unless ( @{ $sth->fetchall_arrayref } );

    return $dbh->selectrow_array(
        'SELECT COALESCE(MAX(seq),0) FROM ' . $DEPLOY_TABLE . ' WHERE app=?',
        undef, $app );
}

sub deploy {
    my $self = shift;
    my $app = shift || confess 'usage: deploy($app)';

    my $class = $app . '::' . $self->dbd;
    eval "require $class;";
    confess $@ if $@;

    my $fh = eval "\\*${class}::DATA";
    my $start_pos = eval { tell $fh };
    die "$class has no __DATA__ section" if $@;

    my $yaml = do { local $/; <$fh> };
    seek $fh, $start_pos, 0;

    my $ref = Load($yaml);

    return $self->conn->txn(
        sub {
            my $dbh = $_;

            my $sth = $dbh->table_info( '%', '%', $DEPLOY_TABLE );
            my $_deploy = $dbh->selectall_arrayref($sth);

            unless (@$_deploy) {
                $log->debug( 'Create table ' . $DEPLOY_TABLE );
                $dbh->do( "
            CREATE TABLE $DEPLOY_TABLE (
                app VARCHAR(40) NOT NULL PRIMARY KEY,
                seq INTEGER NOT NULL DEFAULT 0,
                ctime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                type VARCHAR(20),
                data VARCHAR
            )" );
                $dbh->do( "
CREATE TRIGGER au_$DEPLOY_TABLE AFTER UPDATE ON $DEPLOY_TABLE
FOR EACH ROW
BEGIN
    UPDATE
        $DEPLOY_TABLE
    SET
        seq = seq + 1
    WHERE
        app = OLD.app
    ;
END" );
            }

            my @current = $dbh->selectrow_array(
                'SELECT COUNT(app) from ' . $DEPLOY_TABLE . ' WHERE app=?',
                undef, $app );

            unless ( $current[0] ) {
                $dbh->do( '
                    INSERT INTO ' . $DEPLOY_TABLE . '(app)
                    VALUES(?)
                ', undef, $app );
            }

            my $latest_change_id = $self->last_deploy_id($app);
            $log->debug( 'Latest Change ID:', $latest_change_id );

            my $count = 0;
            foreach my $cmd (@$ref) {
                $count++;
                next unless ( $count > $latest_change_id );

                exists $cmd->{sql}
                  || exists $cmd->{perl}
                  || die "Missing 'sql' or 'perl' key for id " . $count;

                if ( exists $cmd->{sql} ) {
                    $log->debug( $cmd->{sql} );
                    eval { $dbh->do( $cmd->{sql} ) };
                    die $cmd->{sql} . $@ if $@;
                    $dbh->do( "
UPDATE 
    $DEPLOY_TABLE
SET
    type = ?,
    data = ?
WHERE
    app = ?
",
                        undef, 'sql', $cmd->{sql}, $app );
                }

                if ( exists $cmd->{perl} ) {
                    $log->debug( $cmd->{perl} );
                    eval "$cmd->{perl}";
                    die $cmd->{perl} . $@ if $@;
                    $dbh->do( "
UPDATE 
    $DEPLOY_TABLE
SET
    type = ?,
    data = ?
WHERE
    app = ?
",
                        undef, 'sql', $cmd->{perl}, $app );
                }
            }
            $log->debug( 'Deployed to Change ID:', $count );
            return ( $latest_change_id, $count );
        }
    );
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;
