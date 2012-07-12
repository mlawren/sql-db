package SQL::DBx::Deploy;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;
use File::Slurp qw/read_file/;
use File::Temp;
use Path::Class;

our $VERSION      = '0.191.0';
our $DEPLOY_TABLE = '_deploy';

sub last_deploy_id {
    my $self = shift;
    my $app  = shift || 'default';
    my $dbh  = $self->conn->dbh;

    my $sth = $dbh->table_info( '%', '%', $DEPLOY_TABLE );
    return 0 unless ( @{ $sth->fetchall_arrayref } );

    return $dbh->selectrow_array(
        'SELECT COALESCE(MAX(seq),0) FROM ' . $DEPLOY_TABLE . ' WHERE app=?',
        undef, $app );
}

sub _create_deploy_table_SQLite {
    my $self = shift;
    my $dbh  = shift;

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
FOR EACH ROW WHEN OLD.seq = NEW.seq
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
}

sub deploy {
    my $self = shift;
    my $ref  = shift;
    my $app  = shift || 'default';

    confess 'deploy(ARRAYREF)' unless ref $ref eq 'ARRAY';

    return $self->conn->txn(
        sub {
            my $dbh = $_;

            my $name = '_create_deploy_table_' . $self->dbd;
            my $sub  = $self->can($name)
              || die __PACKAGE__ . ' does not implement ' . $self->dbd;

            $self->$sub($dbh);

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
                  || exists $cmd->{pl}
                  || confess "Missing 'sql' or 'pl' key for id " . $count;

                if ( exists $cmd->{sql} ) {
                    $log->debug( "-- change #$count\n" . $cmd->{sql} );
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

                if ( exists $cmd->{pl} ) {
                    $log->debug( "# change #$count\n" . $cmd->{pl} );
                    my $tmp = File::Temp->new;
                    print $tmp $cmd->{pl};
                    system( $^X, $tmp->filename ) == 0 or die "system failed";
                    $dbh->do( "
UPDATE 
    $DEPLOY_TABLE
SET
    type = ?,
    data = ?
WHERE
    app = ?
",
                        undef, 'pl', $cmd->{pl}, $app );
                }
            }
            $log->debug( 'Deployed to Change ID:', $count );
            return ( $latest_change_id, $count );
        }
    );
}

sub _load_deploy_file {
    my $file = shift;
    my $type = lc $file;

    $log->debug( '_load_deploy_file(' . $file . ')' );
    confess "fatal: missing extension/type: $file\n"
      unless $type =~ s/.*\.(.+)$/$1/;

    my $input = read_file $file;
    my @items;

    if ( $type eq 'sql' ) {

        while ($input) {
            $input =~ s/(^[\s\n]+)|(\s\n]+$)//;
            $input =~ s/^\s*--.*\n//gm;
            last unless $input;

            if ( $input =~ m/^CREATE TRIGGER/i ) {
                if ( $input =~ s/(.+?^END;([^\n]*))//ms ) {
                    push( @items, { sql => $1 } );
                }
            }
            elsif ( $input =~ s/(.+?;)([^\n]*)$//ms ) {
                push( @items, { sql => $1 } );
            }
            else {
                ( my $line = $input ) =~ s/\n.*//ms;
                die "fatal: invalid input in $file starting with:\n$input";
            }
        }
    }
    elsif ( $type eq 'pl' ) {
        push( @items, { $type => $input } );
    }
    else {
        die "Cannot deploy file of type '$type': $file";
    }

    return @items;
}

sub deploy_file {
    my $self = shift;
    my $file = shift;
    my $app  = shift;
    $self->deploy( [ _load_deploy_file($file) ], $app );
}

sub deploy_dir {
    my $self = shift;
    my $dir  = dir(shift) || confess 'deploy_dir($dir)';
    my $app  = shift;

    confess "directory not found: $dir" unless -d $dir;

    my @files;
    while ( my $file = $dir->next ) {
        push( @files, $file )
          if $file =~ m/.+\.((sql)|(pl))$/ and -f $file;
    }

    my @items =
      map  { _load_deploy_file($_) }
      sort { $a->stringify cmp $b->stringify } @files;
    $self->deploy( \@items, $app );
}

sub deployed_table_info {
    my $self     = shift;
    my $dbschema = shift;

    if ( !$dbschema ) {
        $dbschema = 'main'   if $self->dbd eq 'SQLite';
        $dbschema = 'public' if $self->dbd eq 'Pg';
        $dbschema = '%';
    }

    my $sth = $self->conn->dbh->table_info( '%', $dbschema, '%',
        "'TABLE','VIEW','GLOBAL TEMPORARY','LOCAL TEMPORARY'" );

    my %tables;

    while ( my $table = $sth->fetchrow_arrayref ) {
        my $sth2 = $self->conn->dbh->column_info( '%', '%', $table->[2], '%' );
        $tables{ $table->[2] } = $sth2->fetchall_arrayref;
    }

    return \%tables;
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;
