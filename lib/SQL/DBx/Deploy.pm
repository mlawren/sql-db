package SQL::DBx::Deploy;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;
use File::ShareDir qw/dist_dir/;
use File::Slurp qw/read_file/;
use File::Temp;
use Path::Class;

our $VERSION = '0.191.0';

sub last_deploy_id {
    my $self = shift;
    my $app  = shift || 'default';
    my $dbh  = $self->conn->dbh;

    my $sth = $dbh->table_info( '%', '%', '_deploy' );
    return 0 unless ( @{ $sth->fetchall_arrayref } );

    return $dbh->selectrow_array(
        'SELECT COALESCE(MAX(seq),0) FROM _deploy WHERE app=?',
        undef, $app );
}

sub _load_file {
    my $file = shift;
    my $type = lc $file;

    $log->debug( '_load_file(' . $file . ')' );
    confess "fatal: missing extension/type: $file\n"
      unless $type =~ s/.*\.(.+)$/$1/;

    my $input = read_file $file;
    my @items;

    if ( $type eq 'sql' ) {

        while ($input) {
            $input =~ s/(^[\s\n]+)|(\s\n]+$)//;
            $input =~ s/^\s*--.*\n//gm;
            $input =~ s!/\*.*?\*/!!gsm;
            last unless $input;

            if ( $input =~ m/^CREATE (OR REPLACE )?FUNCTION/i ) {
                if ( $input =~ s/(.+?LANGUAGE [^\n]+)//ims ) {
                    push( @items, { sql => $1 } );
                }
                else {
                    die "fatal: invalid input in $file starting with:\n$input";
                }
            }
            elsif ( $input =~ m/^CREATE TRIGGER/i ) {

                # PostgreSQL trigger
                if ( $input =~ s/(.+? EXECUTE PROCEDURE .*?;)//ims ) {
                    push( @items, { sql => $1 } );
                }

                # SQLite trigger
                elsif ( $input =~ s/(.+?^END;([^\n]*))//ims ) {
                    push( @items, { sql => $1 } );
                }
                else {
                    die "fatal: invalid input in $file starting with:\n$input";
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
        die "Cannot load file of type '$type': $file";
    }

    return @items;
}

sub _run_cmds {
    my $self = shift;
    my $ref  = shift;

    my $dbh = $self->conn->dbh;

    foreach my $cmd (@$ref) {
        if ( exists $cmd->{sql} ) {
            $log->debug( $cmd->{sql} );
            eval { $dbh->do( $cmd->{sql} ) };
            die $cmd->{sql} . "\n" . $@ if $@;
        }
        elsif ( exists $cmd->{pl} ) {
            $log->debug( $cmd->{pl} );
            my $tmp = File::Temp->new;
            print $tmp $cmd->{pl};
            system( $^X, $tmp->filename ) == 0 or die "system failed";
        }
        else {
            confess "Missing 'sql' or 'pl' key";
        }
    }
}

sub run_file {
    my $self = shift;
    my $file = shift;

    $self->_run_cmds( _load_file($file) );
}

sub run_dir {
    my $self = shift;
    my $dir = dir(shift) || confess 'deploy_dir($dir)';

    confess "directory not found: $dir" unless -d $dir;

    my @files;
    while ( my $file = $dir->next ) {
        push( @files, $file )
          if $file =~ m/.+\.((sql)|(pl))$/ and -f $file;
    }

    my @items =
      map  { _load_file($_) }
      sort { $a->stringify cmp $b->stringify } @files;

    $self->_run_cmds( \@items );
}

sub deploy {
    my $self = shift;
    my $ref  = shift;
    my $app  = shift || 'default';

    confess 'deploy(ARRAYREF)' unless ref $ref eq 'ARRAY';

    # The lib ("prove -Ilib t/*") case:
    my $dir1 =
      file(__FILE__)
      ->parent->parent->parent->parent->subdir( 'share', $self->dbd );

    # The blib ("make test") case
    my $dir2 =
      file(__FILE__)
      ->parent->parent->parent->parent->parent->subdir( 'share', $self->dbd );

    if ( -d $dir1 ) {
        $self->run_dir( $dir1->subdir('deploy') );
    }
    elsif ( -d $dir2 ) {
        $self->run_dir( $dir2->subdir('deploy') );
    }
    else {
        # The "installed" case
        $self->run_dir( dist_dir( 'SQL-DB', $self->dbd, 'deploy' ) );
    }

    my $dbh = $self->conn->dbh;
    my @current =
      $dbh->selectrow_array( 'SELECT COUNT(app) from _deploy WHERE app=?',
        undef, $app );

    unless ( $current[0] ) {
        $dbh->do( '
                    INSERT INTO _deploy(app)
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
            die $cmd->{sql} . "\n" . $@ if $@;
            $dbh->do( "
UPDATE 
    _deploy
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
    _deploy
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

sub deploy_file {
    my $self = shift;
    my $file = shift;
    my $app  = shift;
    $self->deploy( [ _load_file($file) ], $app );
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
      map  { _load_file($_) }
      sort { $a->stringify cmp $b->stringify } @files;

    $self->deploy( \@items, $app );
}

sub deployed_table_info {
    my $self     = shift;
    my $dbschema = shift;

    if ( !$dbschema ) {
        if ( $self->dbd eq 'SQLite' ) {
            $dbschema = 'main';
        }
        elsif ( $self->dbd eq 'Pg' ) {
            $dbschema = 'public';
        }
        else {
            $dbschema = '%';
        }
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
