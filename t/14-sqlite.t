#!/usr/bin/perl
use strict;
use warnings;
use Test::More;
use Test::Database;
use Cwd;
use File::Temp qw/tempdir/;
use SQL::DB qw/:all/;
use SQL::DBx::SQLite;
use File::Slurp qw/read_file/;

BEGIN {
    eval q{
        use Digest::SHA1 qw/sha1 sha1_hex/;
        require Log::Any::Adapter::Dispatch;
        use Log::Any::Adapter;
    } or plan skip_all => 'No Digest::SHA1 or Log::Any::Adapter::Dispatch';
}

my $cwd;
BEGIN { $cwd = getcwd }

my $subs;

my @handles = Test::Database->handles(qw/ SQLite /);

if ( !@handles ) {
    plan skip_all => "No database handles to test with";
}

my $tempdir;
foreach my $handle (@handles) {
    chdir $cwd || die "chdir: $!";
    $tempdir = tempdir( CLEANUP => 1 );
    chdir $tempdir || die "chdir: $!";

    if ( $handle->dbd eq 'SQLite' ) {
        $handle->driver->drop_database( $handle->name );
    }

    my ( $dsn, $user, $pass ) = $handle->connection_info;

    my $db = SQL::DB->new(
        dsn      => $dsn,
        username => $user,
        password => $pass,
    );

    $db->sqlite_create_function_sha1;

    $db->conn->dbh->do(
        q{
        DROP TABLE IF EXISTS x;
    }
    );

    $db->conn->dbh->do(
        q{
        CREATE TABLE x(xb blob, xs char(40), xself blob);
    }
    );

    $db->conn->dbh->do(
        q{
        CREATE TRIGGER trigx AFTER INSERT ON x
        FOR EACH ROW
        BEGIN
            UPDATE
                x
            SET
                xself = CAST(sha1(1) AS BLOB)
            ;
        END;
    }
    );

    $db->conn->dbh->do(
        q{
        INSERT INTO x(xb,xs,xself)
        VALUES(CAST (sha1(1) AS BLOB),
            '356a192b7913b04c54574d18c28d46e6395428ab',
            CAST(sha1(1) AS BLOB));
    }
    );

    my $sha1     = sha1(1);
    my $sha1_hex = sha1_hex(1);

    #    $db->insert(
    #        into   => 'x',
    #        values => {
    #            xb => $sha1,
    #            xs => $sha1_hex,
    #
    #        }
    #    );

    my $x = $db->srow('x');
    my $r;

    $r = $db->fetch1(
        select => [ $x->xb, $x->xs, $x->xself ],
        from   => $x,
        where  => $x->xs == $sha1_hex,
    );

    ok $r, 'fetch sha1_hex';
    is $r->xs, $sha1_hex, 'sha1_hex match';

    is unpack( 'B*', $r->xb ), unpack( 'B*', $r->xself ), 'unpacked';
    is $r->xb,    $sha1, 'sha1 match';
    is $r->xself, $sha1, 'sha1 self match';

    $r = $db->fetch1(
        select => [ $x->xb, $x->xs, $x->xself ],
        from   => $x,
        where  => $x->xb == $sha1,
    );

    ok $r, 'fetch sha1';
    is $r->xb,    $sha1,     'sha1 match';
    is $r->xs,    $sha1_hex, 'sha1 match';
    is $r->xself, $sha1,     'sha1 self match';

    $r = $db->fetch1(
        select => [ $x->xs, $x->xself, $x->xb ],
        from   => $x,
        where  => $x->xb == $x->xself,
    );

    ok $r, 'fetch sha1_hex';
    is $r->xb,    $sha1, 'sha1 match';
    is $r->xself, $sha1, 'sha1 self match';

=cut
            $db->conn->dbh->do(q{ CREATE INDEX xxb ON x(xb); });

            warn 'xxb';

            $db->conn->dbh->do(q{ CREATE INDEX xxs ON x(xs); });

            warn 'xxs';
=cut

    $db->sqlite_create_function_debug;

    Log::Any::Adapter->set( 'Dispatch',
        outputs => [ [ 'File', min_level => 'debug', filename => 'logfile' ], ]
    );

    $db->conn->dbh->do(
        q{
            SELECT debug('a debug');
    }
    );

    ok -e 'logfile', 'logfile exists';
    is read_file('logfile'), "a debug", 'log output';

}

done_testing();

# So that File::Temp doesn't complain if it can't remove $tempdir when
# $tempdir goes out of scope;
END {
    chdir $cwd;
}

