use strict;
use warnings;
use FindBin qw/$Bin/;
use Path::Class;
use SQL::DB;
use SQL::DBx::Deploy;
use Test::More;
use Test::Database;

foreach my $handle ( Test::Database->handles(qw/SQLite/) ) {
    diag "Running with " . $handle->dbd;
    $handle->driver->drop_database($_) for $handle->driver->databases;

    my $db = SQL::DB->connect( $handle->connection_info );

    my $dir1  = dir($Bin)->subdir('deploy');
    my $dir2  = dir($Bin)->subdir('deploy2');
    my $file1 = $dir1->file('1.sql');
    my $file2 = $dir2->file('1.sql');
    my $ret;
    my $prev_id;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 0, 'Nothing deployed yet: ' . $prev_id;

    $ret = $db->deploy_file($file1);
    is $ret, 2, 'deployed to ' . $ret;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 2, 'last id check';

    $ret = $db->deploy_file($file1);
    is $ret, 2, 'still deployed to ' . $ret;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 2, 'still last id check';

    $ret = $db->deploy_file($file2);
    is $ret, 3, 'upgraded to ' . $ret;

    $handle->driver->drop_database($_) for $handle->driver->databases;

    $db = SQL::DB->connect( $handle->connection_info );

    $prev_id = $db->last_deploy_id;
    is $prev_id, 0, 'Nothing deployed yet: ' . $prev_id;

    $ret = $db->deploy_dir($dir1);
    is $ret, 3, 'deployed to ' . $ret;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 3, 'last id check';

    $ret = $db->deploy_dir($dir1);
    is $ret, 3, 'still deployed to ' . $ret;

    $prev_id = $db->last_deploy_id;
    is $prev_id, 3, 'still last id check';

    $ret = $db->deploy_dir($dir2);
    is $ret, 4, 'upgraded to ' . $ret;

    my $table_info = $db->deployed_table_info;
    isa_ok( $table_info, 'HASH' );
    is_deeply(
        [ sort keys %$table_info ],
        [qw/_deploy actors films/],
        'deployed_table_info'
    );
}

done_testing;
