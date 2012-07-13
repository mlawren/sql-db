use strict;
use warnings;
use FindBin qw/$Bin/;
use Path::Class;
use SQL::DB ':all';
use SQL::DBx::Deploy;
use Test::Database;
use Test::More;
use Test::SQL::DB;

foreach my $handle ( Test::Database->handles(qw/SQLite Pg/) ) {
    diag "Running with " . $handle->dbd;

    my $db = SQL::DB->connect( $handle->connection_info );
    isa_ok( $db, 'SQL::DB' );

    $db = SQL::DB->new(
        dsn      => $handle->dsn,
        username => $handle->username,
        password => $handle->password,
    );
    isa_ok( $db, 'SQL::DB' );

    $db->_clean_database();

    $db->deploy_dir( dir($Bin)->subdir('deploy') );

    ok $db->do(
        insert_into => \'actors(id,name)',
        sql_values( 1, 'Mark' )
      ),
      'insert';

    ok $db->do(
        insert_into => sql_table( 'actors', qw/id name/ ),
        sql_values( 2, 'Mark2' )
      ),
      'insert';

    my $actors = $db->irow('actors');
    ok $db->do(
        insert_into => $actors->(qw/id name/),
        sql_values( bv(3), 'Mark3' )
      ),
      'insert';

    $actors = $db->srow('actors');

    my @res = $db->fetch(
        select => [ $actors->id, $actors->name ],
        from   => $actors,
    );

    ok @res == 3, 'select many';
    can_ok $res[0], qw/id name/;

    my $res = $db->fetch1(
        select => [ $actors->id, $actors->name ],
        from   => $actors,
        where  => $actors->id == 1,
    );

    is $res->id,   1,      'res id';
    is $res->name, 'Mark', 'res name';

    # These tests are repeated to make sure we are closing the
    # statement handle at the end.
    foreach ( 1 .. 2 ) {
        my $iter = $db->iter(
            select => [ $actors->id, $actors->name ],
            from   => $actors,
            where => ( $actors->id == 1 ) . AND . ( $actors->name != 'Julie' ),
            limit => 1,
        );

        isa_ok $iter, 'SQL::DB::Iter';
        isa_ok $iter->sth, 'DBI::st';
        ok $iter->class,   $iter->class;

        my $row = $iter->next;
        is ref $row, $iter->class, 'row is ' . $iter->class;
        is $row->id,   1,      'rowid';
        is $row->name, 'Mark', 'row name';

        ok !$iter->next, 'next is undef';

        $iter = $db->iter(
            select => [ $actors->id, $actors->name ],
            from   => $actors,
        );

        my @rows = $iter->all;
        ok !$iter->next, 'next is undef';
        is ref $rows[0], $iter->class, 'row is ' . $iter->class;
    }

    is sql_cast( $actors->id, as => 'char' )->_as_string,
      'CAST( actors0.id AS char )', 'CAST';

    ok $db->delete(
        from  => 'actors',
        where => {},
      ),
      'delete';

    ok $db->insert(
        into   => 'actors',
        values => { id => 1, name => 'Mark' }
      ),
      'insert';

    ok $db->insert(
        into   => 'actors',
        values => { id => 2, name => 'Mark2' }
      ),
      'insert';

    @res = $db->select( [ 'id', 'name' ], from => 'actors', );

    ok @res == 2, 'select many';
    can_ok $res[0], qw/id name/;

    $res = $db->select(
        [ 'id', 'name' ],
        from  => 'actors',
        where => { id => 1 },
    );

    is $res->id,   1,      'res id';
    is $res->name, 'Mark', 'res name';
}

done_testing();
