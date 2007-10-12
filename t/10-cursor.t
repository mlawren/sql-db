use strict;
use warnings;
use Test::More;

BEGIN {
    if (!eval {require DBD::SQLite;1;}) {
        plan skip_all => "DBD::SQLite not installed: $@";
    }
    else {
        plan tests => 2;
    }

}
END {
    unlink "/tmp/sqldbtest.db";
}


use_ok('SQL::DB::Cursor');
can_ok('SQL::DB::Cursor', qw/
    new
    next
/);

