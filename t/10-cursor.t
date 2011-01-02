use strict;
use warnings;
use Test::More tests => 2;

use_ok('SQL::DB::Cursor');
can_ok('SQL::DB::Cursor', qw/
    new
    next
    all
    finish
/);

