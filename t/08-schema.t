use strict;
use warnings;
use Test::More tests => 2;

use_ok('SQL::DB::Schema');
can_ok(
    'SQL::DB::Schema', qw/
      table
      end_schema
      /
);

