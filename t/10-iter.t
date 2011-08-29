use strict;
use warnings;
use Test::More tests => 1;
use SQL::DB::Iter;

can_ok(
    'SQL::DB::Iter', qw/
      new
      next
      all
      finish
      /
);

