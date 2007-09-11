use strict;
use warnings;
use Test::More tests => 3;
require 't/Schema.pm';

use_ok('SQL::DB::Functions');
use_ok('SQL::DB::Schema');

SQL::DB::Functions->import(qw/
    coalesce
/);


my $sql = SQL::DB::Schema->new(Schema->All);
my $artist = Artist->arow;
my $c = coalesce($artist->id, $artist->name)->as('idname');
ok($c eq 'COALESCE(t0.id, t0.name) AS idname', 'coalesce');
