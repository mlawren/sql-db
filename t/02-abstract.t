use strict;
use warnings;
use Test::More tests => 8;

BEGIN { use_ok('SQL::DB::Schema');}
require_ok('t/testlib/Schema.pm');

my @schema = Schema->get;
ok(scalar @schema, 'Have schema');

my $sql = SQL::DB::Schema->new(@schema);
isa_ok($sql, 'SQL::DB::Schema');

my $cd  = $sql->arow('cds');

isa_ok($cd, 'SQL::DB::Abstract::cds', 'Abstract Row');

isa_ok($cd->id, 'SQL::DB::Abstract::cds::id', 'Abstract Column');

isa_ok($cd->artist->id, 'SQL::DB::Abstract::artists::id',
    'Abstract Foreign Column');

isa_ok($cd->artist->id->_arow, 'SQL::DB::Abstract::artists',
    'Abstract Foreign Row');


