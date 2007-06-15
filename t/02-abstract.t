use strict;
use warnings;
use Test::More tests => 8;

BEGIN { use_ok('SQL::DB::Schema');}
require_ok('t/testlib/Schema.pm');

my @schema = Schema->get;
ok(scalar @schema, 'Have schema');

my $sql = SQL::DB::Schema->new(@schema);
isa_ok($sql, 'SQL::DB::Schema');

my $cd  = $sql->row('CD');

isa_ok($cd, 'SQL::DB::Abstract::cd', 'Abstract Row');

isa_ok($cd->id, 'SQL::DB::Abstract::cd::id', 'Abstract Column');

isa_ok($cd->artist->id, 'SQL::DB::Abstract::artist::id',
    'Abstract Foreign Column');

isa_ok($cd->artist->id->_arow, 'SQL::DB::Abstract::artist',
    'Abstract Foreign Row');


