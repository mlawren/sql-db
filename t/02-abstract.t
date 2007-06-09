use strict;
use warnings;
use Test::More tests => 8;

BEGIN { use_ok('SQL::API');}
require_ok('t/testlib/Schema.pm');

my @schema = Schema->get;
ok(scalar @schema, 'Have schema');

my $sql = SQL::API->new(@schema);
isa_ok($sql, 'SQL::API');

my $cd  = $sql->row('CD');

isa_ok($cd, 'SQL::API::Abstract::CD', 'Abstract Row');

isa_ok($cd->id, 'SQL::API::Abstract::CD::id', 'Abstract Column');

isa_ok($cd->artist->id, 'SQL::API::Abstract::Artist::id',
    'Abstract Foreign Column');

isa_ok($cd->artist->id->_arow, 'SQL::API::Abstract::Artist',
    'Abstract Foreign Row');


