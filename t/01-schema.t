use strict;
use warnings;
use Test::More tests => 17;

BEGIN { use_ok('SQL::DB::Schema');}
require_ok('t/testlib/Schema.pm');


can_ok('SQL::DB::Schema', qw(new define tables table arow insert update delete select));

my $sql;
my @schema = Schema->get;
ok(scalar @schema, 'Have schema');

$sql = SQL::DB::Schema->new();
isa_ok($sql, 'SQL::DB::Schema', '->new empty');

my $table;

eval {$table = $sql->define};
like($@, qr/usage: define/, '->define usage');

eval {$table = $sql->define('cds')};
like($@, qr/usage: define/, '->define usage def');

eval {$sql->table('cds');};
like($@, qr/has not been defined/, '->table not defined');

eval {$sql->arow;};
like($@, qr/usage: arow/, '->arow usage');

eval {$sql->arow('Unknown');};
like($@, qr/has not been defined/, '->arow table not defined');

eval {$sql = SQL::DB::Schema->new({});};
like($@, qr/usage: new/, '->new requires arrayref');

@schema = Schema->get;
$sql = SQL::DB::Schema->new(@schema);
isa_ok($sql, 'SQL::DB::Schema', '->new with array');
isa_ok($sql->table('cds'), 'SQL::DB::Table');

@schema = Schema->get;
eval{use warnings FATAL => 'all'; $sql->define($schema[0]);};
like($@, qr/already defined/, 'redefine check');

$sql = SQL::DB::Schema->new(Schema->get);

eval {$sql->select;};
#like($@, qr/query badly defined/, '->query badly defined');

eval {$sql->select({});};
#like($@, qr/query badly defined/, '->query badly defined with hash');

isa_ok($sql->select(columns => []), 'SQL::DB::Query::Select', '->query SELECT');

eval {$sql->insert(insert => []);};
like($@, qr/unknown argument for/m, '->query INSERT usage');

isa_ok($sql->insert(columns => [$sql->arow('cds')->_columns]), 'SQL::DB::Query::Insert', '->query INSERT');


