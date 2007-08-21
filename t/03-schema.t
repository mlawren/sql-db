use strict;
use warnings;
use Test::More tests => 4;

BEGIN { use_ok('SQL::DB::Schema');}
require_ok('t/Schema.pm');


can_ok('SQL::DB::Schema', qw(new define tables table query));

my $sql;

$sql = SQL::DB::Schema->new(Schema->All);
isa_ok($sql, 'SQL::DB::Schema', '->new empty');
__END__

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

my @many = $sql->table('artists')->has_many;
ok(@many > 0, 'Artists has many something');
isa_ok($many[0],'SQL::DB::Column', 'Artists has many');


