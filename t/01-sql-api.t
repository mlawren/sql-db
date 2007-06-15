use strict;
use warnings;
use Test::More tests => 25;

BEGIN { use_ok('SQL::API');}
require_ok('t/testlib/Schema.pm');


can_ok('SQL::API', qw(new define tables table row query));

my $sql;
my @schema = Schema->get;
ok(scalar @schema, 'Have schema');

$sql = SQL::API->new();
isa_ok($sql, 'SQL::API', '->new empty');

my $table;

eval {$table = $sql->define};
like($@, qr/usage: define/, '->define usage');

eval {$table = $sql->define('CD')};
like($@, qr/usage: define/, '->define usage def');

eval {$sql->table('CD');};
like($@, qr/has not been defined/, '->table not defined');

eval {$sql->row;};
like($@, qr/usage: row/, '->row usage');

eval {$sql->row('Unknown');};
like($@, qr/has not been defined/, '->row table not defined');

@schema = Schema->get;
$sql = SQL::API->new(@schema);
isa_ok($sql, 'SQL::API', '->new with array');
isa_ok($sql->table('CD'), 'SQL::API::Table');

@schema = Schema->get;
$sql = SQL::API->new([@schema]);
isa_ok($sql, 'SQL::API', '->new with arrayref');
isa_ok($sql->table('CD'), 'SQL::API::Table');

eval {$sql = SQL::API->new({});};
like($@, qr/requires an array or arrayref/, '->new requires arrayref');

@schema = Schema->get;
$sql = SQL::API->new(@schema);
isa_ok($sql, 'SQL::API', '->new with array');
isa_ok($sql->table('CD'), 'SQL::API::Table');

{
    my $warning;
    local $SIG{__WARN__} = sub {
        $warning = $_[0];
    };

    $sql->define('CD', {});
    like($warning, qr/Redefining table/, 'redefine usage array');
}


{
    my $warning;
    local $SIG{__WARN__} = sub {
        $warning = $_[0];
    };

    $sql->define('CD', {});
    like($warning, qr/Redefining table/, 'redefine usage arrayref');
}


$sql = SQL::API->new(Schema->get);

eval {$sql->query;};
like($@, qr/query badly defined/, '->query badly defined');

eval {$sql->query({});};
like($@, qr/query badly defined/, '->query badly defined with hash');

isa_ok($sql->query(select => []), 'SQL::API::Select', '->query SELECT');

isa_ok($sql->query({select => []}), 'SQL::API::Select', '->query SELECT hash');

eval {$sql->query(insert => []);};
like($@, qr/insert needs/m, '->query INSERT usage');

isa_ok($sql->query(insert => [$sql->row('CD')->_columns]), 'SQL::API::Insert', '->query INSERT');


