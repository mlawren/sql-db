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



my $i = $sql->query(
    insert => [$cd->artist, $cd->year, $cd->title],
    values => ['Queen', 1987, 'A Kind of Magic' ],
);
print $i,"\n";


my $cd2 = $sql->row('CD');

my $q = $sql->query(
    select   => [$cd->_columns],
    where    => (($cd->artist == $cd->artist->id) & ($cd->artist->id == 23))
       & $cd->id->in(
        $sql->query(
            select   => [$cd2->_columns],
            distinct => [$cd2->year, $cd2->title],
            where    => ($cd2->id == 23),
        )
    ),
    order_by => [$cd->id],
);


print $q;


