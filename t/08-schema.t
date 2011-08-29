use strict;
use warnings;
use Test::More;
use SQL::DB::Schema;

can_ok(
    'SQL::DB::Schema', qw/
      new
      not_known
      define
      srow
      urow
      get_schema
      /
);

my $schema = SQL::DB::Schema->new( name => 'test' );
isa_ok $schema, 'SQL::DB::Schema';
is $schema, SQL::DB::Schema::get_schema('test'), 'get_schema()';
is SQL::DB::Schema::get_schema('unknown'), undef, 'get_schema(unknown)';

ok $schema->not_known('unknown'), 'not_known()';

eval { $schema->srow('unknown') };
ok $@, 'Bombs on unknown tables';

$schema->define(
    [
        [ undef, undef, 'users', 'name', undef, 'varchar' ],
        [ undef, undef, 'users', 'dob',  undef, 'TIMESTAMP' ],
    ]
);

ok !$schema->not_known('users'), 'table users is NOT not_known()';

my $srow = $schema->srow('users');
isa_ok $srow, 'SQL::DB::Schema::test::Srow::users';
isa_ok $srow, 'SQL::DB::Expr';
can_ok $srow, 'name', 'dob';
is $srow->name->_btype,     'varchar',     'name is varchar';
is $srow->dob->_btype,      'timestamp',   'dob is timestamp';
is $srow->name->_as_string, 'users0.name', 'as string';
my $sx = $srow->name == 'mark';
is $sx->_as_string, 'users0.name = ?', $sx->_as_string;
is_deeply $sx->_btypes, ['varchar'], 'btypes';

my $urow = $schema->urow('users');
isa_ok $urow, 'SQL::DB::Schema::test::Urow::users';
isa_ok $urow, 'SQL::DB::Expr';
can_ok $urow, 'name', 'dob';
is $urow->name->_btype,     'varchar',   'name is varchar';
is $urow->dob->_btype,      'timestamp', 'dob is timestamp';
is $urow->name->_as_string, 'name',      'as string';
my $ux = $urow->name == 'mark';
is $ux->_as_string, 'name = ?', $ux->_as_string;
is_deeply $ux->_btypes, ['varchar'], 'btypes';

done_testing;
