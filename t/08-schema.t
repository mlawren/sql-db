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
      load_schema
      /
);

my $schema = SQL::DB::Schema->new( name => 'test' );
isa_ok $schema, 'SQL::DB::Schema';

#is $schema, SQL::DB::Schema::load_schema('test'), 'load_schema()';
#is SQL::DB::Schema::load_schema('unknown'), undef, 'load_schema(unknown)';

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

my $irow = $schema->irow('users');
isa_ok $irow, 'CODE';
is $irow->( 'col1', 'col2' ), 'users(col1,col2)', 'irow expansion';

my $srow = $schema->srow('users');
isa_ok $srow, 'SQL::DB::Schema::test::Srow::users';
isa_ok $srow, 'SQL::DB::Expr';
can_ok $srow, 'name', 'dob';
is $srow->name->_btype,     'varchar',     'name is varchar';
is $srow->dob->_btype,      'timestamp',   'dob is timestamp';
is $srow->name->_as_string, 'users0.name', 'as string';
my $sx = $srow->name == 'mark';
like $sx->_as_string, qr/^users0.name = q{mark}/, $sx->_as_string;

my $urow = $schema->urow('users');
isa_ok $urow, 'SQL::DB::Schema::test::Urow::users';
isa_ok $urow, 'SQL::DB::Expr';
can_ok $urow, 'name', 'dob';
is $urow->name->_btype,     'varchar',   'name is varchar';
is $urow->dob->_btype,      'timestamp', 'dob is timestamp';
is $urow->name->_as_string, 'name',      'as string';
my $ux = $urow->name == SQL::DB::Expr::_bval('mark');
like $ux->_as_string, qr/^name = bv{mark}::varchar/, $ux->_as_string;

done_testing;
