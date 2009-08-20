use strict;
use warnings;
use Test::More tests => 29;
use Test::Memory::Cycle;

BEGIN {
    use_ok('SQL::DB::Schema::Table');
}
require_ok('t/TestLib.pm');


can_ok('SQL::DB::Schema::Table', qw(
    new
    setup_schema
    setup_table
    setup_class
    setup_columns
    setup_primary
    setup_unique
    setup_index
    setup_foreign
    setup_type_mysql
    setup_engine_mysql
    setup_default_charset_mysql
    setup_default_charset_pg
    setup_tablespace_pg
    setup_trigger
    name
    class
    columns
    column_names
    column_names_ordered
    column
    primary_columns
    schema
    arow
    set_db_type
    db_type
    sql_create_table
    sql_create_indexes
    sql_triggers
    sql_create
));

my $table = SQL::DB::Schema::Table->new(@{TestLib->Artist});
isa_ok($table, 'SQL::DB::Schema::Table');
isa_ok($table->arow, 'SQL::DB::Schema::ARow::artists');
like($table->name, qr/artists/, 'name');
ok($table->columns, 'columns');
memory_cycle_ok($table, 'memory ok');

my @cols = $table->columns;
ok(@cols == 3, '3 columns');
isa_ok($cols[0], 'SQL::DB::Schema::Column');

my @colnames = $table->column_names;
ok(@colnames == 3, '3 column names');
@colnames = $table->column_names_ordered;
ok(@colnames == 3, '3 column names');
ok($colnames[0] eq 'id', 'First col is id');

isa_ok($table->column('name'), 'SQL::DB::Schema::Column');
ok($table->column('name')->name eq 'name', 'Column name is name.');


like($table->sql_create_table, qr/CREATE TABLE artists/, 'SQL');
like($table->sql_create_table, qr/PRIMARY KEY/, 'SQL');
like($table->sql_create_table, qr/UNIQUE/, 'SQL');

is($table->sql_create_table,'CREATE TABLE artists (
    id              INTEGER        NOT NULL,
    name            VARCHAR(255)   NOT NULL UNIQUE,
    ucname          VARCHAR(255)   NOT NULL,
    PRIMARY KEY(id),
    UNIQUE (name)
)', 'create table no database');


$table->set_db_type('mysql');

is($table->sql_create_table,'CREATE TABLE artists (
    id              INTEGER        NOT NULL,
    name            VARCHAR(255)   NOT NULL UNIQUE,
    ucname          VARCHAR(255)   NOT NULL,
    PRIMARY KEY(id),
    UNIQUE (name)
) ENGINE=InnoDB', 'create table mysql');

my $cd = SQL::DB::Schema::Table->new(@{TestLib->CD});
$cd->column('artist')->references($table->column('id'));

is($cd->column('artist')->references, $table->column('id'), 'references');

memory_cycle_ok($cd, 'cd memory');
memory_cycle_ok($table, 'table memory');

$cd->column('artist')->references($table->column('id'));

is($cd->column('artist')->deferrable, 'INITIALLY IMMEDIATE', 'deferrable');
is($cd->sql_create_table, 'CREATE TABLE cds (
    id              INTEGER        NOT NULL,
    title           VARCHAR(255)   NOT NULL,
    year            INTEGER        NOT NULL,
    artist          INTEGER        NOT NULL REFERENCES artists(id) DEFERRABLE INITIALLY IMMEDIATE,
    PRIMARY KEY(id),
    UNIQUE (title, artist)
)', 'CD as string');

my $default = SQL::DB::Schema::Table->new(@{TestLib->Default});
is($default->column('binary')->type, 'BLOB', 'column type');
is($default->column('binary')->bind_type, undef, 'undef bind_column type');
$default->set_db_type('Pg');
is($default->column('binary')->type, 'BYTEA', 'pg column type');
is($default->column('binary')->bind_type, 'Pg bind type', 'pg bind_column type');


