# Load the Module::Install bundled in ./inc/
use inc::Module::Install;

if ( eval { require App::Dispatcher } ) {
    App::Dispatcher::run_app_dispatcher('App::sqldb_schema');
}

name 'SQL-DB';
license 'GPL';
all_from 'lib/SQL/DB.pm';

requires 'Moo'             => '0.009011';
requires 'Log::Any'        => 0;
requires 'Sub::Install'    => 0.04;
requires 'Sub::Exporter'   => 0.04;
requires 'DBIx::Connector' => 0;

test_requires 'File::Temp'        => 0;
test_requires 'Test::More'        => 0;
test_requires 'Test::Database'    => 1.11;
test_requires 'Test::Differences' => 0;
test_requires 'DBD::SQLite'       => '1.26';
test_requires 'FindBin'           => 0;

feature 'sqldb-schema',
  -default       => 1,
  'DBI'          => 0,
  'Term::Prompt' => 0,
  'File::Slurp'  => 0,
  'Data::Dumper' => 0,
  'Perl::Tidy'   => 0;

feature 'Deploy',
  -default => 1,
  'YAML'   => 0;

feature 'sqldb-deploy',
  -default => 1,
  'YAML'   => 0;

install_script 'bin/sqldb-schema';

repository 'http://github.com/mlawren/sql-db.git/';

auto_install;

WriteAll;

