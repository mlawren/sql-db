# Load the Module::Install bundled in ./inc/
use inc::Module::Install;

name 'SQL-DB';
license 'GPL';
all_from 'lib/SQL/DB.pm';

requires 'Moo'             => 0;
requires 'Log::Any'        => 0;
requires 'Sub::Install'    => 0.04;
requires 'Sub::Exporter'   => 0.04;
requires 'DBIx::Connector' => 0;

build_requires 'File::Temp'        => 0;
build_requires 'Test::More'        => 0;
build_requires 'Test::Database'    => 0;
build_requires 'Test::Differences' => 0;
build_requires 'DBD::SQLite'       => '1.26';

feature 'sqldb-schema',
  -default       => 1,
  'Term::Prompt' => 0,
  'File::Slurp'  => 0,
  'Perl::Tidy'   => 0;

feature 'Deploy',
  -default => 0,
  'YAML'   => 0;

repository 'http://github.com/mlawren/sql-db.git/';

auto_install;

WriteAll;

