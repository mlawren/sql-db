die "SQL::DB is not supported on Perl v$]" if $] eq '5.010000';

# Load the Module::Install bundled in ./inc/
use inc::Module::Install;

name 'SQL-DB';
license 'GPL';
version '0.191.0_2';
all_from 'lib/SQL/DB.pm';
readme_from 'lib/SQL/DB.pod';

requires 'DBIx::Connector' => 0;
requires 'Log::Any'        => 0;
requires 'Moo'             => '0.009011';
requires 'Sub::Install'    => 0.04;
requires 'Sub::Exporter'   => 0.04;

feature 'Deploy',
  -default      => 1,
  'File::Slurp' => 0,
  'File::Temp'  => 0,
  'Path::Class' => 0,
  ;

feature 'SQLite',
  -default      => 1,
  'Digest::SHA' => 0,
  ;

test_requires 'DBD::SQLite'                 => '1.35';
test_requires 'File::Temp'                  => 0;
test_requires 'FindBin'                     => 0;
test_requires 'Log::Any::Adapter::Dispatch' => 0;
test_requires 'Path::Class'                 => 0;
test_requires 'Test::Database'              => 1.11;
test_requires 'Test::Differences'           => 0;
test_requires 'Test::More'                  => 0;

repository 'http://github.com/mlawren/sql-db/';

install_share 'share';

auto_install;

WriteAll;

