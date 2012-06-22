die "SQL::DB is not supported on Perl v$]" if $] eq '5.010000';

# Load the Module::Install bundled in ./inc/
use inc::Module::Install;

name 'SQL-DB';
license 'GPL';
version '0.191.0_1';
all_from 'lib/SQL/DB.pm';
readme_from 'lib/SQL/DB.pod';

requires 'DBIx::Connector' => 0;
requires 'Log::Any'        => 0;
requires 'Moo'             => '0.009011';
requires 'OptArgs'         => 0;
requires 'Sub::Install'    => 0.04;
requires 'Sub::Exporter'   => 0.04;

test_requires 'DBD::SQLite'       => '1.35';
test_requires 'File::Temp'        => 0;
test_requires 'FindBin'           => 0;
test_requires 'Path::Class'       => 0;
test_requires 'Test::Database'    => 1.11;
test_requires 'Test::Differences' => 0;
test_requires 'Test::More'        => 0;

feature 'sqldb-schema',
  -default                         => 1,
  'Data::Dumper'                   => 0,
  'DBI'                            => 0,
  'File::Basename'                 => 0,
  'File::ShareDir::ProjectDistDir' => 0,
  'File::Slurp'                    => 0,
  'File::Spec'                     => 0,
  'Getopt::Long::Descriptive'      => 0,
  'Term::Prompt'                   => 0,
  'Template::Tiny'                 => 0,
  ;

feature 'Deploy',
  -default => 1,
  'YAML'   => 0,
  ;

feature 'SQLite',
  -default                      => 1,
  'Digest::SHA'                 => 0,
  'Log::Any::Adapter::Dispatch' => 0,
  ;

install_share 'share';
install_script 'bin/sqldb-schema';

repository 'http://github.com/mlawren/sql-db/';

auto_install;

WriteAll;

