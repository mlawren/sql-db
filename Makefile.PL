die "SQL::DB is not supported on Perl v$]" if $] eq '5.010000';

# Load the Module::Install bundled in ./inc/
use inc::Module::Install;

name 'SQL-DB';
license 'GPL';
version '0.971.0_1';
all_from 'lib/SQL/DB.pm';
readme_from 'lib/SQL/DB.pod';

feature 'SQL::DB',
  -default          => 1,
  'DBIx::Connector' => 0,
  'Log::Any'        => 0,
  'Moo'             => '0.009011',
  'Sub::Install'    => 0.04,
  'Sub::Exporter'   => 0.04,
  ;

feature 'SQL::DBx::Deploy',
  -default         => 1,
  'File::ShareDir' => 0,
  'File::Slurp'    => 0,
  'File::Temp'     => 0,
  'Log::Any'       => 0,
  'Moo::Role'      => 0,
  'Path::Class'    => 0,
  ;

feature 'SQL::DBx::SQLite',
  -default      => 1,
  'Digest::SHA' => 0,
  'Log::Any'    => 0,
  'Moo::Role'   => 0,
  ;

feature 'Test::SQL::DB',
  -default         => 1,
  'File::ShareDir' => 0,
  'Log::Any'       => 0,
  'Moo::Role'      => 0,
  'Path::Class'    => 0,
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

