# Load the Module::Install bundled in ./inc/
use inc::Module::Install;
    
name            'SQL-DB';
license         'GPL';
all_from        'lib/SQL/DB.pm';

requires        'DBI'                   => 0;
requires        'Scalar::Util'          => 0;
requires        'Class::Accessor::Fast' => 0;
requires        'Return::Value'         => 0;

build_requires  'File::Temp'            => 0;
build_requires  'Test::More'            => 0;
build_requires  'Test::Exception'       => 0;
build_requires  'Test::Memory::Cycle'   => 0;
build_requires  'Test::Pod'             => 0;
build_requires  'Test::Pod::Coverage'   => 0;
build_requires  'PadWalker'             => 0;
build_requires  'DBD::SQLite'           => 1.14;

auto_install;

WriteAll;

sub MY::postamble {'
testpg:
	TEST_PG=1 $(MAKE) test
'}
