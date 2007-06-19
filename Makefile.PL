# Load the Module::Install bundled in ./inc/
use inc::Module::Install;
    
name            'SQL-DB';
license         'GPL';
all_from        'lib/SQL/DB.pm';
requires        'Scalar::Util'       => 0;
requires        'Class::Struct'      => 0;
build_requires  'Test::More'        => 0;
    
auto_install;

WriteAll;
