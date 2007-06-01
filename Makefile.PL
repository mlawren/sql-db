# Load the Module::Install bundled in ./inc/
use inc::Module::Install;
    
name            'SQL-API';
license         'GPL';
all_from        'lib/SQL/API.pm';
#build_requires  'Test::More'        => 0;
    
auto_install;

WriteAll;
