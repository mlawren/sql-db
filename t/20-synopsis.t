use strict;
use warnings;
use Test::More tests => 1;
use File::Slurp;
use SQL::DB;

unlink "sqldbtest$$.db";

my $perl = read_file('lib/SQL/DB.pm');

$perl =~ s/.*=head1 SYNOPSIS//s;
$perl =~ s/=head1 DESCRIPTION.*//s;

$perl = "use strict; use warnings; \n" . $perl;

eval $perl;

my $err = $@;

unlink "sqldbtest$$.db";

if ($err) {
    my $i = 1;
    foreach my $line ( split( /\n/, $perl ) ) {
        printf( "%-03s %s\n", $i++, $line );
    }
    die $err;
}

ok(1);

