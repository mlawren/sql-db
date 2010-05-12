use strict;
use warnings;
use Test::More tests => 1;
use File::Slurp;
use SQL::DB; # this doesn't export @EXPORT_DEFAULT when eval'd. Why?

unlink "sqldbtest$$.db";

my $perl = read_file( 'lib/SQL/DB.pm');

$perl =~ s/.*=head1 SYNOPSIS//s;
$perl =~ s/=head1 DESCRIPTION.*//s;

eval 'use strict; use warnings; '. $perl;

my $err = $@;

unlink "sqldbtest$$.db";

die $err if ( $err ); # die here so we see test output

ok( 1 );

