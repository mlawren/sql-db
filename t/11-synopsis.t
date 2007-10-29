use strict;
use warnings;
use Test::More tests => 4;
require_ok('t/TestLib.pm');

open(SQL, 'lib/SQL/DB.pm') || die "open: $!";

my $dbi = TestLib->dbi;

my @lines;
while (my $line = <SQL>) {
    next unless ($line =~ m/^\s+use SQL::DB/);
    push(@lines, $line);
    while (my $line = <SQL>) {
        $line =~ s/dbi:SQLite:\/tmp\/sqldbtest\.db/$dbi/;
        last if($line =~ m/lives in Springfield/);
        push(@lines, $line);
    }
    last;
}
push(@lines, 'return @items;');

my @res = eval "@lines";
ok(!$@, 'Eval '.($@ ? $@ : ''));
ok(scalar(@res) > 0, 'Result > 0');
isa_ok($res[0], 'SQL::DB::Row::persons.name_persons.age_addresses.city');
