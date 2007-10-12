use strict;
use warnings;
use Test::More;

BEGIN {
    if (!eval {require DBD::SQLite;1;}) {
        plan skip_all => "DBD::SQLite not installed: $@";
    }
    else {
        plan tests => 2;
    }

}
END {
    unlink "/tmp/sqldbtest.db";
}


open(SQL, 'lib/SQL/DB.pm') || die "open: $!";

my @lines;
while (my $line = <SQL>) {
    next unless ($line =~ m/^\s+use SQL::DB/);
    push(@lines, $line);
    while (my $line = <SQL>) {
        last if($line =~ m/lives in Springfield/);
        push(@lines, $line);
    }
    last;
}
push(@lines, 'return @items;');

my $res = eval "@lines";
if ($@) {
    die $@;
}
ok(!$@, 'Eval ok');
ok($res > 0, 'Result > 0');

