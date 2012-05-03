package App::sqldb_schema;
use strict;
use warnings;
use Term::Prompt;
use File::Basename qw/dirname/;
use File::Slurp qw/read_file write_file/;
use File::ShareDir qw/dist_file/;
use File::Spec;
use Data::Dumper;
use DBI;
use Template::Tiny;

our $VERSION = '0.19_15';

sub opt_spec {
    (
        [ "username|u=s", "DSN user name" ],
        [ "password|p=s", "DSN password" ],
        [ "dbschema|d=s", "Database Schema name" ],
    );
}

sub arg_spec {
    (
        [ "dsn=s",     "DSN",             { required => 1 }, ],
        [ "package=s", "Package name",    { required => 1 }, ],
        [ "outfile=s", "output filename", { default  => '-' }, ],
    );
}

sub run {
    my ( $class, $opt ) = @_;

    $opt->{dsn} = 'dbi:SQLite:dbname=' . $opt->{dsn} if -f $opt->{dsn};

    my ( $scheme, $driver, $attr_string, $attr_hash, $driver_dsn ) =
      DBI->parse_dsn( $opt->dsn )
      or die "Could not parse DSN: " . $opt->dsn;

    if ( $driver ne 'SQLite' and not $opt->{username} ) {
        $opt->{username} = prompt( 'x', 'Username:', '', '' );
    }

    if ( $driver ne 'SQLite' and not $opt->{password} ) {
        $opt->{password} = prompt( 'p', 'Password:', '', '' );
        print "\n";
    }

    if ( $driver eq 'SQLite' and !$opt->{dbschema} ) {
        $opt->{dbschema} = 'main';
    }

    if ( $driver eq 'Pg' and !$opt->{dbschema} ) {
        $opt->{dbschema} = 'public';
    }

    my $dbh = DBI->connect( $opt->dsn, $opt->username, $opt->password )
      || die "Could not connect: " . DBI->errstr;

    my $sth =
      $dbh->table_info( '%', $opt->{dbschema}, '%',
        "'TABLE','VIEW','GLOBAL TEMPORARY','LOCAL TEMPORARY'" );

    my @columns;

    while ( my $table = $sth->fetchrow_arrayref ) {
        my $sth2 = $dbh->column_info( '%', '%', $table->[2], '%' );
        push( @columns, @{ $sth2->fetchall_arrayref } );
    }

    local $Data::Dumper::Indent   = 0;
    local $Data::Dumper::Maxdepth = 0;

    ( my $shortpkg = $opt->{package} ) =~ s/(.*)::.*/$1/;

    my $stash = {
        package    => $opt->{package},
        definition => Dumper( \@columns ),
        shortpkg   => $shortpkg,
        date       => scalar localtime,
        program    => __PACKAGE__,
        source     => $opt->dsn,
        driver     => $driver,
    };

    my $develt =
      File::Spec->catfile( dirname($0), File::Spec->updir, 'share',
        'schema.tmpl' );
    my $template = -e $develt ? $develt : dist_file( 'SQL-DB', 'schema.tmpl' );
    my $input = read_file($template);
    my $output;

    Template::Tiny->new->process( \$input, $stash, \$output );

    if ( $opt->outfile eq '-' ) {
        print $output;
    }
    else {
        write_file( $opt->outfile, $output );
    }
}

1;

# vim: set tabstop=4 expandtab:
