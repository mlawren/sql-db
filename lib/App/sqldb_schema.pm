package App::sqldb_schema;
use strict;
use warnings;
use Data::Dumper;
use DBI;
use File::Basename qw/dirname/;
use File::Slurp qw/read_file write_file/;
use File::ShareDir::ProjectDistDir qw/dist_file/;
use File::Spec;
use OptArgs;
use Template::Tiny;
use Term::Prompt;

our $VERSION = '0.191.0';

opt username => (
    isa     => 'Str',
    alias   => 'u',
    comment => 'DSN user name',
);

opt password => (
    isa     => 'Str',
    alias   => 'p',
    comment => 'DSN password',
);

opt dbschema => (
    isa     => 'Str',
    alias   => 'd',
    comment => 'Database Schema name',
);

arg database => (
    isa      => 'Str',
    comment  => 'filename (SQLite) or DBI connection string',
    required => 1,
);

arg package => (
    isa      => 'Str',
    comment  => 'Name of the generated Perl package',
    required => 1,
);

arg outfile => (
    isa     => 'Str',
    comment => 'Destination filename',
    default => '-',
);

sub run {
    my $opts = shift;

    $opts->{database} = 'dbi:SQLite:dbname=' . $opts->{database}
      if -f $opts->{database};

    my ( $scheme, $driver, $attr_string, $attr_hash, $driver_dsn ) =
      DBI->parse_dsn( $opts->{database} );

    die "Could not parse DSN: " . $opts->{database} . "\n" unless $driver;

    if ( $driver ne 'SQLite' and not $opts->{username} ) {
        $opts->{username} = prompt( 'x', 'Username:', '', '' );
    }

    if ( $driver ne 'SQLite' and not $opts->{password} ) {
        $opts->{password} = prompt( 'p', 'Password:', '', '' );
        print "\n";
    }

    if ( $driver eq 'SQLite' and !$opts->{dbschema} ) {
        $opts->{dbschema} = 'main';
    }

    if ( $driver eq 'Pg' and !$opts->{dbschema} ) {
        $opts->{dbschema} = 'public';
    }

    my $dbh =
      DBI->connect( $opts->{database}, $opts->{username}, $opts->{password} )
      || die "connect: " . DBI->errstr;

    my $sth =
      $dbh->table_info( '%', $opts->{dbschema}, '%',
        "'TABLE','VIEW','GLOBAL TEMPORARY','LOCAL TEMPORARY'" );

    my @columns;

    while ( my $table = $sth->fetchrow_arrayref ) {
        my $sth2 = $dbh->column_info( '%', '%', $table->[2], '%' );
        push( @columns, @{ $sth2->fetchall_arrayref } );
    }

    local $Data::Dumper::Indent   = 0;
    local $Data::Dumper::Maxdepth = 0;

    ( my $shortpkg = $opts->{package} ) =~ s/(.*)::.*/$1/;

    my $stash = {
        package    => $opts->{package},
        definition => Dumper( \@columns ),
        shortpkg   => $shortpkg,
        date       => scalar localtime,
        program    => __PACKAGE__,
        source     => $opts->{database},
        driver     => $driver,
    };

    my $template = dist_file( 'SQL-DB', 'schema.tmpl' );
    my $input = read_file($template);
    my $output;

    Template::Tiny->new->process( \$input, $stash, \$output );

    if ( $opts->{outfile} eq '-' ) {
        print $output;
    }
    else {
        write_file( $opts->{outfile}, $output );
    }
}

1;

# vim: set tabstop=4 expandtab:
