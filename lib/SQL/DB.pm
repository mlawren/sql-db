package SQL::DB;
use strict;
use warnings;
use Moo;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;
use Storable qw/dclone/;
use DBI ':sql_types', 'looks_like_number';
use DBIx::Connector;
use SQL::DB::Schema qw/load_schema/;
use SQL::DB::Expr qw/:all/;
use SQL::DB::Iter;

use constant SQL_FUNCTIONS => qw/
  query
  AND
  OR
  sql_and
  sql_case
  sql_cast
  sql_coalesce
  sql_concat
  sql_count
  sql_exists
  sql_func
  sql_length
  sql_lower
  sql_max
  sql_min
  sql_or
  sql_substr
  sql_sum
  sql_table
  sql_upper
  sql_values
  /;

use Sub::Exporter -setup => {
    exports => [SQL_FUNCTIONS],
    groups  => {
        all     => [SQL_FUNCTIONS],
        default => [],
    },
};

our $VERSION = '0.97_3';

### CLASS FUNCTIONS ###

sub query {
    return $_[0] if ( @_ == 1 and ref $_[0] eq 'SQL::DB::Expr' );
    my @statements;
    foreach my $item (@_) {
        if ( eval { $item->isa('SQL::DB::Expr') } ) {
            push( @statements, '    ', $item, "\n" );
        }
        elsif ( ref $item eq 'ARRAY' ) {
            push( @statements, '    ', _bexpr_join( ",\n    ", @$item ), "\n" );
        }
        elsif ( ref $item eq 'SCALAR' ) {
            push( @statements, $$item . "\n" );
        }
        elsif ( ref $item ) {
            confess "Invalid query element: " . $item;
        }
        else {    # ( ref $item eq '' ) {
            ( my $tmp = uc($item) ) =~ s/_/ /g;
            push( @statements, $tmp . "\n" );
        }
    }

    my $e = _expr_join( '', @statements );
    $e->_txt( $e->_txt . "\n" ) unless ( $e->_txt =~ /\n$/ );
    return $e;
}

sub sql_and { _bexpr_join( ' AND ', @_ ) }

sub sql_case {
    @_ || croak 'case([$expr,] when => $expr, then => $val,[else...])';

    my @items = map {
            ( ref $_ eq '' && $_ =~ m/^((when)|(then)|(else))$/i ) ? uc($_)
          : looks_like_number($_) ? $_
          : _bexpr($_)
    } @_;

    return _expr_join( ' ', 'CASE', @items, 'END' );
}

sub sql_coalesce { sql_func( 'COALESCE', @_ ) }

sub sql_cast {
    return _expr_join( ' ', 'CAST(', $_[0], 'AS', $_[2], ')' );
}

sub sql_concat { _expr_binary( '||', $_[0], $_[1] ) }

sub sql_count {
    my $e = sql_func( 'COUNT', @_ );
    $e->_btype( { default => SQL_INTEGER } );
    return $e;
}

sub sql_exists { 'EXISTS(' . query(@_) . ')' }

sub sql_func {
    my $func = shift;
    return $func . '(' . _bexpr_join( ', ', @_ ) . ')';
}

sub sql_length { sql_func( 'LENGTH', @_ ) }

sub sql_lower { sql_func( 'LOWER', @_ ) }

sub sql_or { _bexpr_join( ' OR ', @_ ) }

sub sql_max { sql_func( 'MAX', @_ ) }

sub sql_min { sql_func( 'MIN', @_ ) }

sub sql_substr { sql_func( 'SUBSTR', @_ ) }

sub sql_sum { sql_func( 'SUM', @_ ) }

sub sql_table {
    my $table = shift;
    return SQL::DB::Expr->new( _txt => $table . '(' . join( ',', @_ ) . ')' );
}

sub sql_upper { sql_func( 'UPPER', @_ ) }

sub sql_values { sql_func( 'VALUES', @_ ) }

### OBJECT IMPLEMENTATION ###

has 'conn' => ( is => 'ro' );

has 'dbd' => ( is => 'ro' );

has 'schema' => ( is => 'ro' );

has 'prepare_cached' => (
    is      => 'rw',
    default => sub { 1 },
);

has '_current_timestamp' => ( is => 'rw', init_arg => undef );

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    my %args  = @_;

    $args{dsn} || confess 'Missing argument: dsn';
    my ( $dbi, $dbd, @rest ) = DBI->parse_dsn( $args{dsn} );

    $args{dbd} = $dbd;

    if ( my $sname = $args{schema} ) {
        $sname .= '::' . $dbd;
        $args{schema} = load_schema($sname);
    }
    else {
        ( my $sname = "$args{dsn}" ) =~ s/[^a-zA-Z]/_/g;
        $args{schema} = SQL::DB::Schema->new( name => $sname );
    }

    my $attr = {
        PrintError => 0,
        ChopBlanks => 1,
        $dbd eq 'Pg'     ? ( pg_enable_utf8    => 1 ) : (),
        $dbd eq 'SQLite' ? ( sqlite_unicode    => 1 ) : (),
        $dbd eq 'mysql'  ? ( mysql_enable_utf8 => 1 ) : (),
        %{ $args{attr} || {} },
        RaiseError => 1,
        AutoCommit => 1,
        Callbacks  => {
            connected => sub {
                my $h = shift;
                if ( $dbd eq 'Pg' ) {
                    $h->do('SET client_min_messages = WARNING;');
                    $h->do("SET TIMEZONE TO 'UTC';");
                }
                elsif ( $dbd eq 'SQLite' ) {
                    $h->do('PRAGMA foreign_keys = ON;');
                }
                return;
            },
        }
    };

    $args{conn} =
      DBIx::Connector->new( $args{dsn}, $args{username}, $args{password},
        $attr );

    $args{conn}->mode('fixup');

    return $class->$orig(%args);
};

# For our extensions to 'around' or 'after'
sub BUILD {
}

sub connect {
    my $class    = shift;
    my $dsn      = shift;
    my $username = shift;
    my $password = shift;
    my $attr     = shift || {};

    return $class->new(
        dsn      => $dsn,
        username => $username,
        password => $password,
        attr     => $attr,
    );
}

sub _load_tables {
    my $self = shift;

    my %seen;
    foreach my $table (@_) {
        next if $seen{$table};
        $log->debug( 'Attempting to load schema for table: ' . $table );
        my $sth = $self->conn->dbh->column_info( '%', '%', $table, '%' );
        $self->schema->define( $sth->fetchall_arrayref );
        $seen{$table}++;
    }
}

sub urow {
    my $self = shift;

    if ( my @unknown = $self->schema->not_known(@_) ) {
        $self->_load_tables(@unknown);
    }

    return $self->schema->urow(@_);
}

sub srow {
    my $self = shift;

    if ( my @unknown = $self->schema->not_known(@_) ) {
        $self->_load_tables(@unknown);
    }

    return $self->schema->srow(@_);
}

sub sth {
    my $self    = shift;
    my $prepare = $self->prepare_cached ? 'prepare_cached' : 'prepare';
    my $query   = eval { query(@_) };

    if ( !defined $query ) {
        confess "Bad Query: $@";
    }

    $log->debug(
        $self->query_as_string( $query->_as_string, @{ $query->_bvalues } ) );

    #    $log->debugf( $query->_as_string, @{ $query->_bvalues } );

    my $wantarray = wantarray;

    return $self->conn->run(
        sub {
            my $dbh = $_;
            my $sth = eval { $dbh->$prepare( $query->_as_string ) };
            if ($@) {
                die 'Error: '
                  . $self->query_as_string( $query->_as_string,
                    @{ $query->_bvalues } )
                  . "\n$@";
            }

            my $i     = 0;
            my $types = dclone $query->_btypes;
            foreach my $val ( @{ $query->_bvalues } ) {
                $i++;
                my $type = shift @$types;
                my $btype = eval { $type->{ $self->dbd } || $type->{default} };
                $sth->bind_param( $i, $val, $btype );

#                $log->debugf('binding param %s %s as type %s', $i, $val, $btype );
            }

            {
                no warnings 'uninitialized';

                my $rv = eval { $sth->execute };
                if ($@) {
                    die 'Error: '
                      . $self->query_as_string( $query->_as_string,
                        @{ $query->_bvalues } )
                      . "\n$@";
                }
                $log->debug( "-- Result:", $rv );
                return $wantarray ? ( $sth, $rv ) : $sth;
            }
        },
    );
}

sub do {
    my $self = shift;
    my ( $sth, $rv ) = $self->sth(@_);
    $sth->finish();
    return $rv;
}

sub iter {
    my $self = shift;
    my ( $sth, $rv ) = $self->sth(@_);
    return SQL::DB::Iter->new( sth => $sth );
}

sub fetch {
    my $self = shift;
    return $self->iter(@_)->all;
}

sub fetch1 {
    my $self  = shift;
    my $iter  = $self->iter(@_);
    my $first = $iter->next;

    $iter->finish;
    return $first;
}

sub current_timestamp {
    my $self = shift;
    return $self->_current_timestamp if $self->_current_timestamp;

    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = gmtime;
    $mon  += 1;
    $year += 1900;
    return sprintf( '%04d-%02d-%02d %02d:%02d:%02d',
        $year, $mon, $mday, $hour, $min, $sec );
}

sub txn {
    my $wantarray = wantarray;

    my $self          = shift;
    my $set_timestamp = !$self->_current_timestamp;

    if ($set_timestamp) {
        $log->debug('BEGIN TRANSACTION;');
        $self->_current_timestamp( $self->current_timestamp );
    }

    my @ret = $self->conn->txn(@_);

    if ($set_timestamp) {
        $log->debug('COMMIT;');
        $self->_current_timestamp(undef);
    }

    return $wantarray ? @ret : $ret[0];
}

sub query_as_string {
    my $self = shift;
    my $sql  = shift || confess 'usage: query_as_string($sql,@values)';
    my $dbh  = $self->conn->dbh;

    foreach (@_) {
        my $x = $_;    # don't update the original!
        if ( defined($x) and $x =~ /[^[:graph:][:space:]]/ ) {
            $sql =~ s/\?/*BINARY DATA*/;
        }
        else {
            my $quote;
            if ( defined $x ) {
                if ( looks_like_number($x) ) {
                    $quote = $x;
                }
                else {
                    $x =~ s/\n.*/\.\.\./s;
                    $quote = $dbh->quote("$x");
                }
            }
            else {
                $quote = $dbh->quote(undef);
            }
            $sql =~ s/\?/$quote/;
        }
    }
    return $sql . ';';
}

1;

