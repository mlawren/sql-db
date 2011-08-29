package SQL::DB;
use strict;
use warnings;
use Moo;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;
use Storable qw/dclone/;
use DBI ':sql_types';
use DBIx::Connector;
use SQL::DB::Schema qw/get_schema/;
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
  sql_upper
  sql_values
  /;

use Sub::Exporter -setup => {
    exports => [SQL_FUNCTIONS],
    groups  => {
        all     => [SQL_FUNCTIONS],
        default => [SQL_FUNCTIONS],
    },
};

our $VERSION = '0.97_2';

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
        ( ref $_ eq '' && $_ =~ m/^((when)|(then)|(else))$/i )
          ? uc($_)
          : _bexpr($_)
    } @_;

    return _expr_join( ' ', 'CASE', @items, 'END' );
}

sub sql_coalesce { sql_func( 'COALESCE', @_ ) }

sub sql_cast { sql_func( 'CAST', @_ ) }

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

sub sql_max { sql_func( 'MAX', @_ ) }

sub sql_min { sql_func( 'MIN', @_ ) }

sub sql_substr { sql_func( 'SUBSTR', @_ ) }

sub sql_sum { sql_func( 'SUM', @_ ) }

sub sql_or { _bexpr_join( ' OR ', @_ ) }

sub sql_upper { sql_func( 'UPPER', @_ ) }

sub sql_values { sql_func( 'VALUES', @_ ) }

# SQL::DB Object implementation

has 'debug' => ( is => 'rw', default => sub { 0 } );

has 'dsn' => (
    is       => 'rw',
    required => 1,
    isa      => sub {
        confess "dsn must be 'dbi:...'"
          unless ( defined $_[0] && $_[0] =~ /^dbi:/ );
    },
    trigger => sub {
        my $self = shift;
        ( my $dsn = shift ) =~ /^dbi:(.*?):/;
        my $dbd = $1 || die "Invalid DSN: " . $dsn;
        $self->dbd($dbd);
    },
);

has 'dbd' => ( is => 'rw', init_arg => undef );

has 'dbuser' => ( is => 'rw' );

has 'dbpass' => ( is => 'rw' );

has 'dbattrs' => ( is => 'rw', default => sub { {} } );

has 'conn' => ( is => 'rw', init_arg => undef );

has 'schema' => (
    is      => 'rw',
    trigger => sub {
        my $self = shift;
        $self->_schema( get_schema( $self->schema )
              || SQL::DB::Schema->new( name => $self->schema ) );
    },
);

has '_schema' => (
    is       => 'rw',
    init_arg => undef,
);

has 'prepare_mode' => (
    is  => 'rw',
    isa => sub {
        confess "prepare_mode must be 'prepare|prepare_cached'"
          unless $_[0] =~ m/^(prepare)|(prepare_cached)$/;
    },
    default => sub { 'prepare_cached' },
);

has '_current_timestamp' => ( is => 'rw', init_arg => undef );

sub BUILD {
    my $self = shift;
    $self->dsn( $self->dsn );    # to make the trigger fire
    my $dbd = $self->dbd;

    # Trigger schema creation
    $self->schema( $self->schema || $self->dsn );

    $self->dbattrs(
        {
            PrintError => 0,
            ChopBlanks => 1,
            $dbd eq 'Pg'     ? ( pg_enable_utf8    => 1 ) : (),
            $dbd eq 'SQLite' ? ( sqlite_unicode    => 1 ) : (),
            $dbd eq 'mysql'  ? ( mysql_enable_utf8 => 1 ) : (),
            %{ $self->dbattrs },
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
        }
    );

    $self->conn(
        DBIx::Connector->new(
            $self->dsn, $self->dbuser, $self->dbpass, $self->dbattrs
        )
    );

    $self->conn->mode('fixup');
    return $self;
}

sub connect {
    my $class   = shift;
    my $dsn     = shift;
    my $dbuser  = shift;
    my $dbpass  = shift;
    my $dbattrs = shift || {};
    return $class->new(
        dsn     => $dsn,
        dbuser  => $dbuser,
        dbpass  => $dbpass,
        dbattrs => $dbattrs,
    );
}

sub _load_tables {
    my $self = shift;

    foreach my $table (@_) {
        my $sth = $self->conn->dbh->column_info( '%', '%', $table, '%' );
        $self->_schema->define( $sth->fetchall_arrayref );
    }
}

sub urow {
    my $self = shift;

    if ( my @unknown = $self->_schema->not_known(@_) ) {
        $self->_load_tables(@unknown);
    }

    return $self->_schema->urow(@_);
}

sub srow {
    my $self = shift;

    if ( my @unknown = $self->_schema->not_known(@_) ) {
        $self->_load_tables(@unknown);
    }

    return $self->_schema->srow(@_);
}

sub sth {
    my $self    = shift;
    my $prepare = $self->prepare_mode;
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
                die $self->query_as_string( $query->_as_string,
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

                my $rv = $sth->execute;
                $log->debug( "-- Result:", $rv );
                return $wantarray ? ( $sth, $rv ) : $sth;
            }
        },
    );
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
                $x =~ s/\n.*/\.\.\./s;
                $quote = $dbh->quote("$x");
            }
            else {
                $quote = $dbh->quote(undef);
            }
            $sql =~ s/\?/$quote/;
        }
    }
    return $sql . ';';
}

# $db->insert_into('customers',
#     values => {cid => 1, name => 'Mark'}
# );
sub insert_into {
    my $self  = shift;
    my $table = shift;
    shift;
    my $values = shift;

    my $urow = $self->urow($table);

    my @cols = sort grep { $urow->can($_) } keys %$values;
    my @vals = map       { $values->{$_} } @cols;

    @cols || croak 'insert_into requires columns/values';

    return $self->do(
        insert_into => SQL::DB::Expr->new(
            _txt => $table . '(' . join( ',', @cols ) . ')',
        ),
        sql_values(@vals)
    );
}

# $db->update('purchases',
#     set   => {pid => 2},
#     where => {cid => 1},
# );
sub update {
    my $self  = shift;
    my $table = shift;
    shift;
    my $set = shift;
    shift;
    my $where = shift;

    if ( $self->debug ) {
        require Data::Dumper;
        local $Data::Dumper::Indent   = 1;
        local $Data::Dumper::Maxdepth = 2;

        $log->debug(
            Data::Dumper::Dumper(
                {
                    table => $table,
                    set   => $set,
                    where => $where
                }
            )
        );
    }

    my $urow = $self->urow($table);
    my @updates = map { $urow->$_( $set->{$_} ) }
      grep { $urow->can($_) and !exists $where->{$_} } keys %$set;

    unless (@updates) {
        $log->debug( "Nothing to update for table:", $table );
        return;
    }

    my $expr = _expr_join(
        ' AND ',
        map    { $urow->$_ == $where->{$_} }
          grep { $urow->can($_) } keys %$where
    );

    $expr || croak 'update requires a valid where clause';
    return $self->do(
        update => $urow,
        set    => \@updates,
        where  => $expr,
    );
}

# $db->delete_from('purchases',
#    where => {cid => 1},
# );

sub delete_from {
    my $self  = shift;
    my $table = shift;
    shift;
    my $where = shift;

    my $urow = $self->urow($table);
    my $expr =
      _expr_join( ' AND ', map { $urow->$_ == $where->{$_} } keys %$where );

    $expr || croak 'delete_from requires a where clause';
    return $self->do(
        delete_from => $urow,
        where       => $expr,
    );
}

# my @objs = $db->select( ['pid','label],
#     from => 'customers',
#     where => {cid => 1},
# );
sub select {
    my $self = shift;
    my $list = shift;
    shift;
    my $table = shift;
    shift;
    my $where = shift;

    my $srow = $self->_schema->srow($table);
    my @columns = map { $srow->$_ } @$list;

    @columns || croak 'select requires columns';

    my $expr =
      _expr_join( ' AND ', map { $srow->$_ == $where->{$_} } keys %$where );

    return $self->fetch(
        select => \@columns,
        from   => $srow,
        $expr ? ( where => $expr ) : (),
    ) if wantarray;

    return $self->fetch1(
        select => \@columns,
        from   => $srow,
        $expr ? ( where => $expr ) : (),
    );
}

1;

