package SQL::DB;
use strict;
use warnings;
use Moo;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;
use Storable qw/dclone/;
use DBI ':sql_types', 'looks_like_number';
use DBIx::Connector;
use SQL::DB::Schema;
use SQL::DB::Expr qw/:all/;
use SQL::DB::Iter;

use constant SQL_FUNCTIONS => qw/
  bv
  AND
  OR
  query
  sql_and
  sql_case
  sql_cast
  sql_coalesce
  sql_concat
  sql_count
  sql_exists
  sql_func
  sql_hex
  sql_length
  sql_lower
  sql_ltrim
  sql_max
  sql_min
  sql_or
  sql_replace
  sql_rtrim
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

our $VERSION = '0.19_12';

### CLASS FUNCTIONS ###

sub bv { _bval(@_) }

sub query {
    confess 'query is not a method' if eval { $_[0]->isa('SQL::DB') };
    return _query(@_);
}

sub sql_and { _expr_join( ' AND ', @_ ) }

sub sql_case {
    @_ || croak 'case([$expr,] when => $expr, then => $val,[else...])';

    my $e = SQL::DB::Expr->new( _txt => ["CASE\n"] );

    while ( my ( $keyword, $item ) = splice( @_, 0, 2 ) ) {
        $e .= '    ' . uc($keyword) . "\n        ";

        # Need to do this separately because
        # SQL::DB::Expr::Quote doesn't know how to '.='
        $e .= _quote($item);
        $e .= "\n";
    }
    $e .= '    END';
    return $e;
}

sub sql_coalesce { sql_func( 'COALESCE', @_ ) }

sub sql_cast {
    return _expr_join( ' ', 'CAST(', $_[0], 'AS', $_[2], ')' );
}

sub sql_concat {
    _expr_join( ' || ', map { _quote($_) } @_ );
}

sub sql_count {
    my $e = sql_func( 'COUNT', @_ );
    $e->_type('integer');
    return $e;
}

sub sql_exists { 'EXISTS(' . _query(@_) . ')' }

sub sql_func {
    my $func = shift;
    my $e = SQL::DB::Expr->new( _txt => [ $func . '(', ] );
    $e .= _expr_join( ', ', map { _quote($_) } @_ ) . ')';
    return $e;
}

sub sql_hex { sql_func( 'HEX', @_ ) }

sub sql_length { sql_func( 'LENGTH', @_ ) }

sub sql_lower { sql_func( 'LOWER', @_ ) }

sub sql_ltrim { sql_func( 'LTRIM', @_ ) }

sub sql_max { sql_func( 'MAX', @_ ) }

sub sql_min { sql_func( 'MIN', @_ ) }

sub sql_or { _expr_join( ' OR ', @_ ) }

sub sql_replace { sql_func( 'REPLACE', @_ ) }

sub sql_rtrim { sql_func( 'RTRIM', @_ ) }

sub sql_substr { sql_func( 'SUBSTR', @_ ) }

sub sql_sum { sql_func( 'SUM', @_ ) }

sub sql_table {
    my $table = shift;
    return SQL::DB::Expr->new(
        _txt => [ $table . '(' . join( ', ', @_ ) . ')' ] );
}

sub sql_upper { sql_func( 'UPPER', @_ ) }

sub sql_values { sql_func( 'VALUES', @_ ) }

### OBJECT IMPLEMENTATION ###

has 'conn' => ( is => 'ro' );

has 'dbd' => ( is => 'ro' );

has 'schema' => ( is => 'ro' );

has 'cache_sth' => (
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

    if ( $args{schema} ) {
        $args{schema} = SQL::DB::Schema->new(
            name => $args{schema} . '::' . $dbd,
            load => 1,
        );
    }
    else {

        # auto-generate the name in a semi-random way
        $args{schema} = SQL::DB::Schema->new( name => \%args . $args{dsn} );
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

    $log->debug( 'Connected to ' . $args{dsn} );
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
        $log->debug( 'Loading table schema: ' . $table );
        my $sth = $self->conn->dbh->column_info( '%', '%', $table, '%' );
        $self->schema->define( $sth->fetchall_arrayref );
        $seen{$table}++;
    }
}

sub irow {
    my $self = shift;

    if ( my @unknown = $self->schema->not_known(@_) ) {
        $self->_load_tables(@unknown);
    }

    return $self->schema->irow(@_);
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

sub _prepare {
    my $self    = shift;
    my $prepare = shift;
    my $query   = _query(@_);

    return $self->conn->run(
        sub {
            my $dbh = $_;

            my @bind_values;
            my @bind_types;

            my $ref = $query->_txt;
            foreach my $i ( 0 .. $#{$ref} ) {

                if ( ref $ref->[$i] eq 'SQL::DB::Expr::Quote' ) {
                    $ref->[$i] = $dbh->quote( $ref->[$i]->val );

              #                    if ( looks_like_number( $ref->[$i]->val ) ) {
              #                        $ref->[$i] = $ref->[$i]->val;
              #                    }
              #                    else {
              #                    }
                }
                elsif ( ref $ref->[$i] eq 'SQL::DB::Expr::BindValue' ) {
                    my $val  = $ref->[$i]->val;
                    my $type = $ref->[$i]->type;

                    # TODO: Ignore everything except binary/bytea?
                    if ( !defined $val ) {
                        $ref->[$i] = $dbh->quote(undef);
                        next;
                    }
                    elsif ( $type =~ /^bit/ ) {
                        $type = { TYPE => SQL_BIT };
                    }
                    elsif ( $type =~ /^smallint/ ) {
                        $type = { TYPE => SQL_SMALLINT };
                    }
                    elsif ( $type =~ /^numeric/ ) {
                        $type = { TYPE => SQL_NUMERIC };
                    }
                    elsif ( $type =~ /^decimal/ ) {
                        $type = { TYPE => SQL_DECIMAL };
                    }
                    elsif ( $type =~ /^int/ ) {
                        $type = { TYPE => SQL_INTEGER };
                    }
                    elsif ( $type =~ /^bigint/ ) {
                        $type = { TYPE => SQL_BIGINT };
                    }
                    elsif ( $type =~ /^float/ ) {
                        $type = { TYPE => SQL_FLOAT };
                    }
                    elsif ( $type =~ /^real/ ) {
                        $type = { TYPE => SQL_REAL };
                    }
                    elsif ( $type =~ /^double/ ) {
                        $type = { TYPE => SQL_DOUBLE };
                    }
                    elsif ( $type =~ /^char/ ) {
                        $type = { TYPE => SQL_CHAR };
                    }
                    elsif ( $type =~ /^varchar/ ) {
                        $type = { TYPE => SQL_VARCHAR };
                    }
                    elsif ( $type =~ /^datetime/ ) {
                        $type = { TYPE => SQL_DATETIME };
                    }
                    elsif ( $type =~ /^date/ ) {
                        $type = { TYPE => SQL_DATE };
                    }
                    elsif ( $type =~ /^timestamp/ ) {
                        $type = { TYPE => SQL_TIMESTAMP };
                    }
                    elsif ( $type =~ /^interval/ ) {
                        $type = { TYPE => SQL_INTERVAL };
                    }
                    elsif ( $type =~ /^bin/ ) {
                        $type = { TYPE => SQL_BINARY };
                    }
                    elsif ( $type =~ /^varbin/ ) {
                        $type = { TYPE => SQL_VARBINARY };
                    }
                    elsif ( $type =~ /^blob/ ) {
                        $type = { TYPE => SQL_BLOB };
                    }
                    elsif ( $type =~ /^clob/ ) {
                        $type = { TYPE => SQL_CLOB };
                    }
                    elsif ( $type =~ /^bytea/ ) {
                        $type = { pg_type => eval 'DBD::Pg::PG_BYTEA' };
                    }
                    else {
                        $type = { TYPE => SQL_VARCHAR };
                    }

                    push( @bind_values, $val );
                    push( @bind_types,  $type );
                    $ref->[$i] = '?';
                }
            }

            my $sth = eval { $dbh->$prepare( $query->_as_string ) };
            if ($@) {
                die 'Error: '
                  . $self->query_as_string( $query->_as_string, @bind_values )
                  . "\n$@";
            }

            $log->debugf(
                "/* $prepare with %d bind values*/\n%s",
                scalar @bind_values,
                $self->query_as_string( $query->_as_string, @bind_values )
            ) if $log->is_debug;

            my $i = 0;
            foreach my $val (@bind_values) {
                $i++;
                my $type = shift @bind_types;
                $log->debugf( 'binding param %d as %s', $i, $type );
                $sth->bind_param( $i, $val, $type );
            }

            return ( $query, $sth );
        },
    );
}

sub prepare {
    my $self = shift;
    my ( $query, $sth ) = $self->_prepare( 'prepare', @_ );
    return $sth;
}

sub prepare_cached {
    my $self = shift;
    my ( $query, $sth ) = $self->_prepare( 'prepare_cached', @_ );
    return $sth;
}

sub sth {
    my $self = shift;
    my ( $query, $sth ) =
        $self->cache_sth
      ? $self->_prepare( 'prepare_cached', @_ )
      : $self->_prepare( 'prepare',        @_ );
    my $rv = eval { $sth->execute() };
    if ($@) {
        die 'Error: ' . $query->_as_string . "\n$@";
    }
    return $sth;
}

sub do {
    my $self = shift;
    my ( $query, $sth ) =
        $self->cache_sth
      ? $self->_prepare( 'prepare_cached', @_ )
      : $self->_prepare( 'prepare',        @_ );
    my $rv = eval { $sth->execute() };
    if ($@) {
        die 'Error: ' . $query->_as_string . "\n$@";
    }
    $log->debug( "-- Result:", $rv );
    $sth->finish();
    return $rv;
}

sub iter {
    my $self = shift;
    my ( $query, $sth ) =
        $self->cache_sth
      ? $self->_prepare( 'prepare_cached', @_ )
      : $self->_prepare( 'prepare',        @_ );

    my $rv = eval { $sth->execute() };
    if ($@) {
        die 'Error: ' . $query->_as_string . "\n$@";
    }
    $log->debug( "-- Result:", $rv );
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
    return sprintf( '%04d-%02d-%02d %02d:%02d:%02dZ',
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
        if ( defined($x) and $x =~ /[\P{IsPrint}]/ ) {
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

# $db->insert_into('customers',
#     values => {cid => 1, name => 'Mark'}
# );
sub insert {
    my $self       = shift;
    my $str_into   = shift;
    my $table      = shift;
    my $str_values = shift;
    my $values     = shift;

    unless ($str_into eq 'into'
        and $str_values eq 'values'
        and ( ref $values eq 'HASH' || eval { $values->isa('HASH') } ) )
    {
        confess 'usage: insert(into => $table, values => $hashref)';
    }

    my $urow = $self->urow($table);

    my @cols = sort grep { $urow->can($_) } keys %$values;
    my @vals = map { _bval( $values->{$_}, $urow->$_->_type ) } @cols;

    @cols || confess 'insert_into requires columns/values';

    my $ret = eval {
        $self->do(
            insert_into => sql_table( $table, @cols ),
            sql_values(@vals),
        );
    };

    if ($@) {
        confess $@;
    }

    return $ret;
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

    my $urow = $self->urow($table);
    my @updates = map { $urow->$_( $set->{$_} ) }
      grep { $urow->can($_) and !exists $where->{$_} } keys %$set;

    unless (@updates) {
        $log->debug( "Nothing to update for table:", $table );
        return 0;
    }

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr =
          _expr_join( ' AND ',
            map { $urow->$_ == $where->{$_} } grep { $urow->can($_) } @keys );
    }

    return $self->do(
        update => $urow,
        set    => \@updates,
        $expr ? ( where => $expr ) : (),
    );
}

# $db->delete_from('purchases',
#    where => {cid => 1},
# );

sub delete {
    my $self = shift;
    shift;
    my $table = shift;
    shift;
    my $where = shift;

    my $urow = $self->urow($table);

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr = _expr_join( ' AND ', map { $urow->$_ == $where->{$_} } @keys );
    }

    return $self->do(
        delete_from => $urow,
        $expr ? ( where => $expr ) : (),
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

    my $srow = $self->srow($table);
    my @columns = map { $srow->$_ } @$list;

    @columns || confess 'select requires columns';

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr = _expr_join( ' AND ', map { $srow->$_ == $where->{$_} } @keys );
    }

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
