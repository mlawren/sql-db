package SQL::DB;
use Mouse;
use Return::Value;
use Try::Tiny;
use DBI;
use Carp qw/croak carp cluck/;
use Exporter qw/ import /;

use SQL::DB::Schema;
use SQL::DB::Row;
use SQL::DB::Cursor;
use SQL::DB::Sequence;
use SQL::DB::Expr;


our $VERSION = '0.18';

# AND and OR are re-exported from SQL::DB::Expr
our @EXPORT_DEFAULT = (qw/
    define_table
    row
    arow
    arows
    AND
    OR
    catch
    Schema
/);

our @EXPORT_SQLFUNCS = (qw/
        sql_coalesce
        sql_count
        sql_max
        sql_min
        sql_sum
        sql_length
        sql_cast
        sql_upper
        sql_lower
        sql_case
        sql_exists
        sql_now
        sql_nextval
        sql_currval
        sql_setval
/);

our %EXPORT_TAGS = (
    default  => \@EXPORT_DEFAULT,
    sqlfuncs => \@EXPORT_SQLFUNCS,
    all      => [ @EXPORT_DEFAULT, @EXPORT_SQLFUNCS ],
);

our @EXPORT    = @EXPORT_DEFAULT;
our @EXPORT_OK = ( qw/ :default :sqlfuncs :all /, @{ $EXPORT_TAGS{all} } );



our %schemas = (
    default => SQL::DB::Schema->new( name => 'default' ),
);


# CLASS subroutines
sub Schema {
    my $name  = shift || 'default';
    local %Carp::Internal;
    $Carp::Internal{'SQL::DB'}++;

    if ( ! exists $schemas{$name} ) {
        $schemas{$name} = SQL::DB::Schema->new( name => $name );
    }

    return $schemas{$name};
}

sub define_table {
    local %Carp::Internal;
    $Carp::Internal{'SQL::DB'}++;
    $schemas{default}->define_table( @_ );
}
    
sub arow {
    local %Carp::Internal;
    $Carp::Internal{'SQL::DB'}++;
    $schemas{default}->arow( @_ );
}
    
sub arows {
    local %Carp::Internal;
    $Carp::Internal{'SQL::DB'}++;
    $schemas{default}->arows( @_ );
}
    
sub row {
    local %Carp::Internal;
    $Carp::Internal{'SQL::DB'}++;
    $schemas{default}->row( @_ );
}
    

# Instance attributes

has 'debug' => (
    is => 'rw',
    isa => 'Bool',
    default => 0,
);

has 'sdebug' => (
    is => 'rw',
    isa => 'Bool',
    default => 0,
);

has 'dbh' => (
    is => 'rw',
    isa => 'Undef|DBI::db',
);

has 'dbd' => (
    is => 'rw',
    isa => 'Str',
    init_arg => undef,
);

has 'qcount' => (
    is => 'rw',
    isa => 'Int',
    default => 0,
    init_arg => undef,
);

has 'schema' => (
    is => 'rw',
    isa => 'SQL::DB::Schema',
    handles => [ qw/ query / ],
    default => sub { $schemas{default}->resolve_fk; $schemas{default} },
    trigger => sub { $_[1]->resolve_fk; $_[1]->dbd( $_[0]->dbd ) },
);

has '_txn' => (
    is => 'rw',
    isa => 'Int',
    default => 0,
    init_arg => undef,
);


sub connect {
    my $self = shift;
    my ( $dsn, $user, $pass, $newattrs ) = @_;
    $newattrs = {} unless ( $newattrs );

    my $attrs = {
        sqlite_unicode    => 1,
        mysql_enable_utf8 => 1,
        pg_enable_utf8    => 1,
        PrintError        => 0,
        %$newattrs,
        RaiseError        => 1,
        AutoCommit        => 1,
    };

    my $dbh = DBI->connect_cached( $dsn, $user, $pass, $attrs );

    if ( !$dbh ) {
        return failure( $DBI::errstr );
    }

    $dsn =~ /^dbi:(.*?):/;
    $self->dbd( $1 );
    $self->schema->dbd( $self->dbd );
    $self->dbh( $dbh );
    $self->qcount(0);
    $self->_txn(0);

    if ($self->dbd eq 'Pg') {
        $dbh->do('SET client_min_messages = WARNING;');
    }
    elsif ($self->dbd eq 'SQLite') {
        $dbh->do('PRAGMA foreign_key = ON;');
    }

    warn "debug: connected to $dsn" if($self->debug);

    return success;
}


sub _create_tables {
    my $self = shift;
    $self->dbh || croak 'cannot _create_tables() before connect()';

    my @tables = @_;

    # Faster to do all of this inside a BEGIN/COMMIT block on
    # things like SQLite, and better that we deploy all or nothing
    # anyway.
    return $self->txn( sub {
        my $dbh = $self->dbh;
#        $self->schema->dbd( $self->dbd );

        TABLES: foreach my $table ( @tables ) {
            warn 'debug: create table '. $table->name if ( $self->debug );
            my $sth = $dbh->table_info(undef, undef, $table->name, 'TABLE');
            if (!$sth) {
                die $DBI::errstr;
            }

            while ( my $x = $sth->fetch ) {
                if ( $x->[2] eq $table->name ) {
                    croak 'Table '. $table->name
                         .' already exists - not creating';
                    next TABLES;
                }
            }

            foreach my $action ( $table->as_sql ) {
                try {
                    warn "debug: $action" if ( $self->sqldebug );
                    $dbh->do( $action );

                } catch {
                    die $_;# . '. Query: '. $action;
                };
            }

            $self->create_sequence( name => $table->name );
            if ( $self->dbd ne 'Pg' ) {
                map { $self->_create_sequence( $_ ) } $table->sequences;
            }
        }
        return 1;
    }, catch {
        croak $_;
    });
}


sub create_table {
    my $self = shift;
    my $name = shift;
    $self->dbh || croak 'cannot create_table() before connect()';

    return $self->_create_tables( $self->schema->table( $name ) );
}


sub _drop_tables {
    my $self = shift;
    $self->dbh || croak 'cannot _drop_tables() before connect()';

    my @tables = @_;

    my $dbh = $self->dbh;
    $self->schema->dbd( $self->dbd );

    foreach my $table ( @tables ) {
        warn 'debug: drop table '. $table->name if ( $self->debug );
        my $sth = $dbh->table_info(undef, undef, $table->name, 'TABLE');
        if (!$sth) {
            die $DBI::errstr;
        }

        my $x = $sth->fetch;
        if ( $x and $x->[2] eq $table->name ) {
            my $action = 'DROP TABLE IF EXISTS '.$table->name.
                ($self->dbd eq 'Pg' ? ' CASCADE' : '');

            $self->txn( sub {
                map { $self->_drop_sequence( $_ ) } $table->sequences;
                unless ( $table->name eq $self->schema->seqtable ) {
                    $self->drop_sequence( $table->name )
                }

                warn "debug: $action" if( $self->sqldebug );
                $dbh->do( $action );
            }, catch {
                die $_;
            });
        }
        else {
            warn 'debug: table doesnt exist: '. $table->name if ( $self->debug );
        }
    }
}


sub drop_table {
    my $self = shift;
    my $name = shift;

    return $self->_drop_tables( $self->schema->table( $name ) );
}


sub deploy {
    my $self = shift;
    $self->dbh || croak 'cannot deploy() before connect()';
    carp 'debug: deploy' if ( $self->debug );

    local %Carp::Internal;
    $Carp::Internal{'Try::Tiny'}++;

    my @tables = $self->schema->deploy_order;
    return $self->_create_tables( @tables );
}


sub _undeploy {
    my $self = shift;
    $self->dbh || croak 'cannot _undeploy() before connect()';
    carp 'debug: _undeploy' if ( $self->debug );

    local %Carp::Internal;
    $Carp::Internal{'Try::Tiny'}++;

    my @tables = reverse $self->schema->deploy_order;
    return $self->_drop_tables( @tables );
}


sub query_as_string {
    my $self = shift;
    my $sql  = shift || confess 'query_as_string requires an argument';
    my $dbh  = $self->dbh;
    
    foreach (@_) {
        if (defined($_) and $_ =~ /[^[:graph:][:space:]]/) {
            $sql =~ s/\?/*BINARY DATA*/;
        }
        else {
            my $quote = $dbh->quote(
                defined $_ ? "$_" : undef # make sure it is a string
            );
            $sql =~ s/\?/$quote/;
        }
    }
    return $sql;
}


sub _do {
    my $self    = shift;
    my $prepare = shift || croak '_do($prepare)';
    $self->dbh || confess 'cannot do before connect()';
    my $query   = eval { $self->query( @_ ) };

    if ( ! defined $query ) {
        confess "Bad Query: $@";
    }

    local $Carp::Internal{'Try::Tiny'};
    $Carp::Internal{'Try::Tiny'}++;

    return try {
        my $sth   = $self->dbh->$prepare("$query");
        my $i = 1;
        foreach my $type ($query->bind_types) {
            if ($type) {
                $sth->bind_param($i, undef, $type);
                carp 'debug: binding param '.$i.' with '.$type
                    if( $self->debug && $self->debug > 1 );
            }
            $i++;
        }
        my $rv = $sth->execute($query->bind_values);
        $sth->finish();
        $self->qcount( $self->qcount + 1 );
        carp 'debug: '. $self->query_as_string("$query", $query->bind_values)
            ." /* Result: $rv */" if($self->sqldebug);
        return $rv;
    } catch {
        croak "$_: Query was:\n"
            . $self->query_as_string( "$query", $query->bind_values );
    };
}


sub do {
    my $self = shift;
    return $self->_do('prepare_cached', @_);
}


sub do_nopc {
    my $self = shift;
    return $self->_do('prepare', @_);
}


sub _fetch {
    my $self    = shift;
    my $prepare = shift || croak '_fetch($prepare)';
    $self->dbh || confess 'cannot fetch before connect()';
    my $query   = $self->query( @_ );

    my $wantarray = wantarray;
    local $Carp::Internal{'Try::Tiny'};
    $Carp::Internal{'Try::Tiny'}++;

    try {
        my $class   = SQL::DB::Row->make_class_from( $query->acolumns );
        my $rv;

        my $sth = $self->dbh->$prepare("$query");
        my $i = 1;
        foreach my $type ( $query->bind_types ) {
            $sth->bind_param( $i, undef, $type ) if ( defined $type);
            $i++;
        }
        $rv = $sth->execute($query->bind_values);
        $self->qcount( $self->qcount + 1 );

        if ( $wantarray ) {
            my $arrayref = $sth->fetchall_arrayref();
            carp 'debug: (Rows: '. scalar @$arrayref .') '.
                $self->query_as_string("$query", $query->bind_values)
                if($self->sqldebug);

            return map { $class->new_from_arrayref( $_ )->_inflate }
                @{$arrayref};
        }

        carp 'debug: (Cursor call) '.
            $self->query_as_string( "$query", $query->bind_values )
            if ( $self->sqldebug );

        return SQL::DB::Cursor->new( $sth, $class );

    } catch {
        croak "$_: Query was:\n"
            . $self->query_as_string( "$query", $query->bind_values );
    };
}


sub fetch {
    my $self = shift;
    return $self->_fetch( 'prepare_cached', @_ );
}


sub fetch_nopc {
    my $self = shift;
    return $self->_fetch( 'prepare', @_ );
}


sub fetch1 {
    my $self    = shift;
    my @results = $self->_fetch( 'prepare_cached', @_ );
    return $results[0];
}


sub fetch1_nopc {
    my $self   = shift;
    my @results = $self->_fetch( 'prepare', @_ );
    return $results[0];
}


sub txn {
    my $self = shift;
    my $subref = shift;
    (ref($subref) && ref($subref) eq 'CODE') || croak 'usage txn($subref)';
    my $catch = shift;

    local %Carp::Internal;
    $Carp::Internal{'Try::Tiny'}++;

    my $dbh = $self->dbh;
    my $wantarray = wantarray;
    my @result;

    $self->_txn( $self->_txn + 1 );
    carp 'debug: TXN ('. $self->_txn .')' if( $self->sqldebug );

    try {
        if ( $self->_txn == 1 ) {
            $dbh->begin_work || die $dbh->errstr;
        }
        carp 'debug: BEGIN (txn '. $self->_txn .')'
            if( $self->sqldebug );

        { local $SIG{__DIE__}; @result = &$subref };

        if ( $self->_txn == 1 ) {
            $dbh->commit || die $dbh->errstr;
        }
        carp 'debug: COMMIT (txn '. $self->_txn .')'
            if( $self->sqldebug );

    } catch {
        my $err = $_;
        carp 'debug: FAIL Work (txn '. $self->_txn .'): ' . $err
            if ( $self->sqldebug );

        if ( $self->_txn == 1 ) { # top-most txn
            try {
                $dbh->rollback;
                carp 'debug: ROLLBACK (txn 1)' if ( $self->sqldebug );
            } catch {
                $self->_txn( $self->_txn - 1 );
                croak $err.'. ROLLBACK (txn 1) FAILED: ' .$_;
            };
            if ( $catch ) {
                @result = ${$catch}->( $err ); # Actually a Try::Tiny::Catch
            }
        }
        else { # nested txn - die so the outer txn fails
            $self->_txn( $self->_txn - 1 );
            croak $err;
        }
    };

    $self->_txn( $self->_txn - 1 );

    if ( $wantarray ) {
        return @result;
    }
    return $result[0];
}


sub create_sequence {
    my $self = shift;
    ( @_ >= 2 ) || confess 'usage: create_sequence( name => $name )';
    return $self->_create_sequence( SQL::DB::Sequence->new( @_ ) );
}


sub _create_sequence {
    my $self = shift;
    my $seq  = shift;

#    $self->schema->dbd( $self->dbd );
# FIXME handle real Pg sequences.

    warn 'debug: create sequence '. $seq->name if ( $self->debug );
    my $sequences = $self->schema->arow( $self->schema->seqtable );
    my $existing = $self->fetch1(
        select => $sequences->name,
        from   => $sequences,
        where  => $sequences->name == $seq->name,
    );
    
    if ( $existing ) {
        confess "Sequence ".$seq->name." already exists";
    }

    return $self->insert(
        $self->schema->row( $self->schema->seqtable,
            name => $seq->name,
            inc  => $seq->inc,
            val  => $seq->start - $seq->inc,
        )
    );
}


sub seq {nextval(@_)};

=for comment
--
-- create the table holding all sequences
--
CREATE TABLE sequence (
  name CHAR(10) PRIMARY KEY,
  last_val INT UNSIGNED NOT NULL
);
--
-- initialize (create) a sequence, starting with 42
--
INSERT INTO sequence (name, last_val) VALUES ('foo', 41);
--
-- get a new number from sequence 'foo'
--
UPDATE sequence SET last_val=@val:=last_val+1 WHERE name='foo';
--
-- use the value
--
SELECT @val;
INSERT INTO bar (id, stuff) VALUES (@val, 'baz');
=cut


sub nextval {
    my $self  = shift;
    my $name  = shift || croak 'usage: nextval( $name, $count )';
    my $count = shift || 1;

    if ($count> 1 and !wantarray) {
        croak 'you should want the full array of sequences';
    }

    my $sequences = $self->schema->arow( $self->schema->seqtable );
    my $seq;
    my $no_updates;

    eval {
        # Aparent MySQL bug - no locking with FOR UPDATE
        if ($self->dbd eq 'mysql') {
            $self->dbh->do('LOCK TABLES '. $self->schema->seqtable
                . ' WRITE, '. $self->schema->seqtable . ' AS '.
                                    $sequences->_alias .' WRITE'
            );
        }

        $seq = $self->fetch1(
            select     => [
                $sequences->val,
                $sequences->inc,
            ],
            from       => $sequences,
            where      => $sequences->name == $name,
            for_update => ($self->dbd ne 'SQLite'),
        );

        croak "Can't find sequence '$name'" unless($seq);

        $no_updates = $self->do(
            update  => [
                $sequences->set_val($seq->val + ( $count * $seq->inc ) )
            ],
            where   => $sequences->name == $name,
        );

        if ($self->dbd eq 'mysql') {
            $self->dbh->do('UNLOCK TABLES');
        }

    };

    if ($@ or !$no_updates) {
        my $tmp = $@;

        if ($self->dbd eq 'mysql') {
            $self->dbh->do('UNLOCK TABLES');
        }

        croak "seq: $tmp";
    }


    if (wantarray) {
        my $start = $seq->val + $seq->inc;
        my $stop  = $start + ( $count * $seq->inc ) - $seq->inc;
        return ($start..$stop);
    }
    return $seq->val + ( $count * $seq->inc );
}


sub setval {
    my $self  = shift;
    my $name  = shift || croak 'usage: setval( $name, $val )';
    my $val   = shift || croak 'usage: setval( $name, $val )';

    my $sequences = $self->schema->arow( $self->schema->seqtable );
    return $self->do(
        update => [
            $sequences->set_val( $val ),
        ],
        where => $sequences->name == $name,
    );
}


sub drop_sequence {
    my $self = shift;
    my $name = shift || croak 'usage: drop_sequence( $name )';

# FIXME handle real Pg sequences.

    warn 'debug: drop sequence '. $name if ( $self->debug );
    my $sequences = $self->schema->arow( $self->schema->seqtable );

    return try {
        $self->do(
            delete_from => $sequences,
            where       => $sequences->name == $name,
        );
    } catch {
        my $err = $_;
        my $sth = $self->dbh->table_info(undef, undef,
            $self->schema->seqtable, 'TABLE');

        if (!$sth) {
            die $err ."\n". $DBI::errstr;
        }

        if ( ! $sth->fetch ) { # sequences table doesn't exist so ignore
            return 1;
        }
        die $err;
    }
}


sub _drop_sequence {
    my $self = shift;
    my $seq  = shift;
    return $self->drop_sequence( $seq->name );
}



sub insert {
    my $self = shift;
    my $obj  = shift || croak 'insert($obj)';

    unless(ref($obj) and $obj->can('q_insert')) {
        croak "Not an insert object: $obj";
    }

    my ($arows, @insert) = $obj->q_insert; # reference hand-holding
    if (!@insert) {
        croak "No insert for object. Missing PRIMARY KEY?";
    }
    return $self->do(@insert);
}


sub update {
    my $self = shift;
    my $obj  = shift || croak 'update($obj)';

    unless(ref($obj) and $obj->can('q_update')) {
        croak "Not an updatable object: $obj";
    }

    my ($arows, @update) = $obj->q_update; # reference hand-holding
    if (!@update) {
        croak "No update for object. Missing PRIMARY KEY?";
    }
    return $self->do(@update);
}


sub delete {
    my $self = shift;
    my $obj  = shift || croak 'delete($obj)';

    unless(ref($obj) and $obj->can('q_delete')) {
        croak "Not an delete object: $obj";
    }

    my ($arows, @delete) = $obj->q_delete; # reference hand-holding
    if (!@delete) {
        croak "No delete for object. Missing PRIMARY KEY?";
    }
    return $self->do(@delete);
}


sub quickrows {
    my $self = shift;
    return unless(@_);

    my @keys = $_[0]->_column_names;
    my $c = join(' ', map {'%-'.(length($_)+ 2).'.'
                           .(length($_)+ 2).'s'} @keys) . "\n";

    my $str = sprintf($c, @keys);

    foreach my $row (@_) {
        my @values = map {$row->$_} @keys;
        my @print = map {
            !defined($_) ?
            'NULL' :
            ($_ =~ m/[^[:graph:][:print:]]/ ? '*BINARY*' : $_)
        } @values;

        $str .= sprintf($c, @print);
    }
    return $str;
}
    

sub disconnect {
    my $self = shift;
    if ( $self->dbh ) {
        warn 'debug: Disconnecting from DBI' if ( $self->debug );
        $self->dbh->disconnect;
        $self->dbh( undef );
        $self->dbd( '' );
    }
    return;
}


#
# Functions
#

sub do_function {
    my $name = uc( shift );

    my @vals;
    my @bind;

    foreach (@_) {
        if (UNIVERSAL::isa($_, 'SQL::DB::Expr')) {
            push(@vals, $_);
            push(@bind, $_->bind_values);
        }
        else {
            push(@vals, $_);
        }
    }
    return SQL::DB::Expr->new(
        val => $name .'('. join(', ',@vals) .')',
        bind_values => \@bind,
    );

}


# FIXME set a flag somewhere so that SQL::DB::Row doesn't create a
# modifier method
sub coalesce {
    scalar @_ >= 2 || croak 'coalesce() requires at least two argument';

    my $new;
    if (UNIVERSAL::isa($_[0], 'SQL::DB::Expr')) {
        $new = $_[0]->_clone();
    }
    else {
        $new = SQL::DB::Expr->new;
    }
    $new->set_val('COALESCE('. join(', ', @_) .')');
    return $new;
}


sub sql_count {
    return do_function('COUNT', @_);
}


sub sql_min {
    return do_function('MIN', @_);
}


sub sql_max {
    return do_function('MAX', @_);
}


sub sql_sum {
    return do_function('SUM', @_);
}


sub sql_length {
    return do_function('LENGTH', @_);
}


sub sql_cast {
    return do_function('CAST', @_);
}


sub sql_upper {
    return do_function('UPPER', @_);
}


sub sql_lower {
    return do_function('LOWER', @_);
}


sub sql_exists {
    return do_function('EXISTS', @_);
}


sub sql_case {
    @_ || croak 'case([$expr,] when => $expr, then => $val,[else...])';

    my @bind;

    my $str = 'CASE';
    if ($_[0] !~ /^when$/i) {
        # FIXME more cleaning? What can be injected here?
        my $expr = shift;
        $expr =~ s/\sEND\W.*//gi;
        $str .= ' '.$expr;
    }

    UNIVERSAL::isa($_, 'SQL::DB::Expr') && push(@bind, $_->bind_values);

    my @vals;

    while (my ($p,$v) = splice(@_,0,2)) {
        ($p =~ m/(^when$)|(^then$)|(^else$)/)
            || croak 'case($expr, when => $cond, then => $val, [else...])';

        if (UNIVERSAL::isa($v, 'SQL::DB::Expr')) {
            $str .= ' '.uc($p).' '.$v;
            push(@bind, $v->bind_values);
        }
        else {
            $str .= ' '.uc($p).' ?';
            push(@bind, $v);
        }
    }

    @_ && croak 'case($expr, when => $cond, then => $val,...)';

    return SQL::DB::Expr->new(
        val => $str. ' END',
        bind_values => \@bind
    );
}


sub sql_now {
    return do_function('NOW');
}


sub do_function_quoted {
    my $name = shift;

    my @vals;
    my @bind;

    foreach (@_) {
        if (UNIVERSAL::isa($_, 'SQL::DB::Expr')) {
            push(@vals, "'$_'");
            push(@bind, $_->bind_values);
        }
        else {
            push(@vals, "'$_'");
        }
    }
    return SQL::DB::Expr->new(
        val => $name .'('. join(', ',@vals) .')',
        bind_values => \@bind
    );

}


sub sql_nextval {
    return do_function_quoted('nextval', @_);
}


sub sql_currval {
    return do_function_quoted('currval', @_);
}


sub sql_setval {
    my $expr = SQL::DB::Expr->new;
    if (@_ == 2) {
        $expr->set_val('setval(\''. $_[0] .'\', '.  $_[1] .')');
    }
    elsif (@_ == 3) {
        $expr->set_val('setval(\''. $_[0] .'\', '.  $_[1] .', '.
                           ($_[2] ? 'true' : 'false') .')');
    }
    else {
        confess 'setval() takes 2 or 3 arguments';
    }

    return $expr;
}


# Backwards Compatability Methods
sub set_debug    { sdebug(@_) };
sub sqldebug     { sdebug(@_) };
sub set_sqldebug { sdebug(@_) };

no Mouse;

1;
__END__

=head1 NAME

SQL::DB - Perl interface to SQL Databases

=head1 VERSION

0.18. Development release.

=head1 SYNOPSIS

  use SQL::DB qw/sql_count/;

  define_table(
      name  => 'authors',
      columns => [
        { name => 'id',   type => 'int', primary => 1 },
        { name => 'name', type => 'varchar(255)' },
        { name => 'age',  type => 'int', null => 1 },
      ],
      indexes => [
        { columns => 'name' },
      ],
  );

  define_table(
      name  => 'books',
      columns => [
        { name => 'id',     type => 'int', primary => 1 },
        { name => 'title',  type => 'varchar(255)' },
        { name => 'author', type => 'int', references => 'authors(id)' },
      ],
      unique => [
        { columns => 'title,author' },
      ],
  );

  my $db = SQL::DB->new();

  $db->connect( "dbi:SQLite:sqldbtest$$.db", 'user', 'pass' );
  $db->deploy;

  my $author1 = row( 'authors',
      name => 'Leo Tolstoy',
      age  => 75,
      id   => $db->nextval( 'authors' ),
  );

  my $book1 = row( 'books',
      title  => "A Tramp Abroard",
      author => $author1->id,
      id     => $db->nextval( 'books' ),
  );

  $db->txn( sub{                  # Commit all or nothing please
      $db->insert( $author1 );
      $db->insert( $book1 );
  }, catch {
      die "Error : $_";
  });

  # Woops - we got the author wrong, fix it using SQL
  my ( $authors, $books ) = arows( qw/ authors books / );
  $db->do(
      update => [
          $authors->set_name( 'Mark Twain' ),
      ],
      where  => $authors->id == $author1->id,
  );

  # Now give me some data
  my $cursor = $db->fetch(
      select    => [
          $authors->name,
          sql_count( $books->id )->as( 'bookcount' ),
      ],
      from      => $authors,
      left_join => $books,
      on        => $books->author == $authors->id,
      where     => $authors->age->is_not_null .AND.
                    $books->title->like( '%Tramp%' ),
      order_by  => $authors->age->desc,
      limit     => 10,
  );

  while ( my $row = $cursor->next ) {
      printf( "%s %d\n",  $row->name, $row->bookcount );
      # Mark Twain 1
  }

=head1 DESCRIPTION

B<SQL::DB> is a Perl interface to SQL databases. It provides
functionality similar to Object Relational Mappers (ORM) such as
L<DBIx::Class>, but comes with a little more power and flexibility due
to the use of a slightly a lower-level API. It aims to be a tool for
programmers who want databases to work harder than Perl scripts.

B<SQL::DB> is capable of generating just about any kind of query,
including aggregate expressions, joins, nested selects, unions,
database-side operator invocations, and transactions. It also has some
cross database support, keeping the amount of database-specific code
you have to write to a minimum.

The typical application flow is as follows:

=over 4

=item * A database schema is defined.

=item * An B<SQL::DB> object is constructed, connect()s to the
database and optionally deploy()s the tables.

=item * Using "abstract rows" obtained via the Schema arow() method you
can do() queries and fetch() data.

=item * Using objects obtained via the Schema row() or database fetch()
methods you can insert(), update() or delete() data.

=item * Finally, you can disconnect() from the database.

=back

The effort needed to use B<SQL::DB> is primarily related to learning
the schema definitions and the query syntax.

=head2 Schema Definition

An B<SQL::DB> schema is an object (L<SQL::DB::Schema>) describing the
database tables, columns, indexes and other pieces of information that
determine how application data is stored. The schema is the basis for
the row and abstract row objects used to make queries, and is also used
to deploy the structure to a database. The schema is accessed via the
Schema() class function.

Tables are defined in a schema using define(). The following
(over-complete) example describes the complete set of possible keywords
and data structures accepted. Note that some fields can be
'multivariate'. Ie they can have different values depending on the
L<DBI> database driver (DBD). This is useful if you want a single
codebase to run against different databases types.

    Schema->define_table(
        table => 'mytable',
        columns => [
            {
                name => 'id',
                type => 'integer',            # same for all DBDs
                primary => 1,                 # can be here or below
                references => 'foreign(col)', # can be here or below
                auto_increment => 1,          # invis. to DBD != 'mysql'
            },
            {
                name => 'ablob',
                type => {                    # multivariate
                    Pg    => 'bytea',
                    mysql => 'blob' 
                },
                unique  => 1,                # can be here or below
                null    => 1,
                default => 'defaultval',     # can be \&coderef
            },
        ],
        primary => 'id,ablob',               # here or above
        unique => [
            { columns => 'id' },             # here or above
            { columns => 'id,ablob' },       # note multiple columns
        ],
        foreign => [                         # here or above
            {                                # note multiple columns
                key        => 'id,ablob',
                references => 'foreign(col),foreign(ablob)',
            },
        ],
        indexes => [
            {
                columns => 'id,ablob',
                using   => {                 # multivariate
                    Pg => 'btree',
                },
            },
        ],
        tablespace => {                      # invis. to DBD != 'Pg'
            Pg => 'mytablespace',
        }
        type => {                            # invis. to DBD != 'mysql'
            mysql => 'InnoDB' 
        }
        engine => {                          # invis. to DBD != 'mysql'
            mysql => 'MyISAM',
        }
        indexes => [
            {
                columns => 'id,ablob',
                using   => {                 # multivariate
                    Pg => 'btree',
                },
            },
        ],
        sequences => [
            { name => 'name' },
            { name => 'name2', inc => 2 },
        ],
    );

An B<SQL::DB> application can define multiple database schemas. The
Schema() class method takes an optional name argument, to automatically
create (if needed) and return a different schema. This is useful if you
need to move data from one schema to another, or perhaps wish to
support two versions of a schema simultaneously.

    Schema( 'iso3166' )->define_table(...);
    SQL::DB->new( schema => Schema( 'iso3166' ) );

=cut

 =head2 Schema Autoloading

If you have an existing database, or you do not intend to ever deploy
to your database from Perl then the B<SQL::DB> load_schema() method can
be called after connecting to a database, and the schema will be
automatically populated. Note that this has a startup cost due to the
need for multiple database queries, and may not be supported well (or
accurately) by all drivers. The speed issue can be mitigated by
inserting the output of the Schema()->as_perl() method into your code.

=head2 Row Objects

Row objects can be instantiated using the Schema->row() method. They
have set_col() setters and col() getters, and can be inserted using the
insert() method:

    my $row = Schema->row( 'mytable',
        id       => 1,
        othercol => 'value',
    );
    $row->set_id(2);

    $db->insert( $row );

Row objects are also returned by a successful fetch() or fetch1()
query. However the objects returned from these methods are not limited
to the columns of a single table like row(), but instead have columns
based on whatever tables and or expressions were specified in the
query.

For example, if SQL generated for the query was:

    SELECT
        table1.col1,
        table2.col2,
        1 AS 'col4'
    FROM
        table1,
        table2

then the retrieved objects can do the following:

    $row->col1;
    $row->col2;
    $row->col4;

If the query contained enough primary key information then those same
row objects can be modified and fed back to other database functions:

    $db->delete( $row1 );

    $row2->set_col( $newval );
    $db->update( $row2 );

=head2 Abstract Rows and Expressions

B<SQL::DB> queries use abstract representations of table rows - objects
that can be thought of as matching I<any> row in a table.  Abstract
rows are obtained using the Schema->arow() method.  The objects
returned have methods that match the columns of a table, plus some
extra SQL syntax/sugar methods.

    my ( $cds, $tracks ) = $db->arow(qw/ cds tracks /);

The real power of B<SQL::DB> lies in the way that SQL expressions can
be constructed using these abstract columns. The abstract column
methods do not really act on the abstract row, but intead return an
expression object (L<SQL::DB::Expr>). Using Perl's overload feature
these objects can be combined and nested in Perl in a way that maps
very closely to they way the SQL would be written by hand.

    Perl Expression                     SQL Result
    ----------------------------------  ---------------------------
    $cds                                cds

    $cds->title                         cds.title

    $tracks->cd                         tracks.cd

    $tracks->id->func('count')          count(tracks.cd)

    $tracks->set_length(                SET tracks.length =
        $tracks->length + 10 )              tracks.length + 10

    ( $cds->id == 1 ) .OR.              cds.id = 1 OR
      $cds->title->like( '%Magic%' ) )      cds.title LIKE '%Magic%'

    ( $cds->id->is_not_null .AND.       cds.id IS NOT NULL AND
      ! $tracks->id->in( 1, 2, 5 ) )        NOT tracks.id IN (1,2,5)

Here is a summary of the default available expression operators and
methods. New expression subroutines can be generated in your own code -
see L<SQL::DB::Expr> for details.

    Perl            SQL             Applies to
    ---------       -------         ------------
    .AND.           AND             All Expressions
    .OR.            OR              All Expressions
    !               NOT             All Expressions
    .CONCAT.        || or CONCAT    All Expressions
    ==              =               All Expressions
    !=              !=              All Expressions

    like            LIKE            Column only
    in              IN              Column only
    not_in          NOT IN          Column only
    is_null         IS NULL         Column only
    is_not_null     IS NOT NULL     Column only
    asc             ASC             Column only
    desc            DESC            Column only
    count           COUNT           Column only
    min             MIN             Column only
    max             MAX             Column only
    func('x',...)   X(col,...)      Column only

=head2 Query Syntax

Here is a better example with multiple functions and multiple tables.
For each CD, show me the number of tracks, the length of the longest
track, and the total length of the CD in one query:

    track = $db->arow('tracks');
    @objs = $db->fetch(
        select   => [
            $track->cd->title,
            $track->id->func('count'),
            $track->length->func('max'),
            $track->length->func('sum')
        ],
        group_by  => [
            $track->cd->title,
        ],
    );

    foreach my $obj (@objs) {
        print 'Title: '            . $obj->title      ."\n";
        print 'Number of Tracks: ' . $obj->count_id   ."\n";
        print 'Longest Track: '    . $obj->max_length ."\n";
        print 'CD Length: '        . $obj->sum_length ."\n\n";
    }


=head2 Transactions & Savepoints

=over 4

=back

To be documented.

=head2 Sequences

Emulated on systems that don't support native sequences. Based roughly
on the PostgreSQL api.

To be documented.

=over 4

=back


=head1 CLASS METHODS

=over 4

=item Schema($name) -> SQL::DB::Schema

If the optional $name is given a new schema will be created (if
necessary) and returned. Otherwise the default schema is returned.

=back

=head1 CONSTRUCTOR

The new() constructor accepts the optional 'debug', 'sdebug' and
'schema' arguments.

=head1 ATTRIBUTES

=over 4

=item debug <-> Bool

General debugging state (true/false). Debug messages are 'warn'ed.

=item sdebug <-> Bool

SQL statement debugging (true/false). SQL statements are 'warn'ed.

=item dbh -> DBI::db

The L<DBI> handle connecting the database.

=item qcount -> Int

The number of successful queries made against the current connection.

=item schema <-> SQL::DB::Schema

The schema associated with this instance.

=item dbd -> Str

The L<DBD> driver name ('SQLite', 'mysql', 'Pg' etc) for the type of
database we are connected to. Is 'undef' when not connected. This
attribute is actually provided by L<SQL::DBD::Schema>.

=back

=head1 METHODS

=over 4

=item connect($dsn, $user, $pass, $attrs)

Connect to a database. The parameters are passed directly to
L<DBI>->connect. This method also informs the internal table/column
representations what type of database we are connected to, so they can
set their database-specific features accordingly. Returns the dbh.

=item create_table($name)

Creates the table $name and associated indexes and sequences in the
database.  Will warn and skip on any attempts to create tables that
already exist.

=item deploy

Calls create_table() for all tables in the current schema, inside a
transaction.

=item drop_table($name)

Drops $name and associated indexes and sequences in the database.

=item do(@query)

Constructs a L<SQL::DB::Query> object as defined by @query and runs
that query against the connected database.  Croaks if an error occurs.
This is the method to use for any statement that doesn't retrieve
values (eg INSERT, UPDATE and DELETE). Returns whatever value the
underlying L<DBI>->do call returns.  This method uses "prepare_cached"
to prepare the call to the database.

=item do_nopc(@query)

Same as for do() but uses "prepare" instead of "prepare_cached" to
prepare the call to the database. This is really only necessary if you
tend to be making recursive queries that are exactly the same.
See L<DBI> for details.

=item fetch(@query) -> SQL::DB::Cursor | @SQL::DB::Row

Constructs an L<SQL::DB::Query> object as defined by @query and runs
that query against the connected database.  Croaks if an error occurs.
This method should be used for SELECT-type statements that retrieve
rows. This method uses "prepare_cached" to prepare the call to the database.

When called in array context returns a list of L<SQL::DB::Row> based
objects. The objects have accessors for each column in the query. Be
aware that this can consume large amounts of memory if there are lots
of rows retrieved.

When called in scalar context returns a query cursor (L<SQL::DB::Cursor>)
(with "next", "all" and "reset" methods) to retrieve dynamically
constructed objects one at a time.

=item fetch_nopc(@query) -> SQL::DB::Cursor | @SQL::DB::Row

Same as for fetch() but uses "prepare" instead of "prepare_cached" to
prepare the call to the database. This is really only necessary if you
tend to be making recursive queries that are exactly the same.
See L<DBI> for details.

=item fetch1(@query) -> SQL::DB::Row

Similar to fetch() but always returns only the first object from
the result set. All other rows (if any) can not be retrieved.
You should only use this method if you know/expect one result.
This method uses "prepare_cached" to prepare the call to the database.

=item fetch1_nopc(@query) -> SQL::DB::Row

Same as for fetch1() but uses "prepare" instead of "prepare_cached" to
prepare the call to the database. This is really only necessary if you
tend to be making recursive queries that are exactly the same.
See L<DBI> for details.

=item query(@query)

Return an L<SQL::DB::Query> object as defined by @query. This method
is useful when creating nested SELECTs, UNIONs, or you can print the
returned object if you just want to see what the SQL looks like.

=item query_as_string($sql, @bind_values)

An internal function for pretty printing SQL queries by inserting the
bind values into the SQL itself. Returns a string.

=item insert($row)

Insert $row into the database, where $row is an L<SQL::DB::Row> based
object returned from row() or fetch() methods.

=item update($row)

Update $row in the database, where $row is an L<SQL::DB::Row> based
object returned from row() or fetch() methods.

=item delete($row)

Delete $row from the database, where $row is an L<SQL::DB::Row> based
object returned from row() or fetch() methods.

=item txn(&coderef)

Runs the code in &coderef as an SQL transaction. If &coderef does not
raise any exceptions then the transaction is commited, otherwise it is
rolled back.

Returns true/false on success/failure. The returned value can also be
printed in the event of failure. See L<Return::Value> for details.

This method can be called recursively, but any sub-transaction failure
will always result in the outer-most transaction also being rolled back.

=item quickrows(@objs)

Returns a string containing the column values of @objs in a tabular
format. Useful for having a quick look at what the database has returned:

    my @objs = $db->fetch(....);
    warn $db->quickrows(@objs);

=item create_sequence( @sequence )

Creates a sequence in the database. Takes the same arguments as
L<SQL::DB::Sequence>. Sequences are emulated on systems that don't
support them natively.

    $db->create_sequence( name => 'myseq' );

=item nextval( $name, $count )

Advance the sequence to its next value and return that value. If $count
is specified then a array of $count values are returned and the
sequence incremented appropriately.

=item setval( $name, $value )

Reset the sequence counter value.

=item drop_sequence( $name )

Drops sequence $name from the database.

=item disconnect

Disconnect from the database. Effectively DBI->disconnect.

=back

=head1 COMPATABILITY

Version 0.13 changed the return type of the txn() method. Instead of a
2 value list indicating success/failure and error message, a single
L<Return::Value> object is returned intead.

=head1 SEE ALSO

L<SQL::Abstract>, L<DBIx::Class>, L<Class::DBI>, L<Tangram>

=head1 SUPPORT

This distribution is still under development. Feedback, testing, bug
reports and patches are all welcome.

=over

=item Bug Reporting

    https://rt.cpan.org/Public/Bug/Report.html?Queue=SQL-DB

=item Source Code

    git clone git://github.com/mlawren/sql-db.git

=back

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2009 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
