package SQL::DB;
use strict;
use warnings;
use Moo;
use DBIx::Connector;
use Carp qw/croak carp cluck confess/;
use SQL::DB::Cursor;
use SQL::DB::Expr qw/:all/;
use Sub::Exporter -setup => {
    exports => [
        qw/
          query
          /,
    ],
    groups => {
        default => [
            qw/
              /
        ],
    },
};

our $VERSION = '0.19_3';

# Instance attributes

has 'verbose' => (
    is      => 'rw',
    default => 0,
);

has 'debug' => (
    is      => 'rw',
    default => 0,
);

has 'dsn' => ( is => 'rw', );

has 'dbuser' => ( is => 'rw', );

has 'dbpass' => ( is => 'rw', );

has 'dbattrs' => (
    is      => 'rw',
    default => sub { {} },
);

has 'conn' => ( is => 'rw', );

has 'dbd' => (
    is       => 'rw',
    init_arg => undef,
);

has 'prepare_mode' => (
    is  => 'rw',
    isa => sub {
        die "prepare_mode must be 'prepare|prepare_cached'"
          unless $_[0] =~ m/^(prepare)|(prepare_cached)$/;
    },
    default => sub { 'prepare_cached' },
);

has 'dry_run' => ( is => 'rw' );

has '_current_timestamp' => ( is => 'rw' );

sub BUILD {
    my $self = shift;

    ( my $dsn = $self->dsn ) =~ /^dbi:(.*?):/;
    my $dbd = $1 || die "Invalid DSN: " . $self->dsn;
    $self->dbd($dbd);

    my $attrs = {
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

                    #                    $h->do("SET TIMEZONE TO 'UTC';");
                }
                elsif ( $dbd eq 'SQLite' ) {
                    $h->do('PRAGMA foreign_key = ON;');
                }
                return;
            },
        }
    };

    $self->conn(
        DBIx::Connector->new( $dsn, $self->dbuser, $self->dbpass, $attrs ) );

    $self->conn->mode('fixup');
    return $self;
}

sub create_sequence {
    my $self = shift;
    my $arg  = shift;

    if ( $self->dbd eq 'SQLite' ) {
        my $dsn = $self->dsn . '.seq';
        my $dbh = DBI->connect($dsn);
        $dbh->do( 'CREATE TABLE sequence_' 
              . $arg . ' ('
              . 'seq INTEGER PRIMARY KEY, mtime TIMESTAMP )' );
    }
    else {
        die "Sequence support not implemented for " . $self->dbd;
    }
}

sub nextval {
    my $self = shift;
    my $arg  = shift;

    if ( $self->dbd eq 'SQLite' ) {
        my $dsn = $self->dsn . '.seq';
        my $dbh = DBI->connect($dsn);
        $dbh->do( 'INSERT INTO sequence_' 
              . $arg
              . '(mtime) VALUES(CURRENT_TIMESTAMP)' );
        return $dbh->sqlite_last_insert_rowid();
    }
    else {
        die "Sequence support not implemented for " . $self->dbd;
    }
}

sub query {
    return $_[0] if ( @_ == 1 and ref $_[0] eq 'SQL::DB::Expr' );
    my @statements;
    foreach my $item (@_) {
        if ( ref $item eq '' ) {
            ( my $tmp = $item ) =~ s/_/ /g;
            push( @statements, uc $tmp . "\n" );
        }
        elsif ( ref $item eq 'ARRAY' ) {
            push( @statements, '    ', _bexpr_join( ",\n    ", @$item ), "\n" );

         #FIXME
         #            push(@statements, '    ', query(",\n    ", @$item), "\n");
        }
        elsif ( $item->isa('SQL::DB::Expr') ) {
            push( @statements, '    ', $item, "\n" );
        }
    }

    my $e = _expr_join( '', @statements );
    $e->{_txt} .= "\n" unless ( $e->{_txt} =~ /\n$/ );
    return $e;
}

sub query_as_string {
    my $self = shift;
    my $sql  = shift || confess 'usage: query_as_string($sql,@values)';
    my $dbh  = $self->conn->dbh;

    foreach (@_) {
        if ( defined($_) and $_ =~ /[^[:graph:][:space:]]/ ) {
            $sql =~ s/\?/*BINARY DATA*/;
        }
        else {
            my $quote = $dbh->quote(
                defined $_ ? "$_" : undef    # make sure it is a string
            );
            $sql =~ s/\?/$quote/;
        }
    }
    return $sql;
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
        $self->_current_timestamp( $self->current_timestamp );
    }

    my @ret = $self->conn->txn(@_);

    if ($set_timestamp) {
        $self->_current_timestamp(undef);
    }

    return $wantarray ? @ret : $ret[0];
}

sub do {
    my $self    = shift;
    my $prepare = $self->prepare_mode;
    my $query   = eval { query(@_) };

    if ( !defined $query ) {
        confess "Bad Query: $@";
    }

    print $self->query_as_string( "$query", @{ $query->_bvalues } ) . "\n"
      if $self->verbose;

    return if $self->dry_run;

    return $self->conn->run(
        no_ping => sub {
            my $dbh = $_;
            my $sth = $dbh->$prepare("$query");

            my $i = 0;
            foreach my $ref ( @{ $query->_btypes } ) {
                $i++;
                next unless $ref;
                my $type = $ref->{ $self->dbd } || $ref->{default};
                $sth->bind_param( $i, undef, eval "$type" );
                carp 'binding param ' . $i . ' with ' . $type
                  if ( $self->debug && $self->debug > 1 );
            }
            my $rv = $sth->execute( @{ $query->_bvalues } );
            $sth->finish();
            carp ''
              . $self->query_as_string( "$query", @{ $query->_bvalues } )
              . " /* Result: $rv */"
              if ( $self->debug );
            return $rv;
        },
        sub {
            die $_ if ( $self->verbose );
            die $self->query_as_string( "$query", @{ $query->_bvalues } )
              . "\n$_";
        }
    );
}

sub sth {
    my $self    = shift;
    my $prepare = $self->prepare_mode;
    my $query   = eval { query(@_) };

    if ( !defined $query ) {
        confess "Bad Query: $@";
    }

    print $self->query_as_string( "$query", @{ $query->_bvalues } ) . "\n"
      if $self->verbose;

    return if $self->dry_run;

    my $wantarray = wantarray;

    return $self->conn->run(
        no_ping => sub {
            my $dbh = $_;
            my $sth = $dbh->$prepare("$query");

            my $i = 0;
            foreach my $ref ( @{ $query->_btypes } ) {
                $i++;
                next unless $ref;
                my $type = $ref->{ $self->dbd } || $ref->{default};
                $sth->bind_param( $i, undef, eval "$type" );
                carp 'binding param ' . $i . ' with ' . $type
                  if ( $self->debug && $self->debug > 1 );
            }
            my $rv = $sth->execute( @{ $query->_bvalues } );
            carp ''
              . $self->query_as_string( "$query", @{ $query->_bvalues } )
              . " /* Result: $rv */"
              if ( $self->debug );
            return $wantarray ? ( $sth, $rv ) : $sth;
        },
        sub {
            die $_ if ( $self->verbose );
            die $self->query_as_string( "$query", @{ $query->_bvalues } )
              . "\n$_";
        }
    );
}

sub cursor {
    my $self  = shift;
    my $query = query(@_);
    my $sth   = $self->sth($query);

    return SQL::DB::Cursor->new(
        db    => $self,
        query => $query,
        sth   => $sth,
    );
}

sub fetch {
    my $self = shift;
    return $self->cursor(@_)->all;
}

sub fetch1 {
    my $self   = shift;
    my $cursor = $self->cursor(@_);
    my $first  = $cursor->next;

    $cursor->finish;
    return $first;
}

sub insert {
    my $self = shift;
    my $obj = shift || croak 'insert($obj)';

    unless ( ref($obj) and $obj->can('q_insert') ) {
        croak "Not an insert object: $obj";
    }

    my ( $arows, @insert ) = $obj->q_insert;    # reference hand-holding
    if ( !@insert ) {
        croak "No insert for object. Missing PRIMARY KEY?";
    }
    return $self->do(@insert);
}

sub update {
    my $self = shift;
    my $obj = shift || croak 'update($obj)';

    unless ( ref($obj) and $obj->can('q_update') ) {
        croak "Not an updatable object: $obj";
    }

    my ( $arows, @update ) = $obj->q_update;    # reference hand-holding
    if ( !@update ) {
        croak "No update for object. Missing PRIMARY KEY?";
    }
    return $self->do(@update);
}

sub delete {
    my $self = shift;
    my $obj = shift || croak 'delete($obj)';

    unless ( ref($obj) and $obj->can('q_delete') ) {
        croak "Not an delete object: $obj";
    }

    my ( $arows, @delete ) = $obj->q_delete;    # reference hand-holding
    if ( !@delete ) {
        croak "No delete for object. Missing PRIMARY KEY?";
    }
    return $self->do(@delete);
}

1;
__END__

=head1 NAME

SQL::DB - Perl interface to SQL Databases

=head1 VERSION

0.19. Development release.

=head1 SYNOPSIS

    use SQL::DB;
    use SQL::DB::Schema;

    table 'authors' => (
        { col => 'id' },
        { col => 'name' },
        { col => 'birth' },
        { col => 'death' },
        { col => 'photo',
            btype => 'SQL_BLOB',
            btype_Pg => '{ pg_type => DBD::Pg::PG_BYTEA }' },
    );

    end_schema();

    my $db = SQL::DB->new(
        dsn => "dbi:SQLite:sqldbtest$$.db",
        dbuser => 'user',
        dbpass => 'pass' 
    );

    $db->conn->dbh->do("
        CREATE TABLE authors (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            birth INTEGER NOT NULL,
            death INTEGER NOT NULL,
            photo BYTEA
        );
    ");

    $db->txn( sub{            # Commit all or nothing please
        $db->do(
            insert_into => authors(qw/ id birth death name /),
            sql_values(1, 1828, 1910, 'Leo Tolstoy'),
        );


  #    $db->insert( 'books',
  #        title  => "A Tramp Abroard",
  #        author => $author1->id,
  #        id     => $db->nextval( 'books' ),
  #    );


  #    $db->do(
  #        insert_into => authors('col1', 'col2'),
  #        values(1, 3, sql_func('CURRENT_TIMESTAMP')),
  #    );
  #
  #    $db->do(
  #        insert_into => $authors('col1', 'col2'),
  #        select      => [ $t->col1, $t->col2 ],
  #        from        => $t,
  #        where       => ...
  #    );
  ##

  #    $db->do(
  #        delete_from => $t,
  #        where       => $t->blue->is_null .AND. $t->green->between(3,5)
  #    );
  #
  #    $db->do(
  #        update  => $t,
  #        set     => $t->green(1), $t->blue(2),
  #        where   => $t->green == 2;
  #    );
  #
  #    $db->fetch(
  #        select      => [ $t->green, $t->blue ],
  #        from        => $t,
  #        left_join   => $j,
  #        on          => $t->id == $j->parent,
  #        where       => $t->id > 32,
  #    );

  #    $db->insert( 'authors', %values );

    }, sub {
        die "Error : $_";
    });


    my ($this_year) = (localtime)[5]+1900;
    warn $this_year;

    my $authors = srow(qw/authors/);
    my $cursor = $db->cursor(
        select    => [
            $authors->id,
            $authors->name,
            ($authors->death - $authors->birth)->as('age'),
  #            sql_count( $books->id )->as( 'bookcount' ),
  #           case => $authors->death,
  #           when => undef, then => $authors->death - $authors->birth,
  #           when => undef, then =>
  #           else => -1,
  #           'end', as => 'age',

        ],
        from      => $authors,
  #        left_join => $books,
  #        on        => $books->author == $authors->id,
  #        where     => $authors->year->is_not_null .AND.
  #                      $books->title->like( '%Tramp%' ),
        order_by  => $authors->birth->asc,
          limit     => 10,
    );

    my $row = $db->fetch1(
        select    => [
            $authors->id,
            $authors->name,
            ($authors->death - $authors->birth)->as('age'),
        ],
        from      => $authors,
        where     => $authors->id == 1,
    );
    printf( "%s lived %d years\n", $row->name, $row->age );

    foreach my $row  ( $cursor->all ) {
  #        printf( "%s %d\n",  $row->name, $row->bookcount );
        printf( "%s lived %d years\n",  $row->name, $row->age );
        # Mark Twain 1
    }
    while ( my $row = $cursor->next ) {
  #        printf( "%s %d\n",  $row->name, $row->bookcount );
        printf( "%s lived %d years\n",  $row->name, $row->age );
        # Mark Twain 1
    }


  # Woops - we got the author wrong, fix it using SQL
  #  my ( $authors, $books ) = arows( qw/ authors books / );
  #  $db->do(
  #      update => $authors->set_name( 'Mark Twain' ),
  #      where  => $authors->id == $author1->id,
  #  );

  # Now give me some data
  #  my $cursor = $db->fetch(
  #      select    => [
  #          $authors->name,
  #          sql_count( $books->id )->as( 'bookcount' ),
  #      ],
  #      from      => $authors,
  #      left_join => $books,
  #      on        => $books->author == $authors->id,
  #      where     => $authors->age->is_not_null .AND.
  #                    $books->title->like( '%Tramp%' ),
  #      order_by  => $authors->age->desc,
  #      limit     => 10,
  #  );

  #  while ( my $row = $cursor->next ) {
  #      printf( "%s %d\n",  $row->name, $row->bookcount );
  #      # Mark Twain 1
  #  }

  #    my $c = $db->cursor(
  #        select => [
  #            $t1->col,
  #        ],
  #        from => $t1,
  #        where => $t1->id != 4,
  #    );

  #    $db->insert(
  #        into => 'authors',
  #        values =>
  ##        row( 'authors',
  #            title => 'this title',
  #            id => 'lksjdf dsj',
  ##        ),
  #    );

  #    update authors set col=1,col2=2 where blah.

  #    $db->update( $t1,
  #    $db->do(
  #        update => $t1,
  #        set => {
  #            title => 'this title',
  #            id => 'lksjdf dsj',
  #        },
  #        where => $t1->lksdfl sdflkj dslfkj lskdjf dl
  #    );

  #    $db->delete(
  #        from => $table,
  #        where => $table->column == $value,
  #    );

  #    $db->do(
  #        delete_from => $t1,
  #        where => $t1->junk == lksjdlksjd,
  #    )

  #    $db->update( $t1, {
  #            title => 'this title',
  #            id => 'lksjdf dsj',
  #        }, where => $t1->lksdfl sdflkj dslfkj lskdjf dl
  #    );

  #    $db->do(
  #        update => [
  #            $t1->set_title(lksjdf),
  ##            $t1->set_id(39),
  #        ],
  #        where => $t1->lksdfl sdflkj dslfkj lskdjf dl
  #    );

  #    $db->do(
  #        insert_into => $t1,
  #        values => $t1lksjdlksjd
  #    )


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

=item dbd -> Str

The L<DBD> driver name ('SQLite', 'mysql', 'Pg' etc) for the type of
database we are connected to. Is 'undef' when not connected. This
attribute is actually provided by L<SQL::DBD::Schema>.

=back

=head1 METHODS

=over 4

=item BUILD

Documented here for completeness. This is used by the Moo object system
at instantiation time.

=item connect($dsn, $user, $pass, $attrs)

Connect to a database. The parameters are passed directly to
L<DBI>->connect. This method also informs the internal table/column
representations what type of database we are connected to, so they can
set their database-specific features accordingly. Returns the dbh.

=item create_table($name)

Creates the table $name and associated indexes and sequences in the
database.  Will warn and skip on any attempts to create tables that
already exist.

=item do(@query)

Constructs a L<SQL::DB::Query> object as defined by @query and runs
that query against the connected database.  Croaks if an error occurs.
This is the method to use for any statement that doesn't retrieve
values (eg INSERT, UPDATE and DELETE). Returns whatever value the
underlying L<DBI>->do call returns.  This method uses "prepare_cached"
to prepare the call to the database.

=item fetch(@query) -> SQL::DB::Cursor | @SQL::DB::Row

Constructs an L<SQL::DB::Query> object as defined by @query and runs
that query against the connected database.  Croaks if an error occurs.
This method should be used for SELECT-type statements that retrieve
rows. This method uses "prepare_cached" to prepare the call to the
database.

When called in array context returns a list of L<SQL::DB::Row> based
objects. The objects have accessors for each column in the query. Be
aware that this can consume large amounts of memory if there are lots
of rows retrieved.

When called in scalar context returns a query cursor
(L<SQL::DB::Cursor>) (with "next", "all" and "reset" methods) to
retrieve dynamically constructed objects one at a time.

=item fetch1(@query) -> SQL::DB::Row

Similar to fetch() but always returns only the first object from the
result set. All other rows (if any) can not be retrieved. You should
only use this method if you know/expect one result. This method uses
"prepare_cached" to prepare the call to the database.

=item query(@query)

Return an L<SQL::DB::Query> object as defined by @query. This method is
useful when creating nested SELECTs, UNIONs, or you can print the
returned object if you just want to see what the SQL looks like.

=item query_as_string($sql, @bind_values)

An internal function for pretty printing SQL queries by inserting the
bind values into the SQL itself. Returns a string.

=item current_timestamp

The current date and time (as a string) that remains fixed within a
transaction.

=item cursor( @query )

Runs a query and returns a L<SQL::DB::Cursor> object. You can call
next() and all() methods on this object to obtain data.

=item sth( @query )

Runs a query and returns a L<DBI::st> statement handle. You can call
fetchrow_array() and other L<DBI> method on this handle.

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

This method can be called recursively, but any sub-transaction failure
will always result in the outer-most transaction also being rolled
back.

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

=back

=head1 COMPATABILITY

All SQL::DB releases have so far been DEVELOPMENT!

Version 0.19 was a complete rewrite based on Moo. Lots of things were
simplified, modules deleted, dependencies removed, etc. The API has
changed completely.

Version 0.13 changed the return type of the txn() method. Instead of a
2 value list indicating success/failure and error message, a single
L<Return::Value> object is returned intead.

=head1 SEE ALSO

L<DBIx::Connector>, L<SQL::DB::Expr>, L<SQL::DB::Cursor>,
L<SQL::DB::Schema>

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

Copyright (C) 2007-2011 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

=cut

=head1 CLASS FUNCTIONS

All of the following functions are automatically exported.

=over 4

=item expr( @statements )

Create a new expression based on @statements. This is a very dumb
function. All plain string statements are uppercased with all
occurences of '_' converted to spaces.

=back

=head1 BUGS

Using B<SQL::DB::Expr> objects with the Perl "join" command does not
work as expected, apparently because join does not trigger either the
'.' or '.=' overload methods. The work around is to use the _expr_join
subroutine.

# vim: set tabstop=4 expandtab:



