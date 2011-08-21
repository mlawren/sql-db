package SQL::DB::Schema;
use strict;
use warnings;
use Carp qw/croak carp confess/;
use SQL::DB::Expr ':all';
use Sub::Install qw/install_sub/;

use constant SQL_FUNCTIONS => qw/
  AND
  OR
  sql_coalesce
  sql_length
  sql_cast
  sql_upper
  sql_lower
  sql_case
  sql_concat
  sql_exists
  sql_sum
  sql_min
  sql_max
  sql_count
  sql_values
  sql_func
  /;

#    sql_now
#    sql_nextval
#    sql_currval
#    sql_setval

use Sub::Exporter -setup => {
    exports => [
        qw/
          table
          end_schema
          /,
        SQL_FUNCTIONS
    ],
    groups => {
        default => [
            qw/
              table
              end_schema
              /,

            #            @sql_functions,
            SQL_FUNCTIONS
        ],
    },
};

our $VERSION = '0.19_3';

sub _getglob { no strict 'refs'; \*{ $_[0] } }

my @tables;

sub table {
    my ( $caller, $file, $line ) = caller;
    my $table = shift;

    # now SELECT and UPDATE/DELETE types

    my $srow = $caller . '::Srow::' . $table;
    my $urow = $caller . '::Urow::' . $table;

    @{ *{ _getglob( $srow . '::ISA' ) }{ARRAY} } = ('SQL::DB::Expr');
    @{ *{ _getglob( $urow . '::ISA' ) }{ARRAY} } = ('SQL::DB::Expr');

    my @columns;

    foreach my $def (@_) {
        my $col = $def;
        my $btype;

        if ( ref $def ) {
            $col = delete $def->{col};
            while ( my ( $key, $val ) = each %$def ) {
                if ( $key eq 'btype' ) {
                    $btype->{default} = $val;
                }
                elsif ( $key =~ s/^btype_(.*)/$1/ ) {
                    $btype->{$key} = $val;
                }
                else {
                    die "Unknown column parameter: $key";
                }
            }
        }

        install_sub(
            {
                code => sub {
                    my $table_expr = shift;
                    SQL::DB::Expr->new(
                        _txt   => $table_expr->_alias . '.' . $col,
                        _btype => $btype,
                    );
                },
                into => $srow,
                as   => $col,
            }
        );

        install_sub(
            {
                code => sub {
                    my $table_expr = shift;

                    if (@_) {
                        my $val = shift;
                        return SQL::DB::Expr->new(
                            _txt     => $col . ' = ?',
                            _btype   => $btype,
                            _bvalues => [$val],
                        );
                    }

                    return SQL::DB::Expr->new(
                        _txt   => $col,
                        _btype => $btype,
                    );
                },
                into => $urow,
                as   => $col,
            }
        );

        push( @columns, $col );
    }

    install_sub(
        {
            code => sub {
                my $table_expr = shift;
                return map { $table_expr->$_ } @columns;
            },
            into => $urow,
            as   => '_columns',
        }
    );
    install_sub(
        {
            code => sub {
                my $table_expr = shift;
                return map { $table_expr->$_ } @columns;
            },
            into => $srow,
            as   => '_columns',
        }
    );

    push( @tables, $table );

}

sub end_schema {
    my ( $caller, $file, $line ) = caller;

    #    use Data::Dumper; $Data::Dumper::Indent = 1;
    #    $Data::Dumper::Maxdepth=2;

    foreach my $table (@tables) {
        install_sub(
            {
                code => _build_insert( undef, $table ),
                into => $caller,
                as   => $table,
            }
        );
    }

    install_sub(
        {
            code => _build_srow( $caller, 'srow' ),
            into => $caller,
            as   => 'srow',
        }
    );
    install_sub(
        {
            code => _build_urow( $caller, 'urow' ),
            into => $caller,
            as   => 'urow',
        }
    );

    # FIXME Make this export dependent upon an argument to end_schema;
    # end_schema( export => 1 );
    # Or else on import as in 'use SQL::DB::Schema qw/-reexport/'
    Sub::Exporter::setup_exporter(
        {
            into    => $caller,
            exports => [

               # FIXME do these need to be built?? Or do the subs above suffice?
                srow => \&_build_srow,
                urow => \&_build_urow,
                ( map { $_ => \&_build_insert } @tables ),
                SQL_FUNCTIONS,
            ],
            groups => {
                default => [qw/ srow urow /],
                tables  => \@tables,
                sql     => [SQL_FUNCTIONS],
            },
        }
    );
}

sub _build_srow {
    my ( $class, $name, $arg ) = @_;

    return sub {
        my $srow = $class . '::Srow::';
        foreach my $try (@_) {
            die "Table '$try' not defined" unless grep( /^$try$/, @tables );
        }

        my @ret;
        foreach my $table (@_) {
            my $x = bless(
                SQL::DB::Expr->new(
                    _txt   => 'junk',
                    _alias => $table,
                ),
                $srow . $table
            );
            $x->_set_txt( $table . ' AS ' . $x->_alias );
            return $x unless (wantarray);
            push( @ret, $x );
        }
        return @ret;
      }
}

sub _build_urow {
    my ( $class, $name, $arg ) = @_;

    return sub {
        my $urow = $class . '::Urow::';
        foreach my $try (@_) {
            die "Table '$try' not defined" unless grep( /^$try$/, @tables );
        }

        if (wantarray) {
            return
              map { bless( SQL::DB::Expr->new( _txt => $_, ), $urow . $_ ); }
              @_;
        }
        return bless( SQL::DB::Expr->new( _txt => $_[0], ), $urow . $_[0] );
      }
}

sub _build_insert {
    my ( $class, $name, $arg ) = @_;

    return sub {
        return SQL::DB::Expr->new( _txt => $name . '(' . join( ',', @_ ) . ') ',
        );
      }
}

sub sql_concat { _expr_binary( '||', $_[0], $_[1] ) }
sub sql_exists { 'EXISTS (' . expr(@_) . ')' }
sub sql_sum    { 'SUM(' . _bexpr(shift) . ')' }
sub sql_min    { 'MIN(' . _bexpr(shift) . ')' }
sub sql_max    { 'MAX(' . _bexpr(shift) . ')' }
sub sql_count  { 'COUNT(' . _bexpr(shift) . ')' }

sub sql_values {
    ( VALUES => '(' . _expr_join( ', ', map { _bexpr($_) } @_ ) . ')' );
}

sub sql_func {
    my $func = shift;

    my $last = pop @_;
    my $args = SQL::DB::Expr->new( _txt => $func . '(' );
    map { $args .= _bexpr($_) . ', ' } @_;
    $args .= $last . ')';
    return $args;
}

# FIXME replace all calls to this to  sql_func above
sub _do_function {
    my $name = uc(shift);

    my @vals;
    my @bind;

    foreach (@_) {
        if ( UNIVERSAL::isa( $_, 'SQL::DB::Expr' ) ) {
            push( @vals, $_ );
            push( @bind, $_->bind_values );
        }
        else {
            push( @vals, $_ );
        }
    }
    return SQL::DB::Expr->new(
        val         => $name . '(' . join( ', ', @vals ) . ')',
        bind_values => \@bind,
    );

}

# FIXME set a flag somewhere so that SQL::DB::Row doesn't create a
# modifier method
sub sql_coalesce {
    scalar @_ >= 2 || croak 'sql_coalesce() requires at least two argument';

    return 'COALESCE(' . _expr_join( ', ', map { _bexpr($_) } @_ ) . ')';

    return 'COALESCE(' . _expr_join(
        ',',
        map {
            if ( eval { $_->isa('SQL::DB::Expr') } )
            {
                $_;
            }
            else {
                "'$_'";
            }
          } @_
    ) . ')';

    #    SQL::DB::Expr->new(
    #        _txt
    my $new;
    if ( UNIVERSAL::isa( $_[0], 'SQL::DB::Expr' ) ) {
        $new = $_[0]->_clone();
    }
    else {
        $new = SQL::DB::Expr->new;
    }
    $new->set_val( 'COALESCE(' . join( ', ', @_ ) . ')' );
    return $new;
}

sub sql_length {
    return _do_function( 'LENGTH', @_ );
}

sub sql_cast {
    return _do_function( 'CAST', @_ );
}

sub sql_upper {
    return _do_function( 'UPPER', @_ );
}

sub sql_lower {
    return _do_function( 'LOWER', @_ );
}

sub sql_case {
    @_ || croak 'case([$expr,] when => $expr, then => $val,[else...])';

    my @bind;

    my $str = 'CASE';
    if ( $_[0] !~ /^when$/i ) {

        # FIXME more cleaning? What can be injected here?
        my $expr = shift;
        $expr =~ s/\sEND\W.*//gi;
        $str .= ' ' . $expr;
    }

    UNIVERSAL::isa( $_, 'SQL::DB::Expr' ) && push( @bind, $_->bind_values );

    my @vals;

    while ( my ( $p, $v ) = splice( @_, 0, 2 ) ) {
        ( $p =~ m/(^when$)|(^then$)|(^else$)/ )
          || croak 'case($expr, when => $cond, then => $val, [else...])';

        if ( UNIVERSAL::isa( $v, 'SQL::DB::Expr' ) ) {
            $str .= ' ' . uc($p) . ' ' . $v;
            push( @bind, $v->bind_values );
        }
        else {
            $str .= ' ' . uc($p) . ' ?';
            push( @bind, $v );
        }
    }

    @_ && croak 'case($expr, when => $cond, then => $val,...)';

    return SQL::DB::Expr->new(
        val         => $str . ' END',
        bind_values => \@bind
    );
}

1;

__END__

=head1 NAME

SQL::DB::Schema - Model tables and rows for SQL::DB queries

=head1 VERSION

0.18. Development release.

=head1 SYNOPSIS

  package My::Schema;
  use SQL::DB::Schema;

  table 'cars' => qw/ id make model /;

  table 'people' => qw/ id name carmake carmodel /,
      { col      => 'photo',
        btype    => 'SQL_BLOB',
        btype_Pg => '{ pg_type => DBD::Pg::BYTEA }',
      },
  );

  end_schema();

  # in your application somewhere
  use SQL::DB;
  use My::Schema; # imports cars() and people()

  my $db = SQL::DB->new(...);

  my $cars = srow('cars');
  $db->do(
    insert_into => people(qw/id name carmake carmodel/),
    select  => [ 1, 'Harry', $cars->make, $cars->model ],
    from    => $cars,
    where   => $cars->make == 'Ford',
  );

  my $people = urow('people');
  $db->do(
    update => $people,
    set    => [ $people->name('Linda') ],
    where  => $people->id == 1,
  );

=head1 DESCRIPTION

L<SQL::DB> provides a low-level interface to SQL databases, using Perl
objects and logic operators. B<SQL::DB::Schema> is used to specify
database tables in a way that can be used in L<SQL::DB> queries.

B<NOTE:> You probably don't want to be writing your schema classes
manually.  Build them using L<sqldb-builder>(1) instead.

See L<SQL::DB> for how to use your schema classes.

=head1 FUNCTIONS

=over 4

=item table( $table, @columns )

Define a table in the schema.

=item row( $name, @args ) -> SQL::DB::Row::$name

Returns a new object representing a single row of the table $name, with
values as specified by @args.  Such an object can be inserted (or
potentially updated or deleted) by SQL::DB.

=item arow( $name ) -> SQL::DB::ARow::$name

Returns an object representing I<any> row of table $name. This
abstraction object is used with the SQL::DB 'fetch' and 'do' methods.

=item arows( @names ) -> @{ SQL::DB::ARow::$name1, ... }

Returns objects representing any row of a table. These abstraction
objects are used with the SQL::DB 'fetch' and 'do' methods.

=item end_schema

Called when you have finished defining your schema. This imports a
bunch of functions necessary to use the schema.

=back

=over 4

=item expr( @statements )

Create a new expression based on @statements. This is a very dumb
function. All plain string statements are uppercased with all
occurences of '_' converted to spaces.

=item _expr_join( $separator, @expressions )

Does the same as Perl's 'join' built-in but for SQL::DB::Expr objects.
See BUGS below for why this is needed.

=item _bexpr( $item )

Return $item if it is already an expression, or a new SQL::DB::Expr
object otherwise.

=item _expr_binary( $op, $e1, $e2, $swap )

An internal method for building binary operator expressions.

=item AND, OR

These subroutines let you write SQL logical expressions in Perl using
string concatenation:

    ( $e1 .AND. $e2 ) .OR. ( $e3 .AND. $e4 )

Note that due to operator precedence, expressions either side of .AND.
or .OR. should be bracketed if they are not already single expression
objects.

Things are implemented this way due to Perl not allowing the
overloading of the 'and' and 'or' built-ins.

=item sql_concat( $a, $b )

Maps to "$a || $b".

=item sql_exists( stuff )

Maps to "EXISTS( stuff )".

=item sql_sum( stuff )

Maps to "SUM( stuff )".

=item sql_min( stuff )

Maps to "MIN( stuff )".

=item sql_max( stuff )

Maps to "MAX( stuff )".

=item sql_count( stuff )

Maps to "COUNT( stuff )".

=item sql_case( stuff )
=item sql_cast( stuff )
=item sql_coalesce( stuff )
=item sql_length( stuff )
=item sql_lower( stuff )
=item sql_upper( stuff )
=item sql_values( stuff )

=item sql_func( $name, @args )

This is a generic way to write an SQL function that isn't already
handled by B<SQL::DB::Expr>. For example:

    sub my_weird_func {
        sql_func('MY_WEIRD_FUNC', @_);
    }

    my_weird_func( 1, 3 );

results in an expression with:

    SQL:          MY_WEIRD_FUNC(?, ?)
    Bind values:  [ 1, 3 ]

Plus the appropriate bind_type values according to the expression
arguments.

=back

=head1 SEE ALSO

L<SQL::DB>, L<sqldb-builder>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2007-2011 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

=cut

# vim: set tabstop=4 expandtab:

