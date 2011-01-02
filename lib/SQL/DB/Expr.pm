package SQL::DB::Expr;
use Moo;
use Carp qw/ carp croak confess/;
use Sub::Exporter -setup => {
    exports => [
        qw/
          AND
          OR
          _bexpr
          _expr_binary
          _expr_join
          _bexpr_join
          /
    ],
    groups => {
        default => [
            qw/
              /
        ],
    },
};

use overload
  '""'     => '_as_string',
  '!'      => '_expr_not',
  '=='     => '_expr_eq',
  '!='     => '_expr_ne',
  '&'      => '_expr_bitand',
  '|'      => '_expr_bitor',
  '<'      => '_expr_lt',
  '>'      => '_expr_gt',
  '<='     => '_expr_lte',
  '>='     => '_expr_gte',
  '+'      => '_expr_add',
  '-'      => '_expr_sub',
  '*'      => '_expr_mult',
  '/'      => '_expr_divide',
  '.='     => '_expr_concat',
  '.'      => '_expr_concat2',
  fallback => 1,

  #    'eq'     => '_expr_eq',
  #    'ne'     => '_expr_ne',
  ;

our $VERSION = '0.19_3';

has '_txt' => (
    is       => 'rw',
    required => 1,
    writer   => '_set_txt',
);

has '_alias' => (
    is     => 'rw',
    writer => '_set_alias',
);

has '_btype' => (
    is => 'ro',

    #    isa => sub { die "Must be HASH ref: $_[0]" unless ref $_[0] eq 'HASH'},
);

has '_bvalues' => (
    is  => 'ro',
    isa => sub { die "Must be ARRAY ref" unless ref $_[0] eq 'ARRAY' },
    default => sub { [] },
);

has '_btypes' => (
    is  => 'ro',
    isa => 'ArrayRef',
    isa => sub { die "Must be ARRAY ref" unless ref $_[0] eq 'ARRAY' },
    default => sub { [] },
);

has '_op' => (
    is      => 'ro',
    default => '',
);

has '_multi' => (
    is  => 'ro',
    isa => sub {
        confess "Must be undef, 0 or 1"
          unless ( !defined $_[0] || ( $_[0] == 0 or $_[0] == 1 ) );
    },
    default => sub { 0 },
);

our $tcount = {};

sub BUILD {
    my $self = shift;

    if ( my $name = $self->_alias ) {
        $tcount->{$name} ||= [];
        my $i = 0;
        while ( $tcount->{$name}->[$i] ) {
            $i++;
        }
        $tcount->{$name}->[$i] = 1;
        $self->_set_alias( $name . $i );
        $self->_set_txt( $name . ' AS ' . $name . $i );
    }
}

sub clone {
    my $self = shift;
    bless {%$self}, ref $self;
}

sub _as_string {
    my $self     = shift;
    my $internal = shift;

    return $self->_txt unless $internal;
    return $self->_multi ? '(' . $self->_txt . ')' : $self->_txt;
}

sub _bvalues_sql {
    my $self = shift;
    if ( my @_txts = @{ $self->_bvalues } ) {
        return
          '/* ('
          . join( ", ", map { defined $_ ? "'$_'" : 'NULL' } @_txts ) . ') */';
    }
    return '';
}

sub _expr_concat {

    #no warnings 'uninitialized';
    #Carp::carp "_expr_concat#". join('#',map { ref $_ } @_);
    #Carp::carp "_expr_concat#". join('#', @_);
    #warn caller;
    my ( $e1, $e2 ) = @_;
    my $_txt;
    my @_bvalues;
    my @_btypes;
    my $_multi = $e1->_op ? 1 : 0;
    my $op = '';

    # The argument is undef
    defined $e2
      or Carp::carp('Use of uninitialized value in concatenation (.) or string')
      and return $e1;

    use Data::Dumper;
    $Data::Dumper::Indent   = 1;
    $Data::Dumper::Maxdepth = 2;

    #warn Dumper($e2);
    # The argument is one of us
    if ( eval { ref $e2 ? $e2->isa(__PACKAGE__) : 0 } ) {
        if ( my $op = delete $e1->{_op} || delete $e2->{_op} ) {
            $op =~ s/\s+//g;
            return _expr_binary( $op, $e1, $e2, undef, 1 );
        }
        my $op;
        if ( $e2->_txt eq ' AND ' or $e2->_txt eq ' OR ' ) {

    #        warn "$e1";
    #            croak ".AND. or .OR. only work with complex expressions. "
    #                    . "(Missing brackets around previous/next expression?)"
    #                    unless $e1->_multi;

            ( $op = $e2->_txt ) =~ s/\s+//g;
        }
        return __PACKAGE__->new(
            _txt => $e1->_as_string(1) . ( $op ? '' : $e2->_as_string(1) ),
            _bvalues => [ @{ $e1->_bvalues }, @{ $e2->_bvalues } ],
            _btypes  => [ @{ $e1->_btypes },  @{ $e2->_btypes } ],

            #            _multi => $e1->_multi || $e2->_multi,
            _op => $op,
        );
    }

    if ( $e1->_txt eq ' AND ' or $e1->_txt eq ' OR ' ) {
        croak ".AND. or .OR. only work with expressions. "
          . "(Missing brackets around previous/next expression?)";
    }

    # The argument is something else
    return __PACKAGE__->new(
        _txt     => $e1->_as_string(1) . $e2,
        _bvalues => [ @{ $e1->_bvalues } ],
        _btypes  => [ @{ $e1->_btypes } ],
        _multi   => $e1->_multi,
        _op      => $e1->_op,
    );
}

sub _expr_concat2 {

    #no warnings 'uninitialized';
    #Carp::carp "_expr_concat2(". join('#',map { ref $_ } @_);
    #Carp::carp "_expr_concat2(". join('#', @_);
    #warn caller;
    my ( $e1, $e2, $swap ) = @_;

    return $e1->_expr_concat($e2) unless $swap;

    my $_txt;
    my @_bvalues;
    my @_btypes;
    my $_multi = $e1->_op ? 1 : 0;
    my $op = '';

    # The argument is undef
    defined $e2
      or Carp::carp('Use of uninitialized value in concatenation (.) or string')
      and return $e1;

    # The argument is one of us
    if ( eval { ref $e2 ? $e2->isa(__PACKAGE__) : 0 } ) {
        if ( my $op = delete $e1->{_op} || delete $e2->{_op} ) {
            $op =~ s/\s+//g;
            return _expr_binary( $op, $e1, $e2, undef, 1 );
        }

        my $op;
        if ( $e2->_txt eq ' AND ' or $e2->_txt eq ' OR ' ) {
            ( $op = $e2->_txt ) =~ s/\s+//g;
        }

        return __PACKAGE__->new(
            _txt => $e2->_txt . $e1->_txt,
            _txt => ( $op ? '' : $e2->_as_string(1) ) . $e1->_as_string(1),
            _bvalues => [ @{ $e2->_bvalues }, @{ $e1->_bvalues } ],
            _btypes  => [ @{ $e2->_btypes },  @{ $e1->_btypes } ],
            _multi => $e2->_multi || $e1->_multi,
            _op => $op,
        );
    }

    if ( $e1->_txt eq ' AND ' or $e1->_txt eq ' OR ' ) {
        croak ".AND. or .OR. only work with expressions."
          . "(Missing brackets around previous/next expression?)";
    }

    # The argument is something else
    # FIXME try only modifying $e1 here instead of creating new object
    return __PACKAGE__->new(
        _txt     => $e2 . $e1->_txt,
        _bvalues => [ @{ $e1->_bvalues } ],
        _btypes  => [ @{ $e1->_btypes } ],
        _multi   => $e1->_multi,
        _op      => $e1->_op,
    );
}

sub _expr_not {
    my $e1   = shift;
    my $_txt = 'NOT ' . $e1->_as_string(1);
    my $expr = $e1->clone;
    $expr->{_txt}   = $_txt;
    $expr->{_multi} = undef;
    return $expr;
}

sub _expr_eq     { _expr_binary( '=',  @_ ) }
sub _expr_ne     { _expr_binary( '!=', @_ ) }
sub _expr_bitand { _expr_binary( '&',  @_ ) }
sub _expr_bitor  { _expr_binary( '|',  @_ ) }
sub _expr_lt     { _expr_binary( '<',  @_ ) }
sub _expr_gt     { _expr_binary( '>',  @_ ) }
sub _expr_lte    { _expr_binary( '<=', @_ ) }
sub _expr_gte    { _expr_binary( '>=', @_ ) }
sub _expr_add    { _expr_binary( '+',  @_ ) }
sub _expr_sub    { _expr_binary( '-',  @_ ) }
sub _expr_mult   { _expr_binary( '*',  @_ ) }
sub _expr_divide { _expr_binary( '/',  @_ ) }
sub is_null      { $_[0] . ' IS NULL' }
sub is_not_null  { $_[0] . ' IS NOT NULL' }

sub in {
    my $e1   = shift;
    my $last = pop @_;
    my $_txt = $e1 . ' IN (';
    map { $_txt .= _bexpr( $_, $e1->_btype ) . ', ' } @_;
    $_txt .= _bexpr( $last, $e1->_btype );
    $_txt .= ')';
    return $_txt;
}

sub not_in {
    my $e1   = shift;
    my $last = pop @_;
    my $_txt = $e1 . ' NOT IN (';
    map { $_txt .= _bexpr( $_, $e1->_btype ) . ', ' } @_;
    $_txt .= _bexpr( $last, $e1->_btype );
    $_txt .= ')';
    return $_txt;
}

sub between {
    my $e1 = shift;
    croak 'between($a,$b)' unless @_ == 2;

    my $expr =
        $e1
      . ' BETWEEN '
      . _bexpr( $_[0], $e1->_btype ) . ' AND '
      . _bexpr( $_[1], $e1->_btype );
    $expr->{_multi} = 1;
    return $expr;
}

sub not_between {
    my $e1 = shift;
    croak 'not_between($a,$b)' unless @_ == 2;

    my $expr =
        $e1
      . ' NOT BETWEEN '
      . _bexpr( $_[0], $e1->_btype ) . ' AND '
      . _bexpr( $_[1], $e1->_btype );
    $expr->{_multi} = 1;
    return $expr;
}

sub as {
    my $e1 = shift;
    my $as = shift || croak 'as($value)';

    $as = ' AS ' . $as;    # this must be done first
    my $expr = $e1 . $as;
    $expr->{_multi} = 0;
    return $expr;
}

sub like {
    my $e1 = shift;
    my $like = shift || croak 'like($value)';
    return $e1 . ' LIKE ' . _bexpr( $like, $e1->_btype );
}

sub asc {
    my $e1 = shift;
    return $e1 . ' ASC';
}

sub desc {
    my $e1 = shift;
    return $e1 . ' DESC';
}

DESTROY {
    my $self = shift;
    if ( my $alias = $self->_alias ) {
        $alias =~ m/^(.*?)(\d+)$/;
        $tcount->{$1}->[$2] = undef;
    }
}

# ########################################################################
# FUNCTIONS
# ########################################################################

sub _bexpr {
    my $e1   = shift;
    my $type = shift;

    return $e1 if ( eval { $e1->isa(__PACKAGE__) } );
    return __PACKAGE__->new(
        _txt     => '?',
        _bvalues => [$e1],
        _btypes  => [$type],
    );
}

sub _expr_join {
    my $sep = shift;
    return '' unless @_;
    return $_[0] if @_ == 1;

    my $last = pop @_;
    my $e = SQL::DB::Expr->new( _txt => '' );
    map { $e .= $_ . $sep } @_;
    $e .= $last;
    return $e;
}

sub _bexpr_join {
    my $sep = shift;
    return '' unless @_;
    return $_[0] if @_ == 1;

    my $last = pop @_;
    my $e = SQL::DB::Expr->new( _txt => '' );
    map { $e .= _bexpr($_) . $sep } @_;
    $e .= _bexpr($last);
    return $e;
}

sub _expr_binary {

    #no warnings 'uninitialized';
    #Carp::carp "_expr_binary#". join('#',map { ref $_ } @_);
    #Carp::carp "_expr_binary#". join('#', @_);
    my ( $op, $e1, $e2, $swap, $_multi ) = @_;

    #    $e1->dump;
    #    exit;

    if ($swap) {
        my $tmp = $e1;
        $e1 = $e2;
        $e2 = $tmp;
    }

    my $expr =
        _bexpr( $e1, eval { $e2->_btype } ) . ' ' 
      . $op . ' '
      . _bexpr( $e2, eval { $e1->_btype } );

    # FIXME what' the right behaviour here?
    $expr->{_multi} = $_multi || 1;
    return $expr;
}

sub AND { __PACKAGE__->new( _txt => ' AND ' ) }
sub OR  { __PACKAGE__->new( _txt => ' OR ' ) }

1;

__END__


=head1 NAME

SQL::DB::Expr - Build SQL expressions with Perl

=head1 SYNOPSIS

  use SQL::DB::Expr;

  my $t1 = SQL::DB::Expr->new(
    _txt => 'table1',
  );

  my $col1 = SQL::DB::Expr->new(
    _txt => 'col1',
    _btype => 'SQL_INTEGER',
  );

  my $col2 = SQL::DB::Expr->new(
    _txt => 'col2',
  );

  my $expr = expr(
    select_distinct => [
        $col1,
        sql_min( $col2 )->as( 'min_col2' ),
    ],
    from      => $t1,
    left_join => $t1,
    where     => ( $col1 > 13 ) .AND.  $col2->not_between( 3, 24 ),
    group_by  => $col1,
    order_by  => $col1->asc,
    limit     => 1,
    offset    => 13,
  );

  print "$expr\n";
  # SELECT DISTINCT
  #     col1,
  #     MIN(col2) AS 'min_col2',
  # FROM
  #     t1
  # LEFT JOIN
  #     t1 AS t2
  # WHERE
  #     (col1 > ? AND (col2 NOT BETWEEN ? AND ?))
  # GROUP BY
  #     col1
  # ORDER BY
  #     col1 ASC
  # LIMIT
  #     1
  # OFFSET
  #     13


  use Data::Dumper;
  print Dumper( $expr->_bvalues );
  # [ 13, 3, 24 ]

  print Dumper( $expr->_btypes );
  # [ 'SQL_INTEGER', undef, undef ]

=head1 DESCRIPTION

B<SQL::DB::Expr> provides an object class that supports mapping Perl
syntax to SQL text, values and types suitable for passing to L<DBI>
methods.  This class is the basis for L<SQL::DB> and is not generally
used directly.

=head1 CONSTRUCTOR

    new(
        _txt     => $_txt,
        _btype   => $bind_type,
        _bvalues => [ @_bvalues ],
        _btypes  => [ @_btypes ],
        _multi   => $_multi,
        _op      => $op,
    );

=head1 ATTRIBUTES

=over 4

=item _txt => Scalar

The SQL representation of the expression suitable for passing to DBI
methods such as do(), selectall_arrayref(), prepare(), etc.

=item _btype => Scalar

The DBD bind_type suitable for passing to a L<DBI> statement-handle
bind_param() method.

=item _bvalues => ARRAYREF

The values of the expression suitable for passing to a L<DBI>
statement-handle bind_param() method.

=item _btypes => ARRAYREF

The bind types of the expression _bvalues suitable for passing to a
L<DBI> statement handle bind_param() method.

=item _multi => Bool

Whether the expression requires surrounding brackets.

=item _op => Scalar

The operator of the expression in the case that it is binary.

=back

=head1 METHODS

=over 4

=item BUILD

Part of the object instantiation phase. Documented here to keep
Test::Pod happy.

=item clone

Makes a deep copy of the object.

=item _as_string

Returns a string representation of the expression, providing
surrounding brackets for '_multi' expressions.

=item _bvalues_sql

Returns a string representation of the bind values as an SQL comment.

=item _expr_not

Maps '!' to 'NOT'.

=item _expr_eq

Maps '==' to "==".

=item _expr_ne

Maps '!=' to "!=".

=item _expr_bitand

Maps '&' to "&";

=item _expr_bitor

Maps '|' to "|";

=item _expr_lt

Maps '<' to "<";

=item _expr_gt

Maps '>' to ">";

=item _expr_lte

Maps '<=' to "<=";

=item _expr_gte

Maps '>=' to ">=";

=item _expr_add

Maps '+' to "+";

=item _expr_sub

Maps '-' to "-";

=item _expr_mult

Maps '*' to "*";

=item _expr_divide

Maps '/' to "/";

=item is_null

Maps to "IS NULL".

=item is_not_null

Maps to "IS NOT NULL".

=item in(@values)

Maps to "IN (@VALUES)"

=item not_in( @values )

Maps to "NOT IN ( @VALUES )".

=item between( $a, $b )

Maps to "BETWEEN $a AND $b".

=item not_between( $a, $b )

Maps to "NOT BETWEEN $a AND $b".

=item as( $as )

Maps to "AS $as".

=item like( $like )

Maps to "LIKE $like".

=item asc

Maps to "ASC".

=item desc

Maps to "DESC".

=back

=head1 CLASS FUNCTIONS

The following functions are exported on demand.

=over 4

=item _bexpr( $item )

Return $item if it is already an expression, or a new SQL::DB::Expr
object otherwise.

=item _expr_binary( $op, $e1, $e2, $swap )

An internal method for building binary operator expressions.

=item _expr_join( $separator, @expressions )

Does the same as Perl's 'join' built-in but for SQL::DB::Expr objects.
See BUGS below for why this is needed.

=item _bexpr_join( $separator, @expressions )

Does the same as Perl's 'join' built-in but for SQL::DB::Expr objects,
but using _bexpr on each item of @expressions. See BUGS below for why
this is needed.

=item AND, OR

These subroutines let you write SQL logical expressions in Perl using
string concatenation:

    ( $e1 .AND. $e2 ) .OR. ( $e3 .AND. $e4 )

Note that due to operator precedence, expressions either side of .AND.
or .OR. should be bracketed if they are not already single expression
objects.

Things are implemented this way due to Perl not allowing the
overloading of the 'and' and 'or' built-ins.

=back

=head1 BUGS

Using B<SQL::DB::Expr> objects with the Perl "join" command does not
work as expected, apparently because join does not trigger either the
'.' or '.=' overload methods. The work around is to use the _expr_join
subroutine.

=head1 SEE ALSO

L<SQL::DB>, L<DBI>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2010 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

=cut

# vim: set tabstop=4 expandtab:
