package SQL::DB::Expr;
use strict;
use warnings;
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
    groups => { default => [qw/ /], },
};

use overload

  #  '""'     => '_as_string',
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
  '.'      => '_expr_addstr',
  '.='     => '_expr_addstr',
  fallback => 1,
  ;

our $VERSION = '0.97_3';

has '_txt' => (
    is       => 'rw',
    required => 1,
);

has '_alias' => ( is => 'rw', );

has '_btype' => (
    is => 'rw',

    #    isa => sub { die "Must be HASH ref: $_[0]" unless ref $_[0] eq 'HASH'},
);

has '_bvalues' => (
    is  => 'rw',
    isa => sub { die "Must be ARRAY ref" unless ref $_[0] eq 'ARRAY' },
    default => sub { [] },
);

has '_btypes' => (
    is  => 'rw',
    isa => 'ArrayRef',
    isa => sub { die "Must be ARRAY ref" unless ref $_[0] eq 'ARRAY' },
    default => sub { [] },
);

has '_op' => (
    is      => 'rw',
    default => '',
);

has '_multi' => (
    is      => 'rw',
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
        $self->_alias( $name . $i );
        $self->_txt( $name . ' AS ' . $name . $i );
    }
}

sub _clone {
    my $self = shift;
    bless {%$self}, ref $self;
}

sub _as_string {
    my $self     = shift;
    my $internal = shift;

    if ( $internal and $self->_multi > 1 ) {
        return '(' . $self->_txt . ')';
    }
    return $self->_txt;
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

sub _expr_addstr {
    my ( $e1, $e2, $swap ) = @_;

    # The argument is undef
    if ( !defined $e2 ) {
        Carp::carp('Use of uninitialized value in concatenation (. or .=)');
        return $e1;
    }

    # Trying to catch some cpantesters errors
    if ( !ref $e1 ) {
        no warnings;
        warn "ERROR: _expr_addstr called with unblessed reference?";
        require Data::Dumper;
        $Data::Dumper::Indent   = 1;
        $Data::Dumper::Maxdepth = 2;
        warn Dumper(@_);
        warn Dumper( $e1, $e2 );
    }

    # When the argument is not an expression...
    unless ( eval { $e2->isa(__PACKAGE__) } ) {
        if ( $e1->_txt eq ' AND ' or $e1->_txt eq ' OR ' ) {
            croak ".AND. or .OR. only work with expressions. "
              . "(Missing brackets around previous/next expression?)";
        }

        return __PACKAGE__->new(
            _txt => $swap
            ? $e2 . $e1->_as_string(1)
            : $e1->_as_string(1) . $e2,
            _bvalues => [ @{ $e1->_bvalues } ],
            _btypes  => [ @{ $e1->_btypes } ],
            _multi   => $e1->_multi,
            _op      => $e1->_op,
        );
    }

    # The argument must be a kind of SQL::DB::Expr...

    # Arg is the result of a previous AND or OR
    if ( my $op = $e1->_op || $e2->_op ) {
        $e1->_op(undef);
        $e2->_op(undef);
        $op =~ s/\s+//g;
        return _expr_binary( $op, $e1, $e2, undef, 2 ) unless $swap;
        return _expr_binary( $op, $e2, $e1, undef, 2 );
    }

    # We should only encounter AND or OR on the RHS:
    my $op;
    if ( $e2->_txt eq ' AND ' or $e2->_txt eq ' OR ' ) {
        ( $op = $e2->_txt ) =~ s/\s+//g;
    }

    return __PACKAGE__->new(
        _txt => $e1->_as_string(1) . ( $op ? '' : $e2->_as_string(1) ),
        _bvalues => [ @{ $e1->_bvalues }, @{ $e2->_bvalues } ],
        _btypes  => [ @{ $e1->_btypes },  @{ $e2->_btypes } ],
        _op      => $op,
    );
}

sub _expr_not {
    my $e1   = shift;
    my $expr = $e1->_clone;
    $expr->_txt( 'NOT ' . $e1->_as_string(1) );
    $expr->_multi(0);
    return $expr;
}

sub _expr_eq { _expr_binary( '=', @_ ) }

sub _expr_ne { _expr_binary( '!=', @_ ) }

sub _expr_bitand { _expr_binary( '&', @_ ) }

sub _expr_bitor { _expr_binary( '|', @_ ) }

sub _expr_lt { _expr_binary( '<', @_ ) }

sub _expr_gt { _expr_binary( '>', @_ ) }

sub _expr_lte { _expr_binary( '<=', @_ ) }

sub _expr_gte { _expr_binary( '>=', @_ ) }

sub _expr_add { _expr_binary( '+', @_ ) }

sub _expr_sub { _expr_binary( '-', @_ ) }

sub _expr_mult { _expr_binary( '*', @_ ) }

sub _expr_divide { _expr_binary( '/', @_ ) }

sub is_null { $_[0] . ' IS NULL' }

sub is_not_null { $_[0] . ' IS NOT NULL' }

sub in {
    my $e1 = shift;
    return
      $e1 . ' IN ('
      . _expr_join( ',', map { _bexpr( $_, $e1->_btype ) } @_ ) . ')';
}

sub not_in {
    my $e1 = shift;
    return
        $e1
      . ' NOT IN ('
      . _expr_join( ',', map { _bexpr( $_, $e1->_btype ) } @_ ) . ')';
}

sub between {
    my $e1 = shift;
    croak 'between($a,$b)' unless @_ == 2;

    my $expr =
        $e1
      . ' BETWEEN '
      . _bexpr( $_[0], $e1->_btype ) . ' AND '
      . _bexpr( $_[1], $e1->_btype );
    $expr->_multi(1);
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
    $expr->_multi(1);
    return $expr;
}

sub as {
    my $e1 = shift;
    my $as = shift || croak 'as($value)';

    $as = ' AS ' . $as;    # this must be done first
    my $expr = $e1 . $as;
    $expr->_multi(0);
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
    my ( $op, $e1, $e2, $swap, $_multi ) = @_;

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
    $expr->_multi( $_multi || 1 );
    return $expr;
}

sub AND { __PACKAGE__->new( _txt => ' AND ' ) }
sub OR  { __PACKAGE__->new( _txt => ' OR ' ) }

1;
