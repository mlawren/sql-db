package SQL::DB::Expr;
use strict;
use warnings;
use Moo;
use Carp qw/ carp croak confess/;
use DBI qw/looks_like_number/;
use Sub::Exporter -setup => {
    exports => [
        qw/
          AND
          OR
          _sql
          _quote
          _bval
          _expr_binary
          _expr_join
          _query
          /
    ],
    groups => { default => [qw/ /], },
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
  '.'      => '_expr_addstr',
  '.='     => '_expr_addstr',
  fallback => 1,
  ;

our $VERSION = '0.19_14';
our $tcount  = {};

# ########################################################################
# FUNCTIONS
# ########################################################################

sub AND {
    SQL::DB::Expr->new(
        _txt   => [' AND '],
        _logic => 1
    );
}

sub OR {
    SQL::DB::Expr->new(
        _txt   => [' OR '],
        _logic => 1
    );
}

sub _sql {
    my $val = shift;

    return $val if ( ref $val ) =~ m/^SQL::DB::Expr/;

    return SQL::DB::Expr::SQL->new( val => $val );
}

sub _quote {
    my $val = shift;

    return $val if ( ref $val ) =~ m/^SQL::DB::Expr/;

    return SQL::DB::Expr::SQL->new( val => $val ) if looks_like_number($val);

    return SQL::DB::Expr::Quote->new( val => $val );
}

sub _bval {
    my ( $val, $type ) = @_;

    return $val if ( ref $val ) =~ m/^SQL::DB::Expr/;

    return SQL::DB::Expr::SQL->new( val => $val ) if looks_like_number($val);

    return SQL::DB::Expr::BindValue->new( val => $val, type => $type );
}

sub _expr_join {
    my $sep  = shift;
    my $last = pop @_;

    my $e = SQL::DB::Expr->new(
        _txt => [
            (
                map {
                    eval { $_->isa('SQL::DB::Expr') }
                      ? ( $_->_txts, $sep )
                      : ( $_, $sep )
                  } @_
            ),
            eval { $last->isa('SQL::DB::Expr') } ? $last->_txts : $last
        ]
    );
    return $e;
}

sub _query {
    return $_[0] if ( @_ == 1 and eval { $_[0]->isa('SQL::DB::Expr') } );
    my $e = SQL::DB::Expr->new;

    eval {
        while ( my ( $keyword, $item ) = splice( @_, 0, 2 ) )
        {
            if ( ref $keyword ) {
                $e .= $keyword . "\n";
            }
            else {
                ( my $tmp = uc($keyword) ) =~ s/_/ /g;
                $e .= $tmp . "\n";
            }

            next unless defined $item;
            if ( ref $item eq 'SQL::DB::Expr' ) {
                $e .= '    ' . $item . "\n";
            }
            elsif ( ref $item eq 'ARRAY' ) {
                my @new = map { ref $_ ? $_ : _bval($_) } @$item;
                $e .= '    ' . _expr_join( ",\n    ", @new ) . "\n";
            }
            elsif ( ref $item eq 'SCALAR' ) {
                $e .= '    ' . $$item . "\n";
            }
            else {
                $e .= '    ' . $item . "\n";
            }

            $e->_multi(0);
        }
    };

    confess "Bad Query: $@" if $@;
    return $e;
}

# ########################################################################
# OBJECT INTERFACE
# ########################################################################

has '_txt' => (
    is => 'rw',
    isa =>
      sub { confess "Must be ARRAY ref: $_[0]" unless ref $_[0] eq 'ARRAY' },
    default => sub { [] },
);

has '_alias' => ( is => 'rw', );

has '_type' => ( is => 'rw', );

has '_multi' => (
    is      => 'rw',
    default => sub { 0 },
);

has '_logic' => (
    is      => 'rw',
    default => sub { 0 },
);

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
        $self->_txt( [ $name . ' AS ' . $name . $i ] );
    }
}

sub _txts {
    return @{ shift->_txt };
}

sub _clone {
    my $self = shift;
    bless {%$self}, ref $self;
}

sub _as_string {
    my $self     = shift;
    my $internal = shift;

    return join( '', map { defined $_ ? $_ : '*UNDEF*' } $self->_txts );
}

# A true internal function - don't use outside this package
sub _push {
    my $self = shift;
    push( @{ $self->_txt }, @_ );
}

# A true internal function - don't use outside this package
sub _unshift {
    my $self = shift;
    unshift( @{ $self->_txt }, @_ );
}

sub _expr_addstr {
    my ( $e1, $e2, $swap ) = @_;

    # The argument is undef
    if ( !defined $e2 ) {
        Carp::carp('Use of uninitialized value in concatenation (. or .=)');
        return $e1;
    }

    my $res;

    my $multi = $e1->_multi + ( eval { $e2->_multi } || 0 );

    # $e2 . $e1 (or $e2 .= $e1)
    if ($swap) {
        if ( eval { $e2->isa(__PACKAGE__) } ) {
            $res = __PACKAGE__->new(
                _txt   => [ $e2->_txts, $e1->_txts ],
                _multi => $multi,
                _logic => $e1->_logic,
            );
        }
        else {
            $res = __PACKAGE__->new(
                _txt   => [ $e2, $e1->_txts ],
                _multi => $multi,
                _logic => $e1->_logic,
            );
        }
    }

    # $e1 . $e2
    elsif ( defined $swap ) {

        my $logic = 0;
        my $multi = 0;
        if ( eval { $e2->_logic } ) {
            if ( $e1->_multi ) {
                $e1->_unshift('(');
                $e1->_push(')');
            }
            $logic = 1;
        }
        elsif ( $e1->_logic ) {
            if ( eval { $e2->_multi } ) {
                $e2->_unshift('(');
                $e2->_push(')');
            }
            $multi = 0;
        }

        if ( eval { $e2->isa(__PACKAGE__) } ) {
            $res = __PACKAGE__->new(
                _txt   => [ $e1->_txts, $e2->_txts ],
                _multi => $multi,
                _logic => $logic,
            );
        }
        else {
            $res = __PACKAGE__->new(
                _txt   => [ $e1->_txts, $e2 ],
                _multi => $multi,
                _logic => $logic,
            );
        }
    }

    # $e1 .= $e2
    else {
        my $logic = 0;
        my $multi = 0;
        if ( eval { $e2->_logic } ) {
            if ( $e1->_multi ) {
                $e1->_unshift('(');
                $e1->_push(')');
            }
            $logic = 1;
        }
        elsif ( $e1->_logic ) {
            if ( eval { $e2->_multi } ) {
                $e2->_unshift('(');
                $e2->_push(')');
            }
            $multi = 1;
        }

        if ( eval { $e2->isa(__PACKAGE__) } ) {
            $e1->_push( $e2->_txts );
            $e1->_multi($multi);
            $e1->_logic($logic);
        }
        else {
            $e1->_push($e2);
            $e1->_multi($multi);
            $e1->_logic($logic);
        }
        $res = $e1;
    }

    return $res;
}

sub _expr_not {
    my $e1   = shift;
    my $expr = SQL::DB::Expr->new . $e1;

    if ( $e1->_multi > 0 ) {
        $expr->_unshift('(');
        $expr->_push(')');
    }
    $expr->_unshift('NOT ');
    $expr->_multi(0);
    return $expr;
}

sub _expr_binary {
    my ( $op, $e1, $e2, $swap, $_multi ) = @_;

    my $e = SQL::DB::Expr->new;

    # TODO add ( ) bracketing for multi expressions?
    if ($swap) {
        $e .= _bval( $e2, $e1->_type );
        $e .= ( ' ' . $op . ' ' ) . $e1;
    }
    else {
        $e .= $e1 . ( ' ' . $op . ' ' );
        $e .= _bval( $e2, $e1->_type );
    }

    $e->_multi(1);
    return $e;
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
    if ( @_ >= 2 && $_[0] =~ m/^select/i ) {
        return $e1 . " IN (\n" . _query(@_) . ')';
    }
    return
      $e1 . ' IN ('
      . _expr_join( ', ', map { _bval( $_, $e1->_type ) } @_ ) . ')';
}

sub not_in {
    my $e1 = shift;
    if ( @_ >= 2 && $_[0] =~ m/^select/i ) {
        return $e1 . " NOT IN (\n" . _query(@_) . ')';
    }
    return
        $e1
      . ' NOT IN ('
      . _expr_join( ', ', map { _bval( $_, $e1->_type ) } @_ ) . ')';
}

sub between {
    my $e1 = shift;
    croak 'between($a,$b)' unless @_ == 2;

    my $e = SQL::DB::Expr->new(
        _txt => [
            $e1->_txts,
            ' BETWEEN ',
            _bval( $_[0], $e1->_type ),
            ' AND ',
            _bval( $_[1], $e1->_type )
        ],
    );
    return $e;
}

sub not_between {
    my $e1 = shift;
    croak 'not_between($a,$b)' unless @_ == 2;

    my $e = SQL::DB::Expr->new(
        _txt => [
            $e1->_txts,
            ' NOT BETWEEN ',
            _bval( $_[0], $e1->_type ),
            ' AND ',
            _bval( $_[1], $e1->_type )
        ],
    );
    return $e;
}

sub as {
    my $e1 = shift;
    my $as = shift || croak 'as($value)';

    if ( $e1->_multi > 0 ) {
        my $expr = SQL::DB::Expr->new( _txt => ['('] );
        $expr .= $e1;
        $expr .= ') AS ' . $as;
        return $expr;
    }

    $as = ' AS ' . $as;    # this must be done first
    my $expr = $e1 . $as;
    return $expr;
}

sub like {
    my $e1   = shift;
    my $like = shift || croak 'like($value)';
    my $expr = $e1 . ' LIKE ';
    $expr .= _bval( $like, $e1->_type );
    $expr->_multi(0);
    return $expr;
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
        delete $tcount->{$1}->[$2];
    }
}

package SQL::DB::Expr::SQL;
use strict;
use warnings;
use Moo;
use overload '""' => sub {
    my $self = shift;
    $self->val;
  },
  fallback => 1;

has val => (
    is       => 'ro',
    required => 1,
);

package SQL::DB::Expr::Quote;
use strict;
use warnings;
use Moo;
use overload '""' => sub {
    my $self = shift;
    return 'q{' . ( defined $self->val ? $self->val : 'undef' ) . '}';
  },
  fallback => 1;

has val => (
    is       => 'ro',
    required => 1,
);

package SQL::DB::Expr::BindValue;
use strict;
use warnings;
use Moo;
use Carp qw/confess/;
use overload '""' => sub {
    my $self = shift;
    return
        'bv{'
      . ( defined $self->val  ? $self->val  : 'undef' ) . '}::'
      . ( defined $self->type ? $self->type : '(none)' );
  },
  fallback => 1;

has val => (
    is       => 'ro',
    required => 1,
);

has type => ( is => 'rw', );

1;
