package SQL::API::Expr;
use strict;
use warnings;
use Carp;
use overload
    '""' => 'sql',
    '!' => 'expr_not',
    'not' => 'expr_not',
    '==' => 'expr_eq',
    '&' => 'expr_and',
    '|' => 'expr_or',
    '<' => 'expr_lt',
    '>' => 'expr_gt',
;


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        expr_val         => shift,
        expr_multi       => 0,
        expr_bind_values => \@_,
    };

    bless($self, $class);
    return $self;
}


sub multi {
    my $self = shift;
    $self->{expr_multi} = shift if(@_);
    return $self->{expr_multi};
}


sub push_bind_values {
    my $self = shift;
    push(@{$self->{expr_bind_values}}, @_);
}


sub bind_values {
    my $self = shift;
    return @{$self->{expr_bind_values}};
}


sub expr_eq {
    my $expr = shift;
    my $val  = shift;
    if (ref($val) and $val->isa('SQL::API::Expr')) {
        return __PACKAGE__->new($expr .' = '. $val);
    }
    return __PACKAGE__->new($expr .' = ?', $val);
}


sub is_null {
    my $expr = shift;
    return __PACKAGE__->new($expr .' IS NULL');
}


sub expr_lt {
    my $expr = shift;
    my $val  = shift;
    if (ref($val) and $val->isa('SQL::API::Expr')) {
        return __PACKAGE__->($expr .' < '. $val);
    }
    my $t =  __PACKAGE__->new($expr .' < ?', $val);
    $t->multi(1);
    return $t;
}


sub expr_gt {
    my $expr = shift;
    my $val  = shift;
    if (ref($val) and $val->isa('SQL::API::Expr')) {
        return __PACKAGE__->new($expr .' > '. $val);
    }
    my $t =  __PACKAGE__->new($expr .' > ?', $val);
    $t->multi(1);
    return $t;
}


sub expr_not {
    my $expr = shift;
    if (ref($expr) and $expr->isa('SQL::API::AColumn')) {
        return __PACKAGE__->new($expr . ' NOT NULL');
    }
    if (ref($expr) and $expr->multi) {
        return __PACKAGE__->new('NOT '. $expr .'');
    }
    return __PACKAGE__->new('NOT '. $expr);
}


sub expr_and {
    my $expr1 = shift;
    my $expr2 = shift;

    my @values = $expr1->bind_values;
    if (ref($expr2) and $expr2->isa('SQL::API::Expr')) {
        push(@values, $expr2->bind_values);
    }

    my $newexpr = __PACKAGE__->new($expr1 .' AND '. $expr2, @values);
    $newexpr->multi(1);
    return $newexpr;
}


sub expr_or {
    my $expr1 = shift;
    my $expr2 = shift;

    my @values = $expr1->bind_values;
    if (ref($expr2) and $expr2->isa('SQL::API::Expr')) {
        push(@values, $expr2->bind_values);
    }

    my $newexpr = __PACKAGE__->new($expr1 .' OR '. $expr2, @values);
    $newexpr->multi(1);
    return $newexpr;
}


sub in {
    my $expr1 = shift;

    my @values = $expr1->bind_values;
    my @exprs;

    foreach my $e (@_) {
        if (ref($e) and $e->isa('SQL::API::Expr')) {
            if ($e->isa('SQL::API')) {
                $e->nobind(1);
            }
            push(@exprs, $e);
            push(@values, $e->bind_values);
        }
        else {
            push(@exprs, '?');
            push(@values, $e);
        }
    }

    return __PACKAGE__->new($expr1 .' IN ('.join(', ',@exprs).')', @values);
}



sub sql {
    my $self = shift;
    if ($self->multi) {
        return '(' . $self->{expr_val} .')';
    }
    return $self->{expr_val};
}


1;
__END__
