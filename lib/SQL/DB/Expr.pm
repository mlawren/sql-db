package SQL::DB::Expr;
use strict;
use warnings;
use Carp;
use UNIVERSAL qw(isa);
use overload
    'eq'     => 'expr_eq',
    '=='     => 'expr_eq',
    '!='     => 'expr_ne',
    'ne'     => 'expr_ne',
    '&'      => 'expr_and',
    '!'      => 'expr_not',
    '|'      => 'expr_or',
    '<'      => 'expr_lt',
    '>'      => 'expr_gt',
    '<='     => 'expr_lte',
    '>='     => 'expr_gte',
    '+'      => 'expr_plus',
    '-'      => 'expr_minus',
    '""'     => 'as_string',
    fallback => 1,
;


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        expr_val         => shift,
        expr_op          => '',
        expr_multi       => 0,
        expr_bind_values => \@_,
    };
    bless($self, $class);

    # This is due to some wierdness in Perl - seems to call new() twice.
    if (isa($self->{expr_val}, __PACKAGE__)) {
        return $self->{expr_val};
    }

    return $self;
}


sub _clone {
    my $self  = shift;
    my $class = ref($self) || croak 'can only _clone blessed objects';
    my $new   = {};
    map {$new->{$_} = $self->{$_}} keys %$self;
    bless($new, $class);
    $new->reset_bind_values();
    return $new;
}


sub as {
    my $self        = shift;
    my $new         = $self->_clone();
    $new->{expr_as} = shift || croak 'as() requires an argument';
    $new->push_bind_values($self->bind_values);
    if ($self->op) {
        $new->set_val('('.$self->val .') AS '. $new->{expr_as});
    }
    else {
        $new->set_val($self->val .' AS '. $new->{expr_as});
    }
    return $new;
}


sub _as {
    my $self = shift;
    return $self->{expr_as};
}


sub val {
    my $self = shift;
    return $self->{expr_val};
}


sub set_val {
    my $self = shift;
    if (@_) {
        $self->{expr_val} = shift;
        return;
    }
    croak 'set_val requires an argument';
}


sub reset_bind_values {
    my $self = shift;
    $self->{expr_bind_values} = [];
}


sub push_bind_values {
    my $self = shift;
    push(@{$self->{expr_bind_values}}, @_);
}


sub bind_values {
    my $self = shift;
    return @{$self->{expr_bind_values}};
}


sub multi {
    my $self = shift;
    $self->{expr_multi} = shift if(@_);
    return $self->{expr_multi};
}


sub op {
    my $self = shift;
    $self->{expr_op} = shift if(@_);
    return $self->{expr_op};
}


sub as_string {
    my $self = shift;
    if ($self->{expr_multi}) {
        return '(' . $self->{expr_val} .')';
    }
    return $self->{expr_val};
}


sub bind_values_sql {
    my $self = shift;
    if (my @vals = $self->bind_values) {
        return '/* ('
           . join(", ", map {defined $_ ? "'$_'" : 'NULL'} @vals)
           . ') */';
    }
    return '';
}


sub _as_string {
    my $self = shift;
    my @values = $self->bind_values;
    return $self->as_string . $self->bind_values_sql . "\n";
}



sub expr_binary {
    my ($e1,$op,$e2) = @_;
    my @bind = ();

    if (isa($e1, __PACKAGE__)) {
        push(@bind, $e1->bind_values);
        if ( ($op eq 'OR' and $e1->op =~ m/(OR)|(AND)/) or
             ($op eq 'AND' and $e1->op =~ m/OR/)) {
            # always return a new expression, because if we set multi
            # on the current object we screw it up when it is used in
            # other queries.
            $e1 = $e1->new("$e1", $e1->bind_values);
            $e1->multi(1);
        }
    }
    else {
        push(@bind, $e1);
        $e1 = '?';
    }

    if (isa($e2, __PACKAGE__)) {
        push(@bind, $e2->bind_values);
        if ( ($op eq 'OR' and $e2->op =~ m/(OR)|(AND)/) or
             ($op eq 'AND' and $e2->op =~ m/OR/)) {
            # same as above
            $e2 = $e2->new("$e2", $e2->bind_values);
            $e2->multi(1);
        }
    }
    else {
        push(@bind, $e2);
        $e2 = '?';
    }

    my $expr = __PACKAGE__->new($e1.' '.$op.' '.$e2, @bind);
    $expr->op($op);
    return $expr;
}

sub expr_eq {
    return expr_binary($_[0],'=',$_[1]);
}

sub expr_ne {
    return expr_binary($_[0],'!=',$_[1]);
}

sub expr_and {
    return expr_binary($_[0],'AND',$_[1]);
}

sub expr_or {
    return expr_binary($_[0],'OR',$_[1]);
}

sub expr_lt {
    return expr_binary($_[0],'<',$_[1]);
}

sub expr_lte {
    return expr_binary($_[0],'<=',$_[1]);
}

sub expr_gt {
    return expr_binary($_[0],'>',$_[1]);
}

sub expr_gte {
    return expr_binary($_[0],'>=',$_[1]);
}

sub expr_plus {
    return expr_binary($_[0],'+',$_[1]);
}

sub expr_minus {
    return expr_binary($_[0],'-',$_[1]);
}


sub expr_not {
    my $e1 = shift;
    my $expr = __PACKAGE__->new('NOT ('.$e1.')', $e1->bind_values);
    $expr->op('NOT');
    return $expr;
}


sub in {
    my $expr1 = shift;
    my @bind = $expr1->bind_values;
    my @exprs;

    foreach my $e (@_) {
        if (isa($e, __PACKAGE__)) {
            push(@exprs, $e);
            push(@bind, $e->bind_values);
        }
        else {
            push(@exprs, '?');
            push(@bind, $e);
        }
    }

    return __PACKAGE__->new($expr1 .' IN ('.join(', ',@exprs).')', @bind);
}


sub not_in {
    my $expr1 = shift;
    my @bind = $expr1->bind_values;
    my @exprs;

    foreach my $e (@_) {
        if (isa($e, __PACKAGE__)) {
            push(@exprs, $e);
            push(@bind, $e->bind_values);
        }
        else {
            push(@exprs, '?');
            push(@bind, $e);
        }
    }

    return __PACKAGE__->new($expr1 .' NOT IN ('.join(', ',@exprs).')', @bind);
}


sub between {
    my $expr1 = shift;
    my @bind = $expr1->bind_values;
    my @exprs;

    foreach my $e (@_) {
        if (isa($e, __PACKAGE__)) {
            push(@exprs, $e);
            push(@bind, $e->bind_values);
        }
        else {
            push(@exprs, '?');
            push(@bind, $e);
        }
    }

    my $new =  __PACKAGE__->new($expr1 .' BETWEEN '.
                                 join(' AND ', @exprs), @bind);
    $new->multi(1);
    return $new;
}


1;
__END__
# vim: set tabstop=4 expandtab:
