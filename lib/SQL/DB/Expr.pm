package #hide from CPAN
    SQL::DB::Expr::Logic;
use Mouse;
use overload
    '.'      => 'expr_concat',
    fallback => 1,
;

extends 'SQL::DB::Expr';


has 'left' => (
    is => 'rw',
    isa => 'Any',
);

has 'right' => (
    is => 'rw',
    isa => 'Any',
);


sub expr_concat {
#    warn join(',',
#        map { defined $_
#            ? (ref $_ ? ref $_ : "$_")
#            : '*undef*' } caller, 'expr_concat(', @_, ')',
#    );
    my $self = shift;
    my $val = shift;

    # Make a copy
    if ( ! defined $self->left ) {
        my $new = $self->_clone;
        $new->push_bind_values( $self->bind_values );
        $new->left( $val );
        return $new;
    }

    # We are the copy
    my $op = 'expr_'. $self->op;
    return $self->left->$op( $val );
#    my $new = $self->left->_clone;
#    bless( $new, 'SQL::DB::Expr');
##    $new->push_bind_values( $self->left->bind_values );
#    return $new->$op( $val );
}


1;

package SQL::DB::Expr;
use Mouse;
use Carp qw/ carp croak /;
use Exporter qw/ import /;
use UNIVERSAL qw(isa);
use overload
    'eq'     => 'expr_eq',
    '=='     => 'expr_eq',
    '!='     => 'expr_ne',
    'ne'     => 'expr_ne',
    '&'      => 'expr_bitand',
    '|'      => 'expr_bitor',
    '!'      => 'expr_not',
    '<'      => 'expr_lt',
    '>'      => 'expr_gt',
    '<='     => 'expr_lte',
    '>='     => 'expr_gte',
    '+'      => 'expr_plus',
    '-'      => 'expr_minus',
    '*'      => 'expr_multiply',
    '/'      => 'expr_divide',
    '""'     => 'as_string',
    fallback => 1,
;

our @EXPORT = ( qw/
    AND
    OR
/ );



has 'val' => (
    is => 'rw',
    isa => 'Any',
    required => 1,
    writer => 'set_val',
);

has '_as' => (
    is => 'rw',
    isa => 'Str',
);

has 'op' => (
    is => 'rw',
    isa => 'Str',
    default => '',
);

has 'multi' => (
    is => 'rw',
    isa => 'Bool',
    default => 0,
);

has 'bind_values' => (
    is => 'rw',
    isa => 'ArrayRef',
    default => sub { [] },
    auto_deref => 1,
);


our $AND = SQL::DB::Expr::Logic->new( val => '' );
our $OR = SQL::DB::Expr::Logic->new( val => '' );

$AND->op( 'and' );
$OR->op( 'or' );

sub AND () { $AND };
sub OR  () { $OR };


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
    $new->_as(shift) || croak 'as() requires an argument';
    $new->push_bind_values($self->bind_values);
    if ($self->op) {
        $new->set_val('('.$self->val .') AS '. $new->_as);
    }
    else {
        $new->set_val($self->val .' AS '. $new->_as);
    }
    return $new;
}


sub reset_bind_values {
    my $self = shift;
    $self->bind_values( [] );
}


sub push_bind_values {
    my $self = shift;
    push( @{ $self->bind_values }, @_ );
}


sub as_string {
    my $self = shift;
    if ( $self->multi ) {
        return '(' . $self->val .')';
    }
    return $self->val;
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
        if ($e1->multi
            or ($op =~ /^OR/ and $e1->op =~ /^(OR)|(AND)/)
            or ($op =~ /^AND/ and $e1->op =~ /^OR/)) {
            # always return a new expression, because if we set multi
            # on the current object we screw it up when it is used in
            # other queries.
            $e1 = __PACKAGE__->new(
                val => "$e1",
                bind_values => [ $e1->bind_values ],
            );
            $e1->multi(1);
        }
    }
    else {
        push(@bind, $e1);
        $e1 = '?';
    }

    if (isa($e2, __PACKAGE__)) {
        push(@bind, $e2->bind_values);
        if ($e2->multi
            or ($op =~ /^OR/ and $e2->op =~ /^(OR)|(AND)/)
            or ($op =~ /^AND/ and $e2->op =~ /^OR/)) {
            # same as above
            $e2 = __PACKAGE__->new(
                val => "$e2",
                bind_values => [ $e2->bind_values ],
            );
            $e2->multi(1);
        }
    }
    else {
        push(@bind, $e2);
        $e2 = '?';
    }

    my $expr = __PACKAGE__->new(
        val => $e1.' '.$op.' '.$e2,
        bind_values => \@bind,
    );
    $expr->op($op);
    return $expr;
}

sub expr_eq {
    return expr_binary($_[0],'=',$_[1]);
}

sub expr_ne {
    return expr_binary($_[0],'!=',$_[1]);
}

sub expr_bitand {
    carp "Use of '&' for AND is depreciated.";
    return expr_binary($_[0],'AND',$_[1]);
}

sub expr_bitor {
    carp "Use of '|' for OR is depreciated.";
    return expr_binary($_[0],'OR',$_[1]);
}

sub expr_and {
    return expr_binary($_[0],'AND',$_[1]);
}

sub expr_or {
    return expr_binary($_[0],'OR',$_[1]);
}

#sub AND {
#    return expr_binary($_[0],'AND',$_[1]);
#}
#
#sub OR {
#    return expr_binary($_[0],'OR',$_[1]);
#}

sub expr_lt {
    if ($_[2]) {
        return expr_binary($_[1],'<',$_[0]);
    }
    else {
        return expr_binary($_[0],'<',$_[1]);
    }
}

sub expr_lte {
    if ($_[2]) {
        return expr_binary($_[1],'<=',$_[0]);
    }
    else {
        return expr_binary($_[0],'<=',$_[1]);
    }
}

sub expr_gt {
    if ($_[2]) {
        return expr_binary($_[1],'>',$_[0]);
    }
    else {
        return expr_binary($_[0],'>',$_[1]);
    }
}

sub expr_gte {
    if ($_[2]) {
        return expr_binary($_[1],'>=',$_[0]);
    }
    else {
        return expr_binary($_[0],'>=',$_[1]);
    }
}

sub expr_plus {
    if ($_[2]) {
        return expr_binary($_[1],'+',$_[0]);
    }
    else {
        return expr_binary($_[0],'+',$_[1]);
    }
}

sub expr_minus {
    if ($_[2]) {
        return expr_binary($_[1],'-',$_[0]);
    }
    else {
        return expr_binary($_[0],'-',$_[1]);
    }
}

sub expr_multiply {
    if ($_[2]) {
        return expr_binary($_[1],'*',$_[0]);
    }
    else {
        return expr_binary($_[0],'*',$_[1]);
    }
}

sub expr_divide {
    if ($_[2]) {
        return expr_binary($_[1],'/',$_[0]);
    }
    else {
        return expr_binary($_[0],'/',$_[1]);
    }
}


sub like {
    return expr_binary($_[0],'LIKE',$_[1]);
}


sub concat {
    return expr_binary($_[0],'||',$_[1]);
}


sub expr_not {
    my $e1 = shift;
    my $expr = __PACKAGE__->new(
        val => 'NOT ('.$e1.')',
        bind_values => [ $e1->bind_values ],
    );
    $expr->op('NOT');
    return $expr;
}


sub is_null {
    my $e = shift;
    my $expr = __PACKAGE__->new(
        val => $e .' IS NULL',
        bind_values => [ $e->bind_values ],
    );
    $expr->op('IS NULL');
    return $expr;
}


sub is_not_null {
    my $e = shift;
    my $expr = __PACKAGE__->new(
        val => $e .' IS NOT NULL',
        bind_values => [ $e->bind_values ],
    );
    $expr->op('IS NOT NULL');
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
        elsif (ref($e) and ref($e) eq 'ARRAY') {
            push(@exprs, map {'?'} @$e);
            push(@bind, @$e);
        }
        else {
            push(@exprs, '?');
            push(@bind, $e);
        }
    }

    return __PACKAGE__->new(
        val => $expr1 .' IN ('.join(', ',@exprs).')',
        bind_values => \@bind,
    );
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
        elsif (ref($e) and ref($e) eq 'ARRAY') {
            push(@exprs, map {'?'} @$e);
            push(@bind, @$e);
        }
        else {
            push(@exprs, '?');
            push(@bind, $e);
        }
    }

    return __PACKAGE__->new(
        val => $expr1 .' NOT IN ('.join(', ',@exprs).')',
        bind_values => \@bind,
    );
}


sub between {
    my $expr1 = shift;
    my @bind = $expr1->bind_values;
    my @exprs;

    if (@_ != 2) {
        croak 'between($a,$b)';
    }

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

    my $new =  __PACKAGE__->new(
        val => $expr1 .' BETWEEN '.  join(' AND ', @exprs),
        bind_values => \@bind,
    );
    $new->multi(1);
    return $new;
}


sub not_between {
    my $expr1 = shift;
    my @bind = $expr1->bind_values;
    my @exprs;

    if (@_ != 2) {
        croak 'between($a,$b)';
    }

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

    my $new =  __PACKAGE__->new(
        val => $expr1 .' NOT BETWEEN '.  join(' AND ', @exprs),
        bind_values => \@bind,
    );
    $new->multi(1);
    return $new;
}


1;
__END__
# vim: set tabstop=4 expandtab:


=head1 NAME

SQL::DB::Expr - description

=head1 SYNOPSIS

  use SQL::DB::Expr;

=head1 DESCRIPTION

B<SQL::DB::Expr> is ...

=head1 METHODS

=head2 new



=head2 _clone



=head2 as



=head2 _as



=head2 val



=head2 set_val



=head2 reset_bind_values



=head2 push_bind_values



=head2 bind_values



=head2 multi



=head2 op



=head2 as_string



=head2 bind_values_sql



=head2 _as_string



=head2 expr_binary



=head2 expr_eq



=head2 expr_ne



=head2 expr_and

=head2 expr_or

=head2 and

=head2 or

=head2 and_not

=head2 or_not



=head2 expr_lt



=head2 expr_lte



=head2 expr_gt



=head2 expr_gte



=head2 expr_plus



=head2 expr_minus


=head2 expr_multiply


=head2 expr_divide


=head2 expr_not


=head2 is_null

=head2 is_not_null


=head2 like

=head2 concat

$e1->concat($e2) eq 'e1 || e2'


=head2 in


=head2 not_in


=head2 between


=head2 not_between


=head1 FILES


=head1 SEE ALSO

L<Other>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007,2008 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
