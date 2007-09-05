package SQL::DB::AColumn;
use strict;
use warnings;
use base qw(SQL::DB::Expr);
use Carp qw(carp croak confess);
use Scalar::Util qw(weaken);


sub _new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = $class->SUPER::new;

    my $col   = shift;
    my $arow  = shift;
    $self->{col}  = $col;  # column definition SQL::DB::AColumn
    $self->{arow} = $arow; # abstract representation of a table row

#    weaken($self->{arow}); FIXME or cleanup and remove all weak stuff.

    bless($self, $class);
    return $self;
}


sub clone {
    my $self = shift;
    my $new  = $self->_new($self->{col}, $self->{arow});
}


sub _column {
    my $self = shift;
    return $self->{col};
}


sub _name {
    my $self = shift;
    return $self->{_as} if($self->{_as});
    return $self->{col}->name;
}


sub _arow {
    my $self = shift;
    return $self->{arow};
}


sub as {
    my $self = shift;
    $self->{_as} = shift if(@_);
    return $self;
}


sub is_null {
    my $self = shift;
    return $self->sql .' IS NULL';
}


sub like {
    my $self = shift;
    my $val  = shift;
    if (ref($val) and $val->isa('SQL::DB::Expr')) {
        return SQL::DB::Expr->new($self .' LIKE '. $val, $val->bind_values);
    }
    my $newexpr =  SQL::DB::Expr->new($self .' LIKE ?', $val);
    return $newexpr;
}

sub expr_not {
    my $self = shift;
    return $self->sql .' NOT NULL';
}


sub asc {
    my $self = shift;
    return SQL::DB::Expr->new($self->sql . ' ASC');
}


sub desc {
    my $self = shift;
    return SQL::DB::Expr->new($self->sql . ' DESC');
}


sub func {
    my $self = shift;
    my $new  = $self->clone;
    $new->{_func} = shift;
    $new->{_as} = $new->{_func} .'_'. $new->_name;
    return $new;
}


use UNIVERSAL;
sub set {
    my $self = shift;
    if (@_) {
        my $set = shift;
        my $expr;
        if (UNIVERSAL::isa($set, 'SQL::DB::Expr')) {
            return SQL::DB::Expr->new($self->{col}->name .' = '. $set->sql,
                                      $set->bind_values);
        }
        return SQL::DB::Expr->new($self->{col}->name .' = ?', $set);
    }
    confess "set() is write-only";
#    return $self->{set};
#    my $val  = shift;
#    if (ref($val) and $val->isa('SQL::DB::Expr')) {
#        return SQL::DB::Expr->new($self .' = '. $val, $val->bind_values);
#    }
#    my $newexpr =  SQL::DB::Expr->new($self .' = ?', $val);
#    return $newexpr;
}


sub sql {
    my $self = shift;
    return $self->{arow}->_alias .'.'. $self->{col}->name;
}


sub sql_select {
    my $self = shift;
    if ($self->{_func}) {
        return uc($self->{_func}) .'('. $self->{arow}->_alias 
               .'.'. $self->{col}->name .')'
               .' AS '. $self->{_as};
    }
    if ($self->{_as}) {
        return $self->{arow}->_alias .'.'. $self->{col}->name
               .' AS '. $self->{_as};
    }
    return $self->{arow}->_alias .'.'. $self->{col}->name;
} 


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($SQL::DB::DEBUG && $SQL::DB::DEBUG>3);
}


1;
__END__
# vim: set tabstop=4 expandtab:
