package SQL::DB::AColumn;
use strict;
use warnings;
use base qw(SQL::DB::Expr);
use Carp qw(carp croak confess);
use Scalar::Util qw(weaken);
use UNIVERSAL qw(isa);


sub _new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = $class->SUPER::new;

    my $col   = shift;
    my $arow  = shift;
    $self->{col}  = $col;  # column definition SQL::DB::AColumn
    $self->{arow} = $arow; # abstract representation of a table row
    $self->{sql}  = $arow->_alias .'.'. $col->name;
    $self->{as}   = $self->{sql};

#    weaken($self->{arow}); FIXME or cleanup and remove all weak stuff.

    bless($self, $class);
    return $self;
}


sub _clone {
    my $self  = shift;
    my $class = ref($self) || croak 'can only _clone blessed objects';
    my $new   = $self->SUPER::new();
    map {$new->{$_} = $self->{$_}} keys %$self;
    $new->{expr_bind_values} = []; # FIXME this is a hack
    bless($new, $class);
    return $new;
}


sub _name {
    my $self = shift;
    return $self->{as};
}


sub _column {
    my $self = shift;
    return $self->{col};
}


sub _arow {
    my $self = shift;
    return $self->{arow};
}


sub as {
    my $self     = shift;
    $self        = $self->_clone();
    $self->{as}  = shift || croak 'as() requires an argument';
    $self->{sql} = $self->{arow}->_alias .'.'. $self->{col}->name
                   .' AS '. $self->{as};
    return $self;
}


sub is_null {
    my $self     = shift;
    $self        = $self->_clone();
    $self->{sql} = $self->{arow}->_alias .'.'. $self->{col}->name
                   .' IS NULL';
    return $self;
}
sub expr_not {is_null(@_);}


sub like {
    my $self     = shift;
    my $like     = shift || croak 'like() requires an argument';
    $self        = $self->_clone();
    $self->{sql} = $self->{arow}->_alias .'.'. $self->{col}->name
                   .' LIKE ?';
    $self->push_bind_values($like);
    return $self;
}


sub asc {
    my $self     = shift;
    $self        = $self->_clone();
    $self->{sql} = $self->{arow}->_alias .'.'. $self->{col}->name
                   .' ASC';
    return $self;
}


sub desc {
    my $self     = shift;
    $self        = $self->_clone();
    $self->{sql} = $self->{arow}->_alias .'.'. $self->{col}->name
                   .' DESC';
    return $self;
}


sub set {
    my $self     = shift;
    my $val      = shift || croak 'set() requires an argument';
    $self        = $self->_clone();
    if (UNIVERSAL::isa($val, 'SQL::DB::Expr')) {
        $self->{sql} = $self->{col}->name .' = '. $val;
        $self->push_bind_values($val->bind_values);
    }
    else {
        $self->{sql} = $self->{col}->name .' = ?';
        $self->push_bind_values($val);
    }
    return $self;
}


sub as_string {
    my $self = shift;
    return $self->{sql};
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($SQL::DB::DEBUG && $SQL::DB::DEBUG>3);
}


1;
__END__
# vim: set tabstop=4 expandtab:
