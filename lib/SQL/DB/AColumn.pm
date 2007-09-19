package SQL::DB::AColumn;
use strict;
use warnings;
use base qw(SQL::DB::Expr);
use Carp qw(carp croak confess);
use Scalar::Util qw(weaken);
use UNIVERSAL qw(isa);


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = $class->SUPER::new;

    my $col   = shift;
    my $arow  = shift;
    $self->{col}  = $col;  # column definition SQL::DB::AColumn
    $self->{arow} = $arow; # abstract representation of a table row
    $self->{expr_as}   = $col->name; #FIXME shouldn't know about Expr internals
    $self->set_val($arow->_alias .'.'. $col->name);

#    weaken($self->{arow}); FIXME or cleanup and remove all weak stuff.

    bless($self, $class);
    return $self;
}


sub _column {
    my $self = shift;
    return $self->{col};
}


sub _arow {
    my $self = shift;
    return $self->{arow};
}


sub is_null {
    my $self     = shift;
    $self        = $self->_clone();
    $self->set_val($self->{arow}->_alias .'.'. $self->{col}->name .' IS NULL');
    return $self;
}
sub expr_not {is_null(@_);}


sub like {
    my $self     = shift;
    my $like     = shift || croak 'like() requires an argument';
    $self        = $self->_clone();
    $self->set_val($self->{arow}->_alias .'.'. $self->{col}->name .' LIKE ?');
    $self->push_bind_values($like);
    return $self;
}


sub asc {
    my $self     = shift;
    $self        = $self->_clone();
    $self->set_val($self->{arow}->_alias .'.'. $self->{col}->name .' ASC');
    return $self;
}


sub desc {
    my $self     = shift;
    $self        = $self->_clone();
    $self->set_val($self->{arow}->_alias .'.'. $self->{col}->name .' DESC');
    return $self;
}


sub set {
    my $self     = shift;
    @_ || confess 'set() requires an argument:'. $self;
    my $val      = shift;
    $self        = $self->_clone();
    if (UNIVERSAL::isa($val, 'SQL::DB::Expr')) {
        $self->set_val($self->{col}->name .' = '. $val);
        $self->push_bind_values($val->bind_values);
    }
    else {
        $self->set_val($self->{col}->name .' = ?');
        $self->push_bind_values($val);
    }
    return $self;
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($SQL::DB::DEBUG && $SQL::DB::DEBUG>3);
}


1;
__END__
# vim: set tabstop=4 expandtab:
