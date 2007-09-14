package SQL::DB::Object;
use strict;
use warnings;
use base qw(Class::Accessor);
use Carp qw(carp croak confess);


sub mutator_name_for {'set_'.$_[1]};


sub set {
    my $self = shift;
    my $name = $_[0];
    if ($self->_table->column($name)) {
        $self->{_changed}->{$name} = 1;
    }
    return $self->SUPER::set(@_);
}


sub get {
    my $self = shift;
#    warn "GET $_[0] ". join(', ', keys %$self) . $self->{_in_storage};
    if ($self->{_in_storage} &&
        $self->_table->column($_[0]) && !exists($self->{$_[0]})) {
        confess 'Attempt to access '.ref($self) .'.'. $_[0]
                .' which has not been set/fetched.';
    }
    return $self->SUPER::get(@_);
}


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $hash  = shift || {};
    my $self  = $class->SUPER::new($hash);
    bless($self,$class);
    map {$self->_table->column($_) ? $self->{_changed}->{$_} = 1 : undef} keys %{$hash};
    return $self;
}


sub _table {
    my $self = shift;
    my $proto = ref($self) || $self;
    no strict 'refs';
    return ${ref($self) . '::TABLE'};
}


sub arow {
    my $proto = shift;
    my $class = (ref($proto) || $proto) .'::Abstract';
    return $class->_new;
}


sub _changed {
    my $self = shift;
    if (@_) {
        return exists($self->{_changed}->{$_[0]}) && $self->{_changed}->{$_[0]};
    }
    return keys %{$self->{_changed}};
}


sub _in_storage {
    my $self = shift;
    $self->{_in_storage} = shift if(@_);
    return $self->{_in_storage};
}


sub q_insert {
    my $self = shift;
    my $arow    = $self->arow;
    my @changed = $self->_changed;

    if (!@changed) {
        carp "$self has no values to insert";
    }

    return (
        insert => [ map {$arow->$_} @changed ],
        values => [ map {$self->$_} @changed ],
    );
}


sub q_update {
    my $self    = shift;
    my $arow    = $self->arow;
    my @primary = $self->_table->primary_columns;
    my @changed = $self->_changed;

    if (!@changed) {
        carp "$self has nothing to update";
    }

    my $where;
    foreach my $colname (map {$_->name} @primary) {
        $where = $where ? ($where & ($arow->$colname == $self->$colname))
                        : ($arow->$colname == $self->$colname)
    }

    return (
        update => [ map {$arow->$_->set($self->$_)} @changed],
        where  => $where,
    );
}


sub q_delete {
    my $self    = shift;
    my $arow    = $self->arow;
    my @primary = $self->_table->primary_columns;

    my $where;
    foreach my $colname (map {$_->name} @primary) {
        $where = $where ? ($where & ($arow->$colname == $self->$colname))
                        : ($arow->$colname == $self->$colname)
    }

    return (
        delete_from => $arow,
        where  => $where,
    );
}


1;
__END__


# vim: set tabstop=4 expandtab:
