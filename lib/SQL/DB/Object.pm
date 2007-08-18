package SQL::DB::Object;
use strict;
use warnings;
use base qw(Class::Accessor);
use Carp qw(carp croak confess);

sub mutator_name_for {'set_'.$_[1]};

sub set {
    my $self = shift;
    $self->{_changed}->{$_[0]} = 1;
    $self->SUPER::set(@_);
}

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $hash  = shift || {};
    my $self  = $class->SUPER::new($hash);
    $self->{_changed} = $hash;
    bless($self,$class);
    return $self;
}

sub _table {
    my $self = shift;
    no strict 'refs';
    return ${ref($self) . '::TABLE'};
}


sub arow {
    my $proto = shift;
    no strict 'refs';
    return ${$proto . '::TABLE'}->abstract_row;
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

    if ($self->{_in_storage}) {
        confess "Cannot insert objects already in storage";
    }

    my $arow    = $self->_table->abstract_row;
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

    if (!$self->{_in_storage}) {
        confess "Cannot update objects not already in storage";
    }

    my $arow    = $self->_table->abstract_row;
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
        update => [ map {$arow->$_} @changed ],
        set    => [ map {$self->$_} @changed ],
        where  => $where,
    );
}


1;
__END__
# vim: set tabstop=4 expandtab:
