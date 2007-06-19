package SQL::DB::Query::Update;
use strict;
use warnings;
use base qw(SQL::DB::Query);
use Carp qw(croak confess);


sub columns {
    my $self = shift;

    if (!@_) {
        return $self->SUPER::columns;
    }

    $self->SUPER::columns(@_);

    if (@{$self->{arows}} > 1) {
        confess "Can only update columns from a single table";
    }
    return;
}


sub set {
    my $self = shift;
    my $vals = shift;
    $self->push_bind_values(@{$vals});
}


sub sql {
    my $self = shift;

    $self->get_aliases($self->{arows}->[0]);

    my $where = $self->where_sql;
    while (my ($alias,$table) = each %{$self->{aliases}}) {
        $where =~ s/$alias\./$table\./g;
    }

    return 'UPDATE '
            . $self->{arows}->[0]->_table->name
            . ' SET ' 
            . (join(', ', map {$_->name .' = ?'} @{$self->{columns}}))
            . $where
    ;
}


1;
__END__

