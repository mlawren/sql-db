package SQL::DB::Query::Delete;
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
        confess "Can only delete columns from a single table";
    }
    return;
}


sub sql {
    my $self = shift;

    $self->get_aliases($self->{arows}->[0]);

    my $where = $self->where_sql;
    while (my ($alias,$table) = each %{$self->{aliases}}) {
        $where =~ s/$alias\./$table\./g;
    }

    my $s = 'DELETE FROM '
            . $self->{arows}->[0]->_table->name
            . $where
    ;

    return $s;
}


1;

__END__
