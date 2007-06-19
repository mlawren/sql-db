package SQL::DB::Query::Insert;
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
        confess "Can only insert into columns of the same table";
    }
    return;
}


sub values {
    my $self   = shift;
    my $values = shift;

    if (!$values) {
        confess 'usage: values(\@values)';
    }

    $self->push_bind_values(@{$values});
}


sub sql {
    my $self = shift;

    my $s = 'INSERT INTO '. $self->{arows}->[0]->_table->name
            . ' ('
            . join(', ', map {$_->name} @{$self->{columns}})
            . ') VALUES ('
            . join(', ', map {'?'} $self->bind_values)
            . ')'
    ;

    return $s;
}


1;
__END__
