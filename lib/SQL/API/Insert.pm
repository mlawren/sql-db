package SQL::API::Insert;
use strict;
use warnings;
use base qw(SQL::API::Query);
use Carp qw(croak confess);


sub insert {
    my $self = shift;

    if (!@_) {
        confess 'usage: insert(@columns)';
    }

    if ($self->{values} and
        my $count = scalar(@{$self->{values}})) { # values already defined
        if ($count != scalar(@_)) {
            confess "# of values must be same as # of inserted columns";
        }
    }

    $self->{insert} = \@_;
    $self->{arow}  = $_[0]->_arow;

    foreach (@{$self->{insert}}) {
        if ($_->_arow != $self->{arow}) {
            croak "Can only insert into columns of the same table";
        }
        if (!ref($_) or !$_->isa('SQL::API::AColumn')) {
            croak "arguments must be of type SQL::API::AColumn";
        }
    }
    return $self;
}

sub values {
    my $self = shift;

    if (!@_) {
        confess 'usage: values(@values)';
    }

    if ($self->{insert} and 
        my $count = scalar(@{$self->{insert}})) { # columns already defined
        if ($count != scalar(@_)) {
            confess "# of values must be same as # of inserted columns";
        }
    }

    $self->push_bind_values(@_);
}


sub sql {
    my $self = shift;

    my $s = 'INSERT INTO '. $self->{arow}->_table->name
            . ' ('
            . join(', ', map {$_->_name} @{$self->{insert}})
            . ') VALUES ('
            . join(', ', map {'?'} $self->bind_values)
            . ')'
    ;

    return $s;
}


sub columns {
    my $self = shift;
    return map {$_->_table} @{$self->{insert}};
}


1;
__END__
