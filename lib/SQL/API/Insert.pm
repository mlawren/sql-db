package SQL::API::Insert;
use strict;
use warnings;
use base qw(SQL::API::Query);
use Carp qw(croak confess);


sub insert {
    my $self = shift;
    my $insert = shift;

    foreach (@{$insert}) {
        unless (ref($_) and ($_->isa('SQL::API::AColumn') or
                            $_->isa('SQL::API::ARow'))) {
            confess "insert needs AColumn or ARow" . $_;
        }

        if ($_->isa('SQL::API::AColumn')) {
            if ($self->{arow} and $self->{arow} != $_->_arow) {
                confess "Can only insert into columns of the same table";
            }
            $self->{arow} = $_->_arow;
            push(@{$self->{insert}}, $_);
            push(@{$self->{columns}}, $_->_column);
        }
        else {
            if ($self->{arow} and $self->{arow} != $_) {
                confess "Can only insert into columns of the same table";
            }
            $self->{arow} = $_;
            push(@{$self->{insert}}, $_->_columns);
            push(@{$self->{columns}}, map {$_->_column} $_->_columns);
        }
    }

    unless ($self->{insert}) {
        confess "insert needs AColumn or ARow";
    }
    return $self;
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
