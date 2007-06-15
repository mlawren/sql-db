package SQL::API::Select;
use strict;
use warnings;
use base qw(SQL::API::Query);
use Carp qw(croak confess carp);


sub select {
    my $self   = shift;
    my $select = shift;
    foreach (@{$select}) {
        if (!ref($_)) {
            confess "select needs AColumn or ARow";
        }
        elsif ($_->isa('SQL::API::AColumn')) {
            push(@{$self->{select}}, $_);
            push(@{$self->{columns}}, $_->_column)
        }
        elsif ($_->isa('SQL::API::ARow')) {
            push(@{$self->{select}}, $_->_columns);
            push(@{$self->{columns}}, map {$_->_column} $_->_columns);
        }
        else {
            confess "select needs AColumn or ARow";
        }
    }
    return $self;
}


sub distinct {
    my $self = shift;
    $self->{distinct} = shift;
    return;
}



sub select_sql {
    my $self = shift;

    $self->get_aliases(map {$_->_arow} @{$self->{select}});

    my $s = 'SELECT';

    if ($self->{distinct}) {
        $s .= ' DISTINCT';
        if (ref($self->{distinct}) eq 'ARRAY') {
            $self->get_aliases(map {$_->_arow} @{$self->{distinct}});
            $s .= ' ON (' . join(', ', @{$self->{distinct}}) . ')';
        }
    }

    $s .= "\n    " .
            join(",\n    ", @{$self->{select}});

    $s .= "\nFROM\n    " .
            join(",\n    ", $self->aliases);

    return $s;
}


sub order_by {
    my $self = shift;
    $self->{order_by} = shift;
    return $self;
}


sub order_by_sql {
    my $self = shift;
    if ($self->{order_by}) {
        return "ORDER BY\n    " .
               join(",\n    ", @{$self->{order_by}});
    }
    return '';
}


sub limit {
    my $self = shift;
    $self->{limit} = shift;
    return;
}


sub limit_sql {
    my $self = shift;
    if ($self->{limit}) {
        return "\nLIMIT ". $self->{limit}. "\n";
    }
    return '';
}


sub sql {
    my $self = shift;

    return
          $self->select_sql
        . $self->where_sql
#        . $self->group_by_sql
#        . $self->having_sql
        . $self->order_by_sql
        . $self->limit_sql
    ;

}


sub columns {
    my $self = shift;
    return @{$self->{columns}};
}

sub column_names {
    my $self = shift;
    return map {$_->name} @{$self->{select}};
}


1;
__END__

