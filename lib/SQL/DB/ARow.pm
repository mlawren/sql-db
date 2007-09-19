package SQL::DB::ARow;
use strict;
use warnings;
use base qw(Class::Accessor);
use Carp qw(carp croak confess);
use SQL::DB::AColumn;


our $tcount = 0;
our $DEBUG;


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {
        arow_tid => $tcount++,
    };
    bless($self, $class);

    foreach my $col ($self->_table->columns) {
        my $acol = SQL::DB::AColumn->new($col, $self);

        push(@{$self->{arow_columns}}, $acol);
        $self->{arow_column_names}->{$col->name} = $acol;
        $self->{$col->name} = $acol;
    }
    return $self;
}


sub _table {
    my $self = shift;
    no strict 'refs';
    return ${(ref($self) || $self) . '::TABLE'};
}


sub _table_name {
    my $self = shift;
    return $self->_table->name;
}


sub _alias {
    my $self = shift;
    return 't'. $self->{arow_tid};
}


sub _columns {
    my $self = shift;
    return @{$self->{arow_columns}};
}


sub _column_names {
    my $self = shift;
    return $self->_table->column_names_ordered;
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($SQL::DB::DEBUG && $SQL::DB::DEBUG>3);
}

1;
__END__
# vim: set tabstop=4 expandtab:
