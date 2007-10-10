package SQL::DB::Schema::ARow;
use strict;
use warnings;
use base qw(Class::Accessor);
use Carp qw(carp croak confess);
use SQL::DB::Schema::AColumn;
use Scalar::Util qw(weaken);


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
        my $acol = SQL::DB::Schema::AColumn->new($col, $self);

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


=head1 NAME

SQL::DB::Schema::ARow - description

=head1 SYNOPSIS

  use SQL::DB::Schema::ARow;

=head1 DESCRIPTION

B<SQL::DB::Schema::ARow> is ...

=head1 METHODS

=head2 new



=head2 _table



=head2 _table_name



=head2 _alias



=head2 _columns



=head2 _column_names



=head1 FILES



=head1 SEE ALSO

L<Other>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
