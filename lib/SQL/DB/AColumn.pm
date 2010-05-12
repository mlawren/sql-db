package SQL::DB::AColumn;
use Mouse;
use Carp qw(carp croak confess);
use UNIVERSAL qw(isa);

extends 'SQL::DB::Expr';

has '_column' => (
    is => 'ro',
    isa => 'SQL::DB::Column',
    required => 1,
);

has '_arow' => (
    is => 'ro',
    isa => 'SQL::DB::ARow',
    weak_ref => 1,
    required => 1,
);

sub BUILD {
    my $self = shift;
    $self->_as( $self->_column->name );
}


sub expr_not {is_null(@_);}
sub is_null {
    my $self     = shift;
    $self        = $self->_clone();
    $self->set_val(
        $self->_arow->_alias .'.'. $self->_column->name .' IS NULL'
    );
    return $self;
}


sub is_not_null {
    my $self     = shift;
    $self        = $self->_clone();
    $self->set_val(
        $self->_arow->_alias .'.'. $self->_column->name .' IS NOT NULL'
    );
    return $self;
}


sub like {
    my $self     = shift;
    my $like     = shift || croak 'like() requires an argument';
    $self        = $self->_clone();
    $self->set_val(
        $self->_arow->_alias .'.'. $self->_column->name .' LIKE ?'
    );
    $self->push_bind_values($like);
    return $self;
}


sub asc {
    my $self     = shift;
    $self        = $self->_clone();
    $self->set_val(
        $self->_arow->_alias .'.'. $self->_column->name .' ASC'
    );
    return $self;
}


sub desc {
    my $self     = shift;
    $self        = $self->_clone();
    $self->set_val(
        $self->_arow->_alias .'.'. $self->_column->name .' DESC'
    );
    return $self;
}


sub set {
    my $self     = shift;
    @_ || confess 'set() requires an argument:'. $self;
    my $val      = shift;
    $self        = $self->_clone();
    if (UNIVERSAL::isa($val, 'SQL::DB::Expr')) {
        $self->set_val( $self->_column->name .' = '. $val );
        $self->push_bind_values( $val->bind_values );
    }
    else {
        $self->set_val( $self->_column->name .' = ?' );
        $self->push_bind_values( $val );
    }
    return $self;
}


DESTROY {
    my $self = shift;
    warn "DESTROY $self" if($SQL::DB::DEBUG && $SQL::DB::DEBUG>3);
}


1;
__END__
# vim: set tabstop=4 expandtab:


=head1 NAME

SQL::DB::AColumn - description

=head1 SYNOPSIS

  use SQL::DB::AColumn;

=head1 DESCRIPTION

B<SQL::DB::AColumn> is ...

=head1 METHODS

=head2 new



=head2 _column



=head2 _arow



=head2 is_null



=head2 is_not_null

SQL: IS NOT NULL


=head2 expr_not



=head2 like



=head2 asc



=head2 desc



=head2 set



=head1 FILES



=head1 SEE ALSO

L<Other>

=head1 AUTHOR

Mark Lawrence E<lt>nomad@null.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007,2008 Mark Lawrence <nomad@null.net>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

=cut

# vim: set tabstop=4 expandtab:
