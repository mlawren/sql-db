package SQL::DB::Schema::Function;
use strict;
use warnings;
use overload '""' => 'as_string', fallback => 1;
use Carp qw(croak confess);
use UNIVERSAL;
use SQL::DB::Schema::Expr;
use Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    coalesce
    count
    max
    min
    sum
    cast
    now
    nextval
    currval
    setval
);



sub do_function {
    my $name = shift;

    my @vals;
    my @bind;

    foreach (@_) {
        if (UNIVERSAL::isa($_, 'SQL::DB::Schema::Expr')) {
            push(@vals, $_);
            push(@bind, $_->bind_values);
        }
        else {
            push(@vals, $_);
        }
    }
    return SQL::DB::Schema::Expr->new($name .'('. join(', ',@vals) .')', @bind);

}


# FIXME set a flag somewhere so that SQL::DB::Row doesn't create a
# modifier method
sub coalesce {
    scalar @_ >= 2 || croak 'coalesce() requires at least two argument';

    my $new;
    if (UNIVERSAL::isa($_[0], 'SQL::DB::Schema::Expr')) {
        $new = $_[0]->_clone();
    }
    else {
        $new = SQL::DB::Schema::Expr->new;
    }
    $new->set_val('COALESCE('. join(', ', @_) .')');
    return $new;
}


sub count {
    return do_function('COUNT', @_);
}


sub min {
    return do_function('MIN', @_);
}


sub max {
    return do_function('MAX', @_);
}


sub sum {
    return do_function('SUM', @_);
}


sub cast {
    return do_function('CAST', @_);
}


sub now {
    return do_function('NOW');
}


sub do_function_quoted {
    my $name = shift;

    my @vals;
    my @bind;

    foreach (@_) {
        if (UNIVERSAL::isa($_, 'SQL::DB::Schema::Expr')) {
            push(@vals, "'$_'");
            push(@bind, $_->bind_values);
        }
        else {
            push(@vals, "'$_'");
        }
    }
    return SQL::DB::Schema::Expr->new($name .'('. join(', ',@vals) .')', @bind);

}


sub nextval {
    return do_function_quoted('nextval', @_);
}


sub currval {
    return do_function_quoted('currval', @_);
}


sub setval {
    my $expr = SQL::DB::Schema::Expr->new;
    if (@_ == 2) {
        $expr->set_val('setval(\''. $_[0] .'\', '.  $_[1] .')');
    }
    elsif (@_ == 3) {
        $expr->set_val('setval(\''. $_[0] .'\', '.  $_[1] .', '.
                           ($_[2] ? 'true' : 'false') .')');
    }
    else {
        confess 'setval() takes 2 or 3 arguments';
    }

    return $expr;
}


1;
__END__
# vim: set tabstop=4 expandtab:


=head1 NAME

SQL::DB::Schema::Function - description

=head1 SYNOPSIS

  use SQL::DB::Schema::Function;

=head1 DESCRIPTION

B<SQL::DB::Schema::Function> is ...

=head1 METHODS

=head2 do_function



=head2 coalesce



=head2 count



=head2 min



=head2 max



=head2 sum



=head2 cast



=head2 now



=head2 do_function_quoted



=head2 nextval



=head2 currval



=head2 setval



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
