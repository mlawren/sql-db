package SQL::DB::Function;
use strict;
use warnings;
use overload '""' => 'as_string', fallback => 1;
use Carp qw(croak confess);
use UNIVERSAL;
use SQL::DB::Expr;
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
        if (UNIVERSAL::isa($_, 'SQL::DB::Expr')) {
            push(@vals, $_);
            push(@bind, $_->bind_values);
        }
        else {
            push(@vals, $_);
        }
    }
    return SQL::DB::Expr->new($name .'('. join(', ',@vals) .')', @bind);

}


# FIXME set a flag somewhere so that SQL::DB::Row doesn't create a
# modifier method
sub coalesce {
    scalar @_ >= 2 || croak 'coalesce() requires at least two argument';

    my $new;
    if (UNIVERSAL::isa($_[0], 'SQL::DB::Expr')) {
        $new = $_[0]->_clone();
    }
    else {
        $new = SQL::DB::Expr->new;
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
        if (UNIVERSAL::isa($_, 'SQL::DB::Expr')) {
            push(@vals, "'$_'");
            push(@bind, $_->bind_values);
        }
        else {
            push(@vals, "'$_'");
        }
    }
    return SQL::DB::Expr->new($name .'('. join(', ',@vals) .')', @bind);

}


sub nextval {
    return do_function_quoted('nextval', @_);
}


sub currval {
    return do_function_quoted('currval', @_);
}


sub setval {
    my $expr = SQL::DB::Expr->new;
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
