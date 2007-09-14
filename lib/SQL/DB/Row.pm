package SQL::DB::Row;
use strict;
use warnings;
use Carp qw(croak);


sub make_class_from {
    my $proto = shift;
    @_ || croak 'make_class_from() requires arguments';
    my $class = $proto .'::'. join('_',@_);

    no strict 'refs';
    my $isa = \@{$class . '::ISA'};
    if (defined @{$isa}) {
        return $class;
    }

    use strict;
    my $pkg =  'package '.$class .';our @ISA = \''.$proto.'\';'."\n";
    my $i = 0;
    foreach my $method (@_) {
        $pkg .= "sub $method {\$_[0]->[$i];};\n";
        $i++;
    }
    eval $pkg;
    return $class unless($@);
    warn $pkg;
    croak($@);
}


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = shift || croak 'new requires ARRAYREF argument';
    ref($self) eq 'ARRAY' || croak 'new requires ARRAYREF argument';
    bless($self, $class);
    return $self;
}


1;
__END__
