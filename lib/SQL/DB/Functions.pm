package SQL::DB::Functions;
use strict;
use warnings;
use Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    coalesce
);

sub coalesce {
    return SQL::DB::Function::Coalesce->new(@_);
}


package SQL::DB::Function::Coalesce;
use strict;
use warnings;
#use base qw(SQL::DB::Expr);
use overload '""' => 'as_string', fallback => 1;


sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless($self, $class);

    $self->{cols} = \@_;
    return $self;
}


sub as {
    my $self = shift;
    $self->{name} = shift;
    return $self;
}


sub _arow {
    my $self = shift;
    return '(none)';
}

sub _column {
    my $self = shift;
    return '(none)';
}

sub _name {
    my $self = shift;
    return $self->{name};
}


sub sql {
    my $self = shift;
    die "missing AS for Coalese" unless($self->{name});

    return 'COALESCE('
            . join(', ', map {$_->sql} @{$self->{cols}})
            . ') AS '
            . $self->{name};
}
sub sql_select {sql(@_)};

sub as_string {
    my $self = shift;
    return $self->sql;
}

1;
__END__
# vim: set tabstop=4 expandtab:
