package SQL::DB::Iter;
use Moo;
use Sub::Install qw/install_sub/;
use Carp qw(croak);

our $VERSION = '0.971.0';

has 'sth' => (
    is       => 'ro',
    required => 1,
);

has 'class' => (
    is       => 'rw',
    writer   => '_class',
    init_arg => undef,
);

# '_done' is a bool set to true when there are no more rows to
# be returned.
has '_done' => (
    is       => 'rw',
    init_arg => undef,
);

my %classes;

sub BUILD {
    my $self = shift;
    return if $self->class;

    my @cols = map { $_ =~ s/\s+/_/g; $_ } @{ $self->sth->{NAME_lc} };
    my $class = 'SQL::DB::Row::' . join( '_', @cols );

    if ( !$classes{$class} ) {
        my $i = 0;
        my $x = eval $i;
        foreach my $col (@cols) {
            my $x = eval $i;
            install_sub(
                {
                    code => sub {
                        $_[0]->[$x] = $_[2] if @_ == 2;
                        return $_[0]->[$x];
                    },
                    into => $class,
                    as   => $col,
                }
            );
            $i++;
        }
        $classes{$class} = 1;
    }
    $self->class($class);
}

sub next {
    my $self = shift;
    return $self->object;
}

sub all {
    my $self = shift;
    return $self->objects;
}

sub object {
    my $self = shift;
    return if ( $self->_done );

    my @values = $self->sth->fetchrow_array;

    if ( !@values ) {
        $self->finish;
        return;
    }
    return bless \@values, $self->class;
}

sub objects {
    my $self = shift;
    my $all  = $self->arrays;

    my $class = $self->class;

    foreach ( 0 .. $#{$all} ) {
        bless $all->[$_], $class;
    }

    $self->finish;
    return @$all if wantarray;
    return $all;
}

sub hash {
    my $self = shift;
    return if ( $self->_done );

    my $ref = $self->sth->fetchrow_hashref('NAME_lc');

    if ( !$ref ) {
        $self->finish;
        return;
    }
    return $ref;
}

sub hashes {
    my $self = shift;
    my @all;

    while ( my $ref = $self->sth->fetchrow_hashref('NAME_lc') ) {
        push( @all, $ref );
    }
    $self->finish;
    return @all if wantarray;
    return \@all;
}

sub array {
    my $self = shift;
    return if ( $self->_done );

    my $ref = $self->sth->fetchrow_arrayref;

    if ( !$ref ) {
        $self->finish;
        return;
    }
    return $ref;
}

sub arrays {
    my $self = shift;

    my $all = $self->sth->fetchall_arrayref;
    $self->finish;
    return @$all if wantarray;
    return $all;
}

sub finish {
    my $self = shift;
    return if $self->_done;
    $self->sth->finish;
    $self->_done(1);
}

sub DESTROY {
    my $self = shift;
    $self->finish;
}

1;
