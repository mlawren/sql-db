package SQL::DBx::Simple;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak/;
use SQL::DB::Expr qw/_expr_join/;
use SQL::DB qw/sql_table sql_values/;

our $VERSION = '0.19_11';

# $db->insert_into('customers',
#     values => {cid => 1, name => 'Mark'}
# );
sub insert {
    my $self = shift;
    shift;
    my $table = shift;
    shift;
    my $values = shift;

    my $urow = $self->urow($table);

    my @cols = sort grep { $urow->can($_) } keys %$values;
    my @vals = map       { $values->{$_} } @cols;

    @cols || croak 'insert_into requires columns/values';

    my $ret = eval {
        $self->do(
            insert_into => sql_table( $table, @cols ),
            sql_values(@vals),
        );
    };

    if ($@) {
        croak $@;
    }

    return $ret;
}

# $db->update('purchases',
#     set   => {pid => 2},
#     where => {cid => 1},
# );
sub update {
    my $self  = shift;
    my $table = shift;
    shift;
    my $set = shift;
    shift;
    my $where = shift;

    my $urow = $self->urow($table);
    my @updates = map { $urow->$_( $set->{$_} ) }
      grep { $urow->can($_) and !exists $where->{$_} } keys %$set;

    unless (@updates) {
        $log->debug( "Nothing to update for table:", $table );
        return 0;
    }

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr =
          _expr_join( ' AND ',
            map { $urow->$_ == $where->{$_} } grep { $urow->can($_) } @keys );
    }

    return $self->do(
        update => $urow,
        set    => \@updates,
        $expr ? ( where => $expr ) : (),
    );
}

# $db->delete_from('purchases',
#    where => {cid => 1},
# );

sub delete {
    my $self = shift;
    shift;
    my $table = shift;
    shift;
    my $where = shift;

    my $urow = $self->urow($table);

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr = _expr_join( ' AND ', map { $urow->$_ == $where->{$_} } @keys );
    }

    return $self->do(
        delete_from => $urow,
        $expr ? ( where => $expr ) : (),
    );
}

# my @objs = $db->select( ['pid','label],
#     from => 'customers',
#     where => {cid => 1},
# );
sub select {
    my $self = shift;
    my $list = shift;
    shift;
    my $table = shift;
    shift;
    my $where = shift;

    my $srow = $self->srow($table);
    my @columns = map { $srow->$_ } @$list;

    @columns || croak 'select requires columns';

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr = _expr_join( ' AND ', map { $srow->$_ == $where->{$_} } @keys );
    }

    return $self->fetch(
        select => \@columns,
        from   => $srow,
        $expr ? ( where => $expr ) : (),
    ) if wantarray;

    return $self->fetch1(
        select => \@columns,
        from   => $srow,
        $expr ? ( where => $expr ) : (),
    );
}

Moo::Role->apply_role_to_package( 'SQL::DB', __PACKAGE__ );

1;
