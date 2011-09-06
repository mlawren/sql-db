package SQL::DB::X::Simple;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/croak/;

our $VERSION = '0.97_3';

# $db->insert_into('customers',
#     values => {cid => 1, name => 'Mark'}
# );
sub insert_into {
    my $self  = shift;
    my $table = shift;
    shift;
    my $values = shift;

    my $urow = $self->urow($table);

    my @cols = sort grep { $urow->can($_) } keys %$values;
    my @vals = map       { $values->{$_} } @cols;

    @cols || croak 'insert_into requires columns/values';

    return $self->do(
        insert_into => SQL::DB::Expr->new(
            _txt => $table . '(' . join( ',', @cols ) . ')',
        ),
        SQL::DB::sql_values(@vals)
    );
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

    if ( $self->debug ) {
        require Data::Dumper;
        local $Data::Dumper::Indent   = 1;
        local $Data::Dumper::Maxdepth = 2;

        $log->debug(
            Data::Dumper::Dumper(
                {
                    table => $table,
                    set   => $set,
                    where => $where
                }
            )
        );
    }

    my $urow = $self->urow($table);
    my @updates = map { $urow->$_( $set->{$_} ) }
      grep { $urow->can($_) and !exists $where->{$_} } keys %$set;

    unless (@updates) {
        $log->debug( "Nothing to update for table:", $table );
        return;
    }

    my $expr = SQL::DB::_expr_join(
        ' AND ',
        map    { $urow->$_ == $where->{$_} }
          grep { $urow->can($_) } keys %$where
    );

    $expr || croak 'update requires a valid where clause';
    return $self->do(
        update => $urow,
        set    => \@updates,
        where  => $expr,
    );
}

# $db->delete_from('purchases',
#    where => {cid => 1},
# );

sub delete_from {
    my $self  = shift;
    my $table = shift;
    shift;
    my $where = shift;

    my $urow = $self->urow($table);
    my $expr =
      _expr_join( ' AND ', map { $urow->$_ == $where->{$_} } keys %$where );

    $expr || croak 'delete_from requires a where clause';
    return $self->do(
        delete_from => $urow,
        where       => $expr,
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

    my $srow = $self->_schema->srow($table);
    my @columns = map { $srow->$_ } @$list;

    @columns || croak 'select requires columns';

    my $expr =
      SQL::DB::_expr_join( ' AND ',
        map { $srow->$_ == $where->{$_} } keys %$where );

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
