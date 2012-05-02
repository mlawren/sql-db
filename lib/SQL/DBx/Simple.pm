package SQL::DBx::Simple;
use strict;
use warnings;
use Moo::Role;
use Log::Any qw/$log/;
use Carp qw/confess/;
use SQL::DB::Expr qw/_expr_join _bval/;
use SQL::DB qw/sql_table sql_values/;

our $VERSION = '0.19_12';

# $db->insert_into('customers',
#     values => {cid => 1, name => 'Mark'}
# );
sub insert {
    my $self       = shift;
    my $str_into   = shift;
    my $table      = shift;
    my $str_values = shift;
    my $values     = shift;

    unless ($str_into eq 'into'
        and $str_values eq 'values'
        and ( ref $values eq 'HASH' || eval { $values->isa('HASH') } ) )
    {
        confess 'usage: insert(into => $table, values => $hashref)';
    }

    my $urow = $self->urow($table);

    my @cols = sort grep { $urow->can($_) } keys %$values;
    my @vals = map { _bval( $values->{$_}, $urow->$_->_type ) } @cols;

    @cols || confess 'insert_into requires columns/values';

    my $ret = eval {
        $self->do(
            insert_into => sql_table( $table, @cols ),
            sql_values(@vals),
        );
    };

    if ($@) {
        confess $@;
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

    @columns || confess 'select requires columns';

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
