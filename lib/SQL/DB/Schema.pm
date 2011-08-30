package SQL::DB::Schema;
use strict;
use warnings;
use Moo;
use Carp qw/confess/;
use SQL::DB::Expr;
use Sub::Install qw/install_sub/;
use Sub::Exporter -setup => {
    exports => ['get_schema'],
    groups  => {
        all     => ['get_schema'],
        default => [],
    },
};

our $VERSION = '0.97_3';
my %schema;

# Ordinals for DBI->column_info() results
use constant {
    TABLE_CAT         => 0,
    TABLE_SCHEM       => 1,
    TABLE_NAME        => 2,
    COLUMN_NAME       => 3,
    DATA_TYPE         => 4,
    TYPE_NAME         => 5,
    COLUMN_SIZE       => 6,
    BUFFER_LENGTH     => 7,
    DECIMAL_DIGITS    => 8,
    NUM_PREC_RADIX    => 9,
    NULLABLE          => 10,
    REMARKS           => 11,
    COLUMN_DEF        => 12,
    SQL_DATA_TYPE     => 13,
    SQL_DATETIME_SUB  => 14,
    CHAR_OCTET_LENGTH => 15,
    ORDINAL_POSITION  => 16,
    IS_NULLABLE       => 17,
};

# Object definition

has 'name' => (
    is       => 'rw',
    required => 1,
);

has '_package_root' => (
    is       => 'rw',
    init_arg => undef,
);

has '_tables' => (
    is       => 'ro',
    init_arg => undef,
    default  => sub { {} },
);

sub _getglob { no strict 'refs'; \*{ $_[0] } }

sub BUILD {
    my $self = shift;
    ( my $name = $self->name ) =~ tr/a-zA-Z0-9/_/cs;
    $self->name($name);
    $self->_package_root( __PACKAGE__ . '::' . $name );
    $schema{$name} = $self;
}

sub define {
    my $self = shift;
    my $data = shift;

    my $package_root = $self->_package_root;
    my $tables       = $self->_tables;

    foreach my $colref (@$data) {
        my $table = $colref->[TABLE_NAME];
        my $srow  = $package_root . '::Srow::' . $table;
        my $urow  = $package_root . '::Urow::' . $table;

        if ( !exists $tables->{$table} ) {

            eval "package $srow; use Moo; extends 'SQL::DB::Expr'";
            eval "package $urow; use Moo; extends 'SQL::DB::Expr'";

  #            @{ *{ _getglob( $srow . '::ISA' ) }{ARRAY} } = ('SQL::DB::Expr');
  #            @{ *{ _getglob( $urow . '::ISA' ) }{ARRAY} } = ('SQL::DB::Expr');

            install_sub(
                {
                    code => sub {
                        my $table_expr = shift;
                        return '*';
                    },
                    into => $urow,
                    as   => '_columns',
                }
            );
            install_sub(
                {
                    code => sub {
                        my $table_expr = shift;
                        return '*';
                    },
                    into => $srow,
                    as   => '_columns',
                }
            );
        }
        $tables->{$table}++;

        my $col  = $colref->[COLUMN_NAME];
        my $type = lc $colref->[TYPE_NAME];

        install_sub(
            {
                code => sub {
                    my $table_expr = shift;
                    SQL::DB::Expr->new(
                        _txt   => $table_expr->_alias . '.' . $col,
                        _btype => $type,
                    );
                },
                into => $srow,
                as   => $col,
            }
        );

        install_sub(
            {
                code => sub {
                    my $table_expr = shift;

                    if (@_) {
                        my $val = shift;
                        return SQL::DB::Expr->new(
                            _txt     => $col . ' = ?',
                            _btype   => $type,
                            _bvalues => [$val],
                        );
                    }

                    return SQL::DB::Expr->new(
                        _txt   => $col,
                        _btype => $type,
                    );
                },
                into => $urow,
                as   => $col,
            }
        );
    }

    return;
}

sub not_known {
    my $self   = shift;
    my $tables = $self->_tables;
    return grep { !exists $tables->{$_} } @_;
}

sub srow {
    my $self = shift;

    my @ret;
    foreach my $name (@_) {
        my $class = $self->_package_root . '::Srow::' . $name;
        my $srow = eval { $class->new( _txt => $name, _alias => $name ) };
        confess "$@\nTable not defined in schema?: $name" if $@;
        return $srow unless (wantarray);
        push( @ret, $srow );
    }
    return @ret;
}

sub urow {
    my $self = shift;

    my @ret;
    foreach my $name (@_) {
        my $class = $self->_package_root . '::Urow::' . $name;
        my $urow = eval { $class->new( _txt => $name ) };
        confess "$@\nTable not defined in schema?: $name" if $@;
        return $urow unless (wantarray);
        push( @ret, $urow );
    }
    return @ret;
}

# Class functions

sub get_schema {
    my $name = shift;
    return $schema{$name} if ( exists $schema{$name} );
    return;
}

1;

# vim: set tabstop=4 expandtab:
