create table $(part)
(
    p_partkey     integer        not null distkey
        primary key,
    p_name        varchar(55)    not null,
    p_mfgr        char(25)       not null,
    p_brand       char(10)       not null,
    p_type        varchar(25)    not null,
    p_size        integer        not null encode az64,
    p_container   char(10)       not null,
    p_retailprice numeric(15, 2) not null encode az64,
    p_comment     varchar(23)    not null
)
    diststyle key
    sortkey (p_partkey);

create table $(region)
(
    r_regionkey integer  not null
        primary key,
    r_name      char(25) not null,
    r_comment   varchar(152)
)
    diststyle all
    sortkey (r_regionkey);

create table $(partsupp)
(
    ps_partkey    integer        not null distkey,
    ps_suppkey    integer        not null encode az64,
    ps_availqty   integer        not null encode az64,
    ps_supplycost numeric(15, 2) not null encode az64,
    ps_comment    varchar(199)   not null,
    primary key (ps_partkey, ps_suppkey)
)
    diststyle key
    sortkey (ps_partkey);

create table $(nation)
(
    n_nationkey integer  not null
        primary key,
    n_name      char(25) not null,
    n_regionkey integer  not null encode az64
        constraint $(nation)_fk1
            references $(region),
    n_comment   varchar(152)
)
    diststyle all
    sortkey (n_nationkey);

create table $(supplier)
(
    s_suppkey   integer        not null distkey
        primary key,
    s_name      char(25)       not null,
    s_address   varchar(40)    not null,
    s_nationkey integer        not null encode az64
        constraint $(supplier)_fk1
            references $(nation),
    s_phone     char(15)       not null,
    s_acctbal   numeric(15, 2) not null encode az64,
    s_comment   varchar(101)   not null
)
    diststyle key
    sortkey (s_suppkey);

create table $(customer)
(
    c_custkey    integer        not null
        primary key,
    c_name       varchar(25)    not null,
    c_address    varchar(40)    not null,
    c_nationkey  integer        not null encode az64
        constraint $(customer)_fk1
            references $(nation),
    c_phone      char(15)       not null,
    c_acctbal    numeric(15, 2) not null encode az64,
    c_mktsegment char(10)       not null,
    c_comment    varchar(117)   not null
)
    sortkey (c_custkey);

create table $(orders)
(
    o_orderkey      bigint         not null encode az64 distkey
        primary key,
    o_custkey       bigint         not null encode az64
        constraint $(orders)_fk1
            references $(customer),
    o_orderstatus   char           not null,
    o_totalprice    numeric(15, 2) not null encode az64,
    o_orderdate     date           not null,
    o_orderpriority char(15)       not null,
    o_clerk         char(15)       not null,
    o_shippriority  integer        not null encode az64,
    o_comment       varchar(79)    not null
)
    diststyle key
    sortkey (o_orderdate);

create table $(lineitem)
(
    l_orderkey      bigint         not null encode az64 distkey
        constraint $(lineitem)_fk1
            references $(orders),
    l_partkey       bigint         not null encode az64,
    l_suppkey       integer        not null encode az64,
    l_linenumber    integer        not null encode az64,
    l_quantity      numeric(15, 2) not null encode az64,
    l_extendedprice numeric(15, 2) not null encode az64,
    l_discount      numeric(15, 2) not null encode az64,
    l_tax           numeric(15, 2) not null encode az64,
    l_returnflag    char           not null,
    l_linestatus    char           not null,
    l_shipdate      date           not null,
    l_commitdate    date           not null encode az64,
    l_receiptdate   date           not null encode az64,
    l_shipinstruct  char(25)       not null,
    l_shipmode      char(10)       not null,
    l_comment       varchar(44)    not null,
    primary key (l_orderkey, l_linenumber),
    constraint $(lineitem)_fk2
        foreign key (l_partkey, l_suppkey) references $(partsupp)
)
    diststyle key
    sortkey (l_shipdate);
