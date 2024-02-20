create table orders_copy_:1 as (select * from $(orders), $(region) where extract(day from o_orderdate) <= 5);
drop table orders_copy_:1;