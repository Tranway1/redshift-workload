create table orders_copy as (select * from $(orders), $(region) where extract(day from o_orderdate) <= 5);