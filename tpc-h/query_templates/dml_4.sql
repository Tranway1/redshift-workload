create table lineitem_copy_:1 as (select * from $(lineitem), $(region) where extract(day from l_shipdate) <= 5);
drop table lineitem_copy_:1;