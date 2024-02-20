create table customer_copy_:1 as (select * from $(customer), $(region) where c_mktsegment = 'MACHINERY');
drop table customer_copy_:1;