create table region_copy_:1 as (select * from $(region));
drop table region_copy_:1;