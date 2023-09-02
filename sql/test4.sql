--
-- test4.sql
--
create table t41(x1 int);
create table t42(x2 int);
--
select * from t41 where x1 = (select max(x2) from t42);
--
--
create table t43(x3 int);
create table t44(x4 int);
--
with v as (select x3 from t43) select count(*) from v where x3 = 123;
--
--
select 
 (select min(x3) from t43) as c1, 
 (select max(x4) from t44) as c2, 
 avg(x1) as c3 from t41;


