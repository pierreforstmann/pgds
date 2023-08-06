--
-- test2.sql
--
--
create table t21 (x int);
create table t22 (x int);
create view v23 as select * from t21 union select * from t22;
--
select * from v23;
insert into t21 values(1);
select * from v23;
insert into t22 values(2);
select * from v23;
--
select * from v23;
