--
-- test3.sql
--
create table t11(x int);
create table t12(x int);
create table t13(x int);
create table t14(x int);
create table t15(x int);
create table t16(x int);
--
create view v31 as select * from t11 union select * from t12;
create view v32 as select * from t13 union select * from t14;
create view v33 as select * from t15 union select * from t16;
--
create view v42 as 
 select * from v31 union
 select * from v32 union
 select * from v33;
--
insert into t1 values(1);
select * from v42;
