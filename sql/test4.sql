--
-- test1.sql
--
create table t41(x1 int);
create table t42(x2 int);
--
select * from t41 where x1 = (select max(x2) from t42);
