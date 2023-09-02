--
-- test5.sql
--
\pset null (null)
--
create table t500(x int, y date) partition by range(y);

create table t500_2020 partition of t500
for values from ('2020-01-01') to ('2020-12-31');
--
--
select min(y) from t500;
--
create table t500_2021 partition of t500
for values from ('2021-01-01') to ('2021-12-31');
--
create table t500_2022 partition of t500
for values from ('2022-01-01') to ('2022-12-31');
--
--
select max(y) from t500;
