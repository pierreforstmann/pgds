--
-- pgsq-0.0.1.sql
--
-- Most code copyright Cybertec PostgreSQL Laurenz Albe 2019
-- from: https://www.cybertec-postgresql.com/en/tracking-view-dependencies-in-postgresql/
--
-- additional code copyright Pierre Forstmann 2023
--
-- NB: To avoid following errors with PG 12.8:
--
-- ERROR:  return type mismatch in function declared to return record
-- DETAIL:  Final statement returns regclass instead of text at column 2.
-- CONTEXT:  SQL function "find_tables" during startup
-- SQL statement "SELECT * from find_tables(16397)"
-- 
-- added conversions to text and char in top level SELECT for column 2 and colum 3:
-- refobjid::regclass::text,
-- relkind::char,
--
CREATE FUNCTION find_tables(p_oid oid)
RETURNS TABLE (relid oid, relname text, relkind char, relowner oid)
AS
$$
WITH RECURSIVE cte AS (
  SELECT v.oid::regclass AS view,
       d.refobjid,
       d.refobjid::regclass::text AS relname,
       c.relkind,
       c.relowner
  FROM pg_depend AS d      -- objects that depend on the table
   JOIN pg_rewrite AS r  -- rules depending on the table
      ON r.oid = d.objid
   JOIN pg_class AS v    -- views for the rules
      ON v.oid = r.ev_class
   JOIN pg_class as c
      ON d.refobjid = c.oid
  WHERE v.relkind = 'v'    -- only interested in views
  -- dependency must be a rule depending on a relation
   AND d.classid = 'pg_rewrite'::regclass
   AND d.refclassid = 'pg_class'::regclass
   AND d.deptype = 'n'    -- normal dependency
   AND v.oid = p_oid
   AND d.refobjid <> v.oid
 UNION
  SELECT v.oid::regclass AS view,
       d.refobjid,
       d.refobjid::regclass::text AS relname,
       c.relkind,
       c.relowner
  FROM pg_depend AS d      -- objects that depend on the table
   JOIN pg_rewrite AS r  -- rules depending on the table
      ON r.oid = d.objid
   JOIN pg_class AS v    -- views for the rules
      ON v.oid = r.ev_class
   JOIN pg_class as c
      ON d.refobjid = c.oid
   JOIN cte
    ON v.oid = cte.refobjid 
  WHERE v.relkind = 'v'    -- only interested in views
  -- dependency must be a rule depending on a relation
   AND d.classid = 'pg_rewrite'::regclass
   AND d.refclassid = 'pg_class'::regclass
   AND d.deptype = 'n'    -- normal dependency
   AND d.refobjid <> v.oid
 )
 SELECT
   refobjid,
   refobjid::regclass::text,
   relkind::char,
   relowner
 FROM cte
 WHERE relkind ='r';
 $$
LANGUAGE SQL;
