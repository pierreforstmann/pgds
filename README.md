# pgds

PostgreSQL extension to gather dynamic statistics on tables

## Compiling

This module can be built using the standard PGXS infrastructure: for this to work, the `pg_config` program must be available in your $PATH:

`git clone https://github.com/pierreforstmann/pgds.git`
<br>
`cd pgds`
<br>
`make`
<br>
`make install`
<br>

## PostgreSQL setup

Extension must be loaded:

At server level with shared_preload_libraries parameter:
<br>
`shared_preload_libraries = 'pgds'`
<br>

And following SQL statement should be run:
<br>
`create extension pgds;`
<br>

pgds has been successfully tested with PostgreSQL 12, 13, 14, 15, 16 and 17.

## Usage

pgds has no GUC parameter.

What it does:

- it parses tables used in SQL statements
- it checks whether statistics exists for these tables 
- if not, runs ANALYZE statement if the user executing the query is the owner of the table.

