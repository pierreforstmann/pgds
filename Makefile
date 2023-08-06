# pgds Makefile

MODULES = pgds

EXTENSION = pgds
DATA = pgds--0.0.1.sql
PGFILEDESC = "pgds - DS"

REGRESS_OPTS =  --temp-instance=/tmp/5555 --port=5555 --temp-config pgds.conf
REGRESS = test1 test2

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

pgxn:
	git archive --format zip  --output ../pgxn/pgds/pgds-0.0.1.zip main 
