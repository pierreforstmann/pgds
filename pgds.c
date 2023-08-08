/*-------------------------------------------------------------------------
 *  
 * pgds.c
 * 
 * Trying to implement dynamic statistics i.e. running ANALYZE 
 * if statistics are missing for tables used in currently running
 * SQL statements.
 * 
 * ANALYZE source code: src/backend/commands/analyze.c .
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *          
 * Copyright (c) 2023 Pierre Forstmann.
 *            
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "access/xact.h"
#include "parser/parse_node.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#if PG_VERSION_NUM <= 90600
#include "storage/lwlock.h"
#endif
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= 90600
#include "nodes/extensible.h"
#endif
#if PG_VERSION_NUM > 120000
#include "nodes/pathnodes.h"
#endif
#include "nodes/plannodes.h"
#include "utils/datum.h"
#include "utils/builtins.h"
#include "unistd.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "catalog/catalog.h"
#include "utils/selfuncs.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"

PG_MODULE_MAGIC;

/* ---- Static variable definition ---- */

typedef struct pgdsSharedState
{
	LWLock 		*lock;
	
} pgdsSharedState;

static pgdsSharedState *pgds = NULL;

#define	MAX_RELS		1024
static 	Oid pgds_rel_array[MAX_RELS] = {};
static	int	pgds_rel_index = 0;

#define MAX_TABLES		1024
static 	Oid pgds_tableoid_array[MAX_TABLES] = {};
static 	char *pgds_tablename_array[MAX_TABLES] = {};
static	int pgds_table_index = 0;

/* Saved hook values in case of unload */
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

static int pgds_avoid_recursion = 0;

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);

static 	void 	pgds_shmem_startup(void);
static 	void 	pgds_shmem_shutdown(int, Datum);

#if PG_VERSION_NUM < 140000
static  void    pgds_analyze(ParseState *pstate, Query *query);
#else
static  void    pgds_analyze(ParseState *pstate, Query *query, JumbleState *jstate);
#endif

static 	void	pgds_analyze_table(int);
static	void	pgds_build_rel_array(ParseState *);
static	void	pgds_build_table_array();


/*
 *  Estimate shared memory space needed.
 * 
 */
static Size
pgds_memsize(void)
{
	Size		size;

	size = 1024;

	return size;
}

/*
 * shmen_request_hook
 *
 */
static void
pgds_shmem_request(void)
{
	/*
 	 * Request additional shared resources.  (These are no-ops if we're not in
 	 * the postmaster process.)  We'll allocate or attach to the shared
 	 * resources in pgls_shmem_startup().
	 */

#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	RequestAddinShmemSpace(pgds_memsize());
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("pgds", 1);
#endif

}


/*
 *  shmem_startup hook: allocate or attach to shared memory.
 *  
 */
static void
pgds_shmem_startup(void)
{
	bool		found;

	elog(DEBUG5, "pgds: pgds_shmem_startup: entry");

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	
	/*
 	 * Create or attach to the shared memory state
 	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgds = ShmemInitStruct("pgds",
				pgds_memsize(),
			        &found);

	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM <= 90600
		RequestAddinLWLocks(1);
		pgds->lock = LWLockAssign();
#else
		pgds->lock = &(GetNamedLWLockTranche("pgds"))->lock;
#endif

	}

	LWLockRelease(AddinShmemInitLock);


	/*
 	 *  If we're in the postmaster (or a standalone backend...), set up a shmem
 	 *  exit hook (no current need ???) 
 	 */ 
        if (!IsUnderPostmaster)
		on_shmem_exit(pgds_shmem_shutdown, (Datum) 0);


	/*
    	 * Done if some other process already completed our initialization.
    	 */
	elog(DEBUG5, "pgds: pgds_shmem_startup: exit");
	if (found)
		return;


}

/*
 *  
 *     shmem_shutdown hook
 *       
 *     Note: we don't bother with acquiring lock, because there should be no
 *     other processes running when this is called.
 */
static void
pgds_shmem_shutdown(int code, Datum arg)
{
	elog(DEBUG5, "pgds: pgds_shmem_shutdown: entry");

	/* Don't do anything during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!pgds)
		return;
	
	/* currently: no action */

	elog(DEBUG5, "pgds: pgds_shmem_shutdown: exit");
}


/*
 * Module load callback
 */
void
_PG_init(void)
{
	elog(DEBUG5, "pgds:_PG_init():entry");

	if (!process_shared_preload_libraries_in_progress)
		return;

	elog(LOG, "pgds:_PG_init(): pgds is enabled ");

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pgds_shmem_request;
#else
	pgqr_shmem_request();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgds_shmem_startup;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgds_analyze;

	elog(DEBUG5, "pgds:_PG_init():exit");
}


/*
 *  Module unload callback
 */
void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;	
	post_parse_analyze_hook = prev_post_parse_analyze_hook;

}

/*
 * build_rel_array
 */
static void pgds_build_rel_array(ParseState *pstate)
{
	ListCell *cell;
	Oid rel_id;

	foreach(cell, pstate->p_rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(cell);
		rel_id = rte->relid;
		
		pgds_rel_array[pgds_rel_index] = rel_id;
		pgds_rel_index++;
	}
}


/*
 *   pgds_get_rel_details
 */
static void pgds_get_rel_details(Oid rel_id, char **relname, char** relkind)
{
	StringInfoData buf_select;	
	SPITupleTable *tuptable;
	TupleDesc tupdesc;
	int ret;
	int nr;

	initStringInfo(&buf_select);
	appendStringInfo(&buf_select, 
				     "select relnamespace, relname, relkind from pg_class where oid = '%d'", rel_id);
	ret = SPI_execute(buf_select.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "cannot select from pg_class for rel_id: %d  error code: %d", rel_id, ret);
	nr = SPI_processed;
	if (nr == 0)
		elog(FATAL, "rel_id: %d not found in pg_class", rel_id);
	if (nr > 1)
		elog(FATAL, "too many rel.: %d found in pg_class for rel_id: %d" , nr, rel_id);
	/*
	 * relname = column 2 for single row result
	 * relkind = column 3 for single row result
	*/
	tuptable = SPI_tuptable;
	tupdesc = tuptable->tupdesc;
	*relname = SPI_getvalue(tuptable->vals[0], tupdesc, 2);
	*relkind = SPI_getvalue(tuptable->vals[0], tupdesc, 3);

}

/*
 * pgds_build_table_array
 */
static void pgds_build_table_array(Oid rel_id)
{

	StringInfoData buf_select;	
	int j;
	int nr;
	int ret;
	char *relname;
	char *relkind;
	int	ref_rel_id;
	char *ref_rel_kind;
	char *ref_rel_name;
	bool isnull;

	if (rel_id == 0)
		return;

	pgds_get_rel_details(rel_id, &relname, &relkind);
	elog(LOG, "pgds_build_table_array: reld_id=%d relname=%s, relkind=%s", rel_id, relname, relkind);

	if (strcmp(relkind, "r") == 0)
	{
			pgds_tableoid_array[pgds_table_index] = rel_id;
			pgds_tablename_array[pgds_table_index] = relname;
			pgds_table_index++;
	} 
	else if (strcmp(relkind, "v") == 0)
	{
		/*
		 * search relations referenced by rel_id view
		 */	
		initStringInfo(&buf_select);
		appendStringInfo(&buf_select, 
					    "SELECT v.oid::regclass AS view,"
						"d.refobjid, "
						"d.refobjid::regclass, "
       					"c.relkind "
						"FROM pg_depend AS d "
   						"JOIN pg_rewrite AS r "
      					"ON r.oid = d.objid "
   						"JOIN pg_class AS v "
      					"ON v.oid = r.ev_class "
   						"JOIN pg_class as c "
      					"ON d.refobjid = c.oid "
						"WHERE v.relkind = 'v' "
  						"AND d.classid = 'pg_rewrite'::regclass "
  						"AND d.refclassid = 'pg_class'::regclass "
  						"AND d.deptype = 'n' "
  						"AND v.oid = %d "
						"AND d.refobjid <> v.oid;", rel_id);
		ret = SPI_execute(buf_select.data, false, 0);
		if (ret != SPI_OK_SELECT)
			elog(FATAL, "cannot get dependant relations for rel_id %d: error code: %d", rel_id, ret);
		nr = SPI_processed;		
		/*
		 * column 2 is referenced rel_id
		 * column 3 is referenced rel_name
		 * column 4 is referenced rel_kind
		*/
		for (j = 0; j < nr; j++)
		{
			ref_rel_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[j],
							  SPI_tuptable->tupdesc, 2, &isnull));
			ref_rel_name = SPI_getvalue(SPI_tuptable->vals[j],
							  SPI_tuptable->tupdesc, 3);
			ref_rel_kind = SPI_getvalue(SPI_tuptable->vals[j],
							  SPI_tuptable->tupdesc, 4);
			if (strcmp(ref_rel_kind,"r") == 0 && ref_rel_id != 0)
			{
					pgds_tableoid_array[pgds_table_index] = ref_rel_id;
					pgds_tablename_array[pgds_table_index] = ref_rel_name;
					pgds_table_index++;
			}
			else	pgds_build_table_array(ref_rel_id);
		}
	}
	else
	{
		elog(FATAL, "unexpected rel_type: %s for rel_id: %d", relkind, rel_id);
	}

}


/*
 *
 * pgds_analyze: main routine
 *
 */
#if PG_VERSION_NUM < 140000
static void pgds_analyze(ParseState *pstate, Query *query)
#else
static void pgds_analyze(ParseState *pstate, Query *query, JumbleState *js)
#endif
{
	int i;

	elog(DEBUG1,"pgds: pgds_analyze: entry: %s",pstate->p_sourcetext);

	/* pstate->p_sourcetext is the current query text */	
	elog(LOG,"pgds: pgds_analyze: %s",pstate->p_sourcetext);

	if (pgds_avoid_recursion == 0)
	{
		pgds_avoid_recursion = 1;
		SPI_connect();
	
		/*
		 *  1. find all tables from all relations
	 	 *  2. for all tables: check and gather statistics
	 	 */

		pgds_build_rel_array(pstate);
		for (i = 0; i < pgds_rel_index; i++)
			pgds_build_table_array(pgds_rel_array[i]);
		for (i = 0 ; i < pgds_table_index; i++)
			pgds_analyze_table(i);

		SPI_finish();
		pgds_avoid_recursion = 0;

		pgds_rel_index = 0;
		pgds_table_index = 0;
	}
	else
	{
		elog(LOG, "pgds: pgds_analyze: return");
	}

	if (prev_post_parse_analyze_hook)
	{
#if PG_VERSION_NUM < 140000
		prev_post_parse_analyze_hook(pstate, query);
#else
		prev_post_parse_analyze_hook(pstate, query, js);
#endif
	 }

	elog(LOG, "pgds: pgds_analyze: exit");
}


/*
 *
 * pgds_analyze_table: main routine
 *
 */
static void pgds_analyze_table(int index)
{
	
	StringInfoData buf_select;
	StringInfoData buf_analyze;
	SPITupleTable *tuptable;
    TupleDesc tupdesc;
    char *count_val;
	int ret;

    initStringInfo(&buf_select);
    appendStringInfo(&buf_select,
                     " select count(*) from pg_statistic where starelid = '%d'", pgds_tableoid_array[index]);
    ret = SPI_execute(buf_select.data, false, 0);
    if (ret != SPI_OK_SELECT)
         elog(FATAL, "cannot select from pg_statistic for rel_id: %d  error code: %d", pgds_tableoid_array[index], ret);
    /* 
    ** count(*) returns only 1 row with 1 column
    */
    tuptable = SPI_tuptable;
    tupdesc = tuptable->tupdesc;
    count_val = SPI_getvalue(tuptable->vals[0], tupdesc, 1);
	elog(DEBUG1,"pgds: pgds_analyze_table: oid: %d  tablename: %s count_val: %s", 
	             pgds_tableoid_array[index], pgds_tablename_array[index], count_val);

    if (strcmp(count_val, "0") == 0) 
	{
		initStringInfo(&buf_analyze);
		appendStringInfo(&buf_analyze, "analyze verbose %s;", pgds_tablename_array[index]);
		elog(DEBUG1,"pgds: pgds_analyze_table: analyze: %s", pgds_tablename_array[index]);
		ret = SPI_execute(buf_analyze.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "cannot run analyze for %s: error code %d", pgds_tablename_array[index], ret);
	}
}



