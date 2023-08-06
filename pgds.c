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

typedef struct pgdsSharedState
{
	LWLock 		*lock;
	
} pgdsSharedState;

static pgdsSharedState *pgds = NULL;

/* Saved hook values in case of unload */
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static get_relation_stats_hook_type prev_get_relation_stats_hook= NULL;
static get_relation_info_hook_type prev_get_relation_info_hook= NULL;

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);

static 	void 	pgds_shmem_startup(void);
static 	void 	pgds_shmem_shutdown(int, Datum);
static  PlannedStmt *pgds_planner_hook (Query *, const char *, int, ParamListInfo);
static 	void	pgds_analyze_table(Oid);
static 	bool	pgds_get_relation_stats(PlannerInfo *, RangeTblEntry *, short int, VariableStatData *);
static 	void	pgds_get_relation_info(PlannerInfo *, Oid, bool, RelOptInfo *);


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

	prev_planner_hook = planner_hook;
	planner_hook = pgds_planner_hook;

	prev_get_relation_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = pgds_get_relation_stats;

	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = pgds_get_relation_info;

	elog(DEBUG5, "pgds:_PG_init():exit");
}


/*
 *  Module unload callback
 */
void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;	
	get_relation_stats_hook = prev_get_relation_stats_hook;
	get_relation_info_hook = prev_get_relation_info_hook;

}

/*
 *
 * pgds_planner_hook
 *
 */

static PlannedStmt *pgds_planner_hook (Query *parse,
                                       const char *query_string,
                                       int cursorOptions,
                                       ParamListInfo boundParams)
{
	PlannedStmt *result;

	elog(DEBUG1,"pgds: pgds_planner_hook: entry");

	result = standard_planner(parse, query_string, cursorOptions, boundParams);

	if (prev_planner_hook) 
	{
		elog(DEBUG1,"pgds: pgds_planner_hook: return");
		return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
	}

	elog(DEBUG1,"pgds: pgds_planner_hook: exit");
    return result;
}
					
/*
 *
 * pgds_analyze_table: main routine
 *
 */
static void pgds_analyze_table(Oid rel_id)
{
	StringInfoData buf_select1;
	StringInfoData buf_select2;
	StringInfoData buf_analyze;
	int ret;
	int nr;
	SPITupleTable *tuptable;
	TupleDesc tupdesc;
	char *count_val;
	char *relname;

	initStringInfo(&buf_select1);
	appendStringInfo(&buf_select1, 
			     "select relnamespace, relname, reltype from pg_class where oid = '%d'", rel_id);

	SPI_connect();
	ret = SPI_execute(buf_select1.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "cannot select from pg_class for rel_id: %d  error code: %d", rel_id, ret);
	nr = SPI_processed;
	if (nr == 0)
		elog(FATAL, "rel_id: %d not found in pg_class", rel_id);
	if (nr > 1)
		elog(FATAL, "too many rel.: %d found in pg_class for rel_id: %d" , nr, rel_id);
	/*
	 * get only relname column for single row result
	 */
	tuptable = SPI_tuptable;
	tupdesc = tuptable->tupdesc;
	relname = SPI_getvalue(tuptable->vals[0], tupdesc, 2);

	initStringInfo(&buf_select2);
	appendStringInfo(&buf_select2, 
				 " select count(*) from pg_statistic where starelid = '%d'", rel_id);
	ret = SPI_execute(buf_select2.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "cannot select from pg_statistic for rel_id: %d  error code: %d", rel_id, ret);
	/* 
	 * count(*) returns only 1 row with 1 column
	 */
	tuptable = SPI_tuptable;
	tupdesc = tuptable->tupdesc;
	count_val = SPI_getvalue(tuptable->vals[0], tupdesc, 1);
	elog(DEBUG1,"pgds: pgds_analyze_table: count_val: %s", count_val);

	if (strcmp(count_val, "0") == 0) {
		initStringInfo(&buf_analyze);
		appendStringInfo(&buf_analyze, "analyze verbose %s;", relname);
		elog(DEBUG1,"pgds: pgds_analyze_table: analyze: %s", relname);
		ret = SPI_execute(buf_analyze.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "cannot run analyze for %s: error code %d", relname, ret);
		}

	SPI_finish();


}

static 	bool pgds_get_relation_stats(PlannerInfo *root, RangeTblEntry *rte, short int attnum, VariableStatData *vardata)
{
	elog(DEBUG1,"pgds: pgds_get_relation_stats: entry: relid = %d", rte->relid);

	/*
	 * avoid call not useful
	 */
	if (IsCatalogRelationOid(rte->relid))
	{
		elog(DEBUG1,"pgds: pgds_get_relation_stats: return");
		return false;
	}

	if (prev_get_relation_stats_hook)
		return prev_get_relation_stats_hook(root, rte, attnum, vardata);

	elog(DEBUG1,"pgds: pgds_get_relation_stats: exit");
	return false;
}

static void pgds_get_relation_info(PlannerInfo *root, Oid rel_oid, bool inhparent, RelOptInfo *rel)
{

	elog(DEBUG1,"pgds: pgds_get_relation_info: entry: rel_oid= %d", rel_oid);

    /*
     * avoid infinite recursion
     */
    if (IsCatalogRelationOid(rel_oid))
	{
		elog(DEBUG1,"pgds: pgds_get_relation_info: return");
		return;
	}

	elog(DEBUG1, "pgds: pgds_get_relation_info: rel_oid = %d", rel_oid);
	pgds_analyze_table(rel_oid);

	if (prev_get_relation_info_hook)
		return prev_get_relation_info_hook(root, rel_oid, inhparent, rel);

	elog(DEBUG1,"pgds: pgds_get_relation_info: exit");
}


