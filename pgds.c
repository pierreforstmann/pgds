/*-------------------------------------------------------------------------
 *  
 * pgds.c
 * 
 * dynamic statistics implementation i.e. running ANALYZE 
 * if statistics are missing for tables used in currently running
 * SQL statement.
 * 
 * NB: ANALYZE command source code: src/backend/commands/analyze.c
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
#include "nodes/nodeFuncs.h"

PG_MODULE_MAGIC;

/* ---- Static variable definition ---- */

typedef struct pgdsSharedState
{
	LWLock 		*lock;
	
} pgdsSharedState;

static pgdsSharedState *pgds = NULL;

#define	MAX_REL	1024
static 	Oid pgds_rel_array[MAX_REL] = {};
static	int	pgds_rel_index = 0;

#define MAX_TABLE	10*MAX_REL
static 	Oid pgds_tableoid_array[MAX_TABLE] = {};
static 	char *pgds_tablename_array[MAX_TABLE] = {};
static 	Oid pgds_tableowner_array[MAX_TABLE] = {};
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
static	void	pgds_build_rel_array(Query *);
static	void	pgds_build_table_array();
static  bool    pgds_tree_walker(Query *node, void *context);
static  bool    pgds_sublink_walker(Node *node, void *context);
static  void 	pgds_add_rel_array(Oid relid);

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

static void pgds_add_rel_array(Oid relid)
{
	bool found = false;
	int i;

	/*
	 * tree walkers may find same relation several times
	 */
	for (i = 0 ; i < pgds_rel_index; i++)
	{
		if (pgds_rel_array[i] == relid) 
		{
			found = true;
			break;
		}
	}
	if (found == true)
		return;

	if (pgds_rel_index < MAX_REL )
	{
		pgds_rel_array[pgds_rel_index] = relid;
		pgds_rel_index++;
	} 
	else elog(ERROR, "pgds_add_rel_array: too many relations (%d)", MAX_REL);
}

static bool pgds_tree_walker(Query *node, void *context)
{
	/*
	 *  Note to self: full list of node tags are only in *compiled* src/include/nodes/nodes.h
	 */

	 ListCell   *l;

	// elog(INFO, "pgds_tree_walker: input= %s", nodeToString(node));

	/*
	 * from rewriteHandler.c
	 * AcquireRewriteLocks
	 */ 

	if (node == NULL)
         return false;

 	if (IsA(node, Query))
    {

         ListCell   *lc;
  
         foreach(lc, node->rtable)
         {
             RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
  
             if (rte->rtekind == RTE_RELATION)
			 {
					// elog(INFO, "pgds_tree_walker: relid=%d", rte->relid);
					pgds_add_rel_array(rte->relid);
			 }

			if (rte->rtekind == RTE_SUBQUERY)
				(void)pgds_tree_walker(rte->subquery, context);
		 }

		/* Recurse into subqueries in WITH */
    	foreach(l, node->cteList)
    	{	
       		 CommonTableExpr *cte = (CommonTableExpr *) lfirst(l);
  
       		pgds_tree_walker((Query *) cte->ctequery, context);
    	}
	
	}

	/*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (node->hasSubLinks)
       	query_tree_walker(node, pgds_sublink_walker, &context,
    		       	               QTW_IGNORE_RC_SUBQUERIES);
		// pgds_sublink_walker((Node *)node, context);

}

static bool pgds_sublink_walker(Node *node, void *context)
{

	//elog(INFO, "pgds_sublink_walker: input=%s", nodeToString(node));
	/*
	 * from rewriteHandler.c
	 * acquireLocksOnSubLinks
	 */
	if (node == NULL)
         return false;
     if (IsA(node, SubLink))
     {
         SubLink    *sub = (SubLink *) node;
  
         /* Do what we came for */
         pgds_tree_walker((Query *) sub->subselect,
                             context);
         /* Fall through to process lefthand args of SubLink */
     }
  
     /*
      * Do NOT recurse into Query nodes, because (AcquireRewriteLocks)pgds_tree_walker already
      * processed subselects of subselects for us.
      */
     return expression_tree_walker(node, pgds_sublink_walker, context);

}

/*
 * build_rel_array
 */
static void pgds_build_rel_array(Query *query)
{
	void *context = NULL;

	(void)pgds_tree_walker(query, context);
}


/*
 *   pgds_get_rel_details
 */
static void pgds_get_rel_details(Oid rel_id, char **relname, char** relkind, Oid *relowner)
{
	StringInfoData buf_select;	
	SPITupleTable *tuptable;
	TupleDesc tupdesc;
	int ret;
	int nr;
	bool isnull;

	initStringInfo(&buf_select);
	appendStringInfo(&buf_select, 
				     "select relnamespace, relname, relkind, relowner from pg_class where oid = '%d'", rel_id);
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
	 * relowner = column 4 for single row result
	*/
	tuptable = SPI_tuptable;
	tupdesc = tuptable->tupdesc;
	*relname = SPI_getvalue(tuptable->vals[0], tupdesc, 2);
	*relkind = SPI_getvalue(tuptable->vals[0], tupdesc, 3);
	*relowner = DatumGetInt32(SPI_getbinval(tuptable->vals[0], tupdesc, 4, &isnull));

}

/*
 * pgds_build_table_array
 */
static void pgds_build_table_array(Oid rel_id)
{

	StringInfoData buf_select;	
	SPITupleTable *tuptable;
    TupleDesc tupdesc;
	int j;
	int nr;
	int ret;
	char *relname;
	char *relkind;
	Oid relowner;
	int	ref_rel_id;
	char *ref_rel_kind;
	char *ref_rel_name;
	Oid ref_rel_owner;
	bool isnull;

	if (rel_id == 0)
		return;

	pgds_get_rel_details(rel_id, &relname, &relkind, &relowner);
	elog(LOG, "pgds_build_table_array: reld_id=%d relname=%s, relkind=%s relwoner=%d", rel_id, relname, relkind, relowner);

	if (strcmp(relkind, "r") == 0 || strcmp(relkind, "p") == 0)
	{
			if (pgds_table_index < MAX_TABLE)
			{
				pgds_tableoid_array[pgds_table_index] = rel_id;
				pgds_tablename_array[pgds_table_index] = relname;
				pgds_tableowner_array[pgds_table_index] = relowner;
				pgds_table_index++;
			} 
			else elog(ERROR, "pgds_build_table_array: too many tables(%d)", MAX_TABLE);
	} 
	else if (strcmp(relkind, "v") == 0)
	{
		/*
		 * search relations referenced by rel_id view
		 */	
		initStringInfo(&buf_select);
		appendStringInfo(&buf_select, 
					    "SELECT * from find_tables(%d)", rel_id);
		ret = SPI_execute(buf_select.data, false, 0);
		if (ret != SPI_OK_SELECT)
			elog(FATAL, "cannot get dependant relations for rel_id %d: error code: %d", rel_id, ret);
		nr = SPI_processed;		
		elog(LOG, "pgds_build_table_array: nr=%d", nr);
		/*
		 * column 1 is referenced rel_id
		 * column 2 is referenced rel_name
		 * column 3 is referenced rel_kind
		 * column 4 is referenced rel_owner
		*/
	 	tuptable = SPI_tuptable;
    	tupdesc = tuptable->tupdesc;
		for (j = 0; j < nr; j++)
		{
			elog(LOG, "pgds_build_table_array: j=%d", j);
			ref_rel_id = DatumGetInt32(SPI_getbinval(tuptable->vals[j],
							  tupdesc, 1, &isnull));
			ref_rel_name = SPI_getvalue(tuptable->vals[j],
							  tupdesc, 2);
			ref_rel_kind = SPI_getvalue(tuptable->vals[j],
							  tupdesc, 3);
			ref_rel_owner = DatumGetInt32(SPI_getbinval(tuptable->vals[j],
							  tupdesc, 4, &isnull));
			if (strcmp(ref_rel_kind,"r") == 0 && ref_rel_id != 0)
			{
					if (pgds_table_index < MAX_TABLE)
					{
						pgds_tableoid_array[pgds_table_index] = ref_rel_id;
						pgds_tablename_array[pgds_table_index] = ref_rel_name;
						pgds_tableowner_array[pgds_table_index] = ref_rel_owner;
						pgds_table_index++;
					} else elog(ERROR, "pgds_build_table_array: too many tables(%d)", MAX_TABLE);
			}
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

		pgds_build_rel_array(query);
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
 * pgds_analyze_table
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

	if (!superuser() && GetUserId() != pgds_tableowner_array[index])
	{
		elog (INFO, "pgds_analyze_table: current user cannot analyze %s", pgds_tablename_array[index]);
		return;
	}

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



