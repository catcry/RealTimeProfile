
-- Copyright (c) 2010 Xtract Oy. All rights reserved.  
-- This software is the proprietary information of Xtract Oy. 
-- Use is subject to license terms. --

--------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.add_sequence_partitions(in_sequence_name text, in_new_id int)
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Adds a new partition to all tables that are partitioned over a column taken from 
 * a sequence (in_sequence_name). The new partition is created for value in_new_id.
 * The partitioned tables are configured in table core.partition_sequence_tables. Adds also
 * compression to the tables if configured.
 *
 * VERSION
 * 28.02.2013 MOj
 */
DECLARE
  table_prop record;
BEGIN
  FOR table_prop IN (
    SELECT table_name, compresslevel, cleanup
    FROM core.partition_sequence_tables
    WHERE sequence_name = in_sequence_name
  )

  LOOP
    INSERT INTO core.partition_sequence_create_times (table_name, sequence_name, sequence_id, time_created, cleanup)
    VALUES (table_prop.table_name, in_sequence_name, in_new_id, now(), table_prop.cleanup);

    EXECUTE 'ALTER TABLE ' || table_prop.table_name || ' '
         || 'ADD PARTITION "' || in_new_id || '" '
         || 'START (' || in_new_id || ') END (' || (in_new_id + 1) || ') '
         || COALESCE('WITH (appendonly=true, compresslevel=' || table_prop.compresslevel || ')', '');
  END LOOP;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.add_sequence_partitions(in_sequence_name text, in_new_id int) OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.remove_sequence_partitions(in_sequence_name text, in_sequence_id int)
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Removes partitions for given sequence and related id from all configured tables.
 * Only for manual use, does not obey cleanup = FALSE conditions.
 *
 * VERSION
 * 28.02.2013 MOj
 */
DECLARE
  table_prop record;
BEGIN
  --loop over all tables for this sequence
  FOR table_prop IN (
    SELECT table_name 
    FROM core.partition_sequence_create_times 
    WHERE sequence_name = in_sequence_name
    AND sequence_id = in_sequence_id
  )
  LOOP
    EXECUTE 'ALTER TABLE ' || table_prop.table_name || ' '
         || 'DROP PARTITION IF EXISTS FOR (' || in_sequence_id || ')';
  END LOOP;

  --delete from status table
  DELETE FROM core.partition_sequence_create_times 
  WHERE sequence_name = in_sequence_name
  AND sequence_id = in_sequence_id;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.remove_sequence_partitions(in_sequence_name text, in_sequence_id int) OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.cleanup_sequence_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Removes old partitions from all tables that are partitioned over a column taken from 
 * a sequence. The retention times per table are stored in table core.partition_sequence_tables. 
 *
 * Note: only cleanup condition in table core.partition_sequence_create_times is obeyed, not the
 * one in the core.partition_sequence_tables. 
 *
 * VERSION
 * 28.02.2013 MOj
 */
DECLARE
  partition_prop record;
BEGIN
  --find old partitions to delete
  FOR partition_prop IN (
    SELECT a.table_name, a.sequence_id
    FROM core.partition_sequence_create_times a
    JOIN core.partition_sequence_tables b
    ON a.table_name = b.table_name
    AND a.sequence_name = b.sequence_name
    WHERE a.time_created::date <= current_date - b.retention_dates
    AND a.cleanup IS TRUE
  )
  LOOP
    DELETE FROM core.partition_sequence_create_times
    WHERE table_name = partition_prop.table_name
    AND sequence_id = partition_prop.sequence_id;

    EXECUTE 'ALTER TABLE ' || partition_prop.table_name || ' '
         || 'DROP PARTITION IF EXISTS FOR (' || partition_prop.sequence_id || ')';
  END LOOP;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.cleanup_sequence_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.nextval_with_partitions(in_sequence_name text)
  RETURNS INT AS
$BODY$
/* SUMMARY
 * Returns a next value from a sequence and takes care of related 
 * database maintenance issues like creating new partitions.
 * If no maintenance is needed for this sequence, equals to a function call
 * nextval(in_sequence_name). Inside SL, this function should be always used.
 *
 * Partitionings are configured to tables:
 * - core.partition_sequence_tables
 *
 * VERSION
 * 28.02.2013 MOj
 */
DECLARE
  new_id int := nextval(in_sequence_name);
BEGIN
  --PERFORM core.cleanup_sequence_partitions();
  PERFORM core.add_sequence_partitions(in_sequence_name, new_id);
  RETURN new_id;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.nextval_with_partitions(in_sequence_name text) OWNER TO xsl;


-------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION core.analyze(in_table_name text, in_sequence_id int)
  RETURNS text AS
$BODY$
/* SUMMARY
 * Analyzes the partition with sequence ID in_sequence_id of table in_table_name.
 * If the table is not partitioned or partition for the given sequence ID does not exist, 
 * the whole table is analyzed. 
 *
 * VERSION
 * 18.09.2013 JVi: Imported function created for x85 by HMa, added output to function
 * 17.09.2013 HMa
 */
DECLARE

  partition_name text;
  partition_exists text;
  analyze_query text;

BEGIN

  partition_name := in_table_name || '_1_prt_' || in_sequence_id; 

  partition_exists := 
    table_name
  FROM (
    SELECT
      sc.nspname || '.' || tbl.relname AS table_name
    FROM pg_class AS tbl -- table name
    INNER JOIN pg_namespace AS sc -- schema name
    ON sc.oid = tbl.relnamespace
  ) a
  WHERE table_name = partition_name;

  IF partition_exists IS NOT NULL THEN 

    analyze_query := 'ANALYZE ' || partition_name;
    EXECUTE analyze_query;
    RETURN 'Analyzed partition ' || partition_name || ' of table ' || in_table_name;

  ELSE

    analyze_query := 'ANALYZE ' || in_table_name;
    EXECUTE analyze_query;
    RETURN 'Analyzed table ' || in_table_name;

  END IF;

    RETURN 'Analyze not performed';

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.analyze(in_table_name text, in_sequence_id int) OWNER TO xsl;


-------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.concatenate_yyyyww(date1 date, date2 date, sep text)
RETURNS text AS 
$BODY$
/* SUMMARY
 * This function concatenates "year-week" strings such that the first
 * "year-week" is greater than or equal to date1 and the last "year-week" is
 * less than date2. 
 *
 * INPUTS
 * date1    : date : Inclusive lower bound for the first "year-week" string
 * date2    : date : Exclusuve upper bound for the last "year-week" string
 * sep      : text : Separator
 *
 * OUTPUT
 * weeklist : text : Concatenated weeklist
 *
 * VERSION
 * 18.12.2012 MOj - ICIF-92 fixed
 * 08.05.2009 TSi - Bug 179 fixed
 * 30.03.2009 TSi
 */
BEGIN
  -- Concatenate all weeks between date1 and date2 as a long string
  RETURN coalesce(array_to_string(array(
    SELECT core.date_to_yyyyww(date1 + 7 * (wi-1))
    FROM generate_series(1, ( date_trunc('week', date2)::date - date_trunc('week', date1)::date ) / 7 ) wi
    ORDER BY 1
  ), sep), '');
END;
$BODY$
  LANGUAGE 'plpgsql' STABLE;
ALTER FUNCTION core.concatenate_yyyyww(date, date, text) OWNER TO xsl;


--------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION core.cleanup_table(in_table text, in_column text, in_num_retained_values integer)
  RETURNS SETOF text AS
$BODY$

 /* SUMMARY
  * This is a clean-up function that removes some rows from a given table. The
  * rows that have the highest values of a given column are retained and the
  * rest are removed.  
  *
  * INPUTS
  * in_table  : Name of table from which some rows are removed
  * in_column : Name of column by which the rows are ranked
  * in_num_retained_values : Number of distinct values of in_column that are
  *                          retained. The values are sorted in descending order
  *                          and the top is retained.
  * 
  * VERSION
  * 2011-11-22 LBe bug 791
  * 2011-03-16 TSi 
  */

DECLARE

  table_relid integer := (
    SELECT c.oid
    FROM pg_catalog.pg_class AS c
    LEFT JOIN pg_catalog.pg_namespace AS n 
    ON n.oid = c.relnamespace
    WHERE n.nspname||'.'||c.relname = (CASE WHEN strpos(in_table, '.') < 1 THEN 'public.' ELSE '' END)||in_table
  );

  table_compresslevel text := (
    SELECT 
      CASE 
        WHEN a.compresslevel > 0 
        THEN 'WITH (appendonly=true, compresslevel='||a.compresslevel||')'
        ELSE ''
      END
    FROM (SELECT table_relid AS relid) AS b
    LEFT JOIN pg_appendonly AS a 
    USING (relid)
  );

  table_columns text[] := (
    SELECT array(
      SELECT '"'||a.attname||'"'
      FROM pg_catalog.pg_attribute AS a
      WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = table_relid
      ORDER BY a.attnum
    )
  );

  table_columns_types text := (
    SELECT array_to_string(
      array(
        SELECT '"'||a.attname||'" '||pg_catalog.format_type(a.atttypid, a.atttypmod)
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = table_relid
        ORDER BY a.attnum
      ), ', '
    )
  );

  cleanup_table_name text := 
    in_table||'_cleanup'||ceiling(1e6*random());

  cleanup_table_create_query text := 
    'CREATE TABLE '||cleanup_table_name||' '|| 
    '('||table_columns_types||') '||
    table_compresslevel||' '||
    'DISTRIBUTED RANDOMLY;';

  sub_query1 text := 
    'SELECT bb."'||in_column||'", count(*) AS "count" '||
    'FROM '||in_table||' AS bb '||
    'GROUP BY bb."'||in_column||'" '||
    'ORDER BY bb."'||in_column||'" DESC '||
    'LIMIT '||in_num_retained_values;

  insert_select_query1 text := 
    'INSERT INTO '||cleanup_table_name||' '||
    '('||array_to_string(table_columns, ', ')||') '||
    'SELECT a.'||array_to_string(table_columns, ', a.')||' '||
    'FROM '||in_table||' AS a '||
    'INNER JOIN ('||sub_query1||') AS b '||
    'ON a."'||in_column||'" = '||'b."'||in_column||'"';

  insert_select_query2 text := 
    'INSERT INTO '||in_table||' '||
    '('||array_to_string(table_columns, ', ')||') '||
    'SELECT '||array_to_string(table_columns, ', ')||' '||
    'FROM '||cleanup_table_name;

  row_count1 bigint := 0;

  row_count2 bigint := 0;

BEGIN

  EXECUTE 'DROP TABLE IF EXISTS '||cleanup_table_name;

  EXECUTE cleanup_table_create_query;

  EXECUTE insert_select_query1;

  EXECUTE 'SELECT coalesce(sum(b.count), 0) FROM ('||sub_query1||') AS b' INTO row_count1;

  EXECUTE 'SELECT count(*) FROM '||cleanup_table_name INTO row_count2;

  IF row_count1 = row_count2 AND row_count1 > 0 THEN

    EXECUTE 'TRUNCATE '||in_table;

    EXECUTE insert_select_query2;  

    EXECUTE 'ANALYZE '||in_table;

    EXECUTE 'TRUNCATE '||cleanup_table_name;    

    EXECUTE 'DROP TABLE '||cleanup_table_name;
    
    RETURN NEXT 'OK';
    
  ELSEIF row_count1 = 0 THEN

    RETURN NEXT
     'ERROR: Table '||in_table||' is left untouched because '||
     'it does not have any rows to be retained. Use TRUNCATE if you really want remove all rows.';

  ELSE
  
    RETURN NEXT
     'ERROR: Table '||in_table|| ' is left untouched because '||
     'the number of rows to be retained does not match with row-count of '||cleanup_table_name||' table.'; 

  END IF;

  RETURN NEXT cleanup_table_create_query;

  RETURN NEXT insert_select_query1;

  RETURN NEXT insert_select_query2;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.cleanup_table(text, text, integer) OWNER TO xsl;

--------------------------------------------------------------------------------------


-- This core.array_sort is needed in the stack to matrix batch implementation below
-- Used also elsewhere, don't remove! 
CREATE OR REPLACE FUNCTION core.array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(
    SELECT $1[s.i] AS "foo"
    FROM
        generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)

    ORDER BY foo
);
$$;
ALTER FUNCTION core.array_sort(ANYARRAY) OWNER TO xsl;


--------------------------------------------------------------------------------------


-------------------------Target list creation---------------------------------


------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.combine_target_lists(in_mod_job_id integer)
  RETURNS integer AS
$BODY$

/* SUMMARY
 * This function combines the target lists of different use cases of the given 
 * module job. 
 * Reads from tmp.module_targets_tmp (in stack format) and 
 * writes to work.module_targets. 
 *
 * VERSION
 * 30.05.2013 KL  Changed some queries to execute to get function to work when creating apply quality charts
 * 31.01.2013 HMa 
 */

DECLARE
  use_cases text[];
  use_case text;
  uc_model_id integer;
  n_rows integer;
  sql_string text;
  colname_string text := '';
  max_string text := '';
  case_string text := '';
  query text;
  target_check boolean;
  return_value integer;

BEGIN

query='SELECT (SELECT mt.alias_id FROM work.module_targets AS mt WHERE mt.mod_job_id ='|| in_mod_job_id||' LIMIT 1 ) IS NOT NULL';

EXECUTE query INTO target_check;

  IF target_check THEN
    RAISE EXCEPTION 'Target list has already been calculated for module_job_id %', in_mod_job_id;
  END IF;

 query = 'SELECT 
      use_case_name 
    FROM tmp.module_targets_tmp
    WHERE mod_job_id = '||in_mod_job_id||'
    GROUP BY use_case_name;';
  
  FOR use_case IN 
   EXECUTE query
  
  LOOP
    
    colname_string = colname_string || 'target_'||use_case||', '|| 
                                       'audience_'||use_case||', ';

    max_string = max_string || 'max(target_'||use_case||') AS target_'||use_case||', '||
                               'max(audience_'||use_case||') AS audience_'||use_case||', ';

    case_string = case_string || 'CASE WHEN mtt.use_case_name = '''||use_case||''' THEN target ELSE NULL END AS target_'||use_case||', ' ||
                                 'CASE WHEN mtt.use_case_name = '''||use_case||''' THEN 1 ELSE NULL END AS audience_'||use_case||', ';
                                 
  END LOOP;
  
  -- Trim strings:
  colname_string := trim(TRAILING ', ' FROM colname_string);
  max_string     := trim(TRAILING ', ' FROM max_string);
  case_string    := trim(TRAILING ', ' FROM case_string);

  sql_string :=
 'INSERT INTO work.module_targets (
    mod_job_id, 
    alias_id, ' ||
    colname_string || '
  ) 
  SELECT '||
    in_mod_job_id || ' AS mod_job_id, 
    alias_id, ' ||
    max_string || '
  FROM
  (
    SELECT
      mtt.alias_id, ' ||
      case_string || '
    FROM tmp.module_targets_tmp AS mtt
    WHERE mtt.mod_job_id = ' || in_mod_job_id ||'
  ) a
  GROUP BY alias_id;';
  
  EXECUTE sql_string;
  

  query= 'SELECT count(mt.*) FROM work.module_targets AS mt WHERE mt.mod_job_id =' ||in_mod_job_id;

  EXECUTE query into return_value;

  RETURN return_value;


END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.combine_target_lists(integer) OWNER TO xsl;



------------------------------------------------------------------------------------------------------------
-- Function: work.create_target_list_churn_inactivity(integer)

-- DROP FUNCTION work.create_target_list_churn_inactivity(integer);

CREATE OR REPLACE FUNCTION work.create_target_list_churn_inactivity(in_mod_job_id integer)
  RETURNS integer AS
$BODY$

/* SUMMARY
 * This function determines the set of aliases that will be processed during a
 * certain module job in the churn_inactivity use case. If possible, the value of target 
 * variable is also calculated. The function writes to tmp.module_targets_tmp. After
 * calling the use case-specific create_target_list functions, calling work.create_target_list
 * combines the target lists into table work.module_targets.
 *
 * VERSION
 * 07.08.2013 HMa Consider only outgoing traffic as activity
 * 30.07.2013 KL Fixed to work right with new blacklists.
 * 28.06.2013 AV Check target count, positive/negative target percentages
 * 30.05.2013 KL  Changed some queries to execute to get function to work when creating apply quality charts
  * 17.05.2013 KL  Removed alias_id:s in aliases.blacklist from targets
 * 11.01.2013 HMa
 */

DECLARE

  mjp record;  
  query text;
  target_check boolean; 
  result record;

BEGIN

  SELECT DISTINCT
    in_mod_job_id AS mod_job_id,
    max(CASE WHEN d.key        IN ('t2', 'uc_churn_inactivity_t2') THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS t2,
    max(CASE WHEN d.key        = 'uc_churn_inactivity_include_postpaid' THEN d.value::smallint ELSE NULL END) AS uc_churn_inactivity_include_postpaid,
    max(CASE WHEN d.key        = 'uc_churn_inactivity_t4' THEN to_date(d.value, 'YYYY-MM-DD')  ELSE NULL END) AS uc_churn_inactivity_t4,
    max(CASE WHEN d.key        = 'uc_churn_inactivity_t5' THEN to_date(d.value, 'YYYY-MM-DD')  ELSE NULL END) AS uc_churn_inactivity_t5,
    max(CASE WHEN d.key        = 'uc_churn_inactivity_t6' THEN to_date(d.value, 'YYYY-MM-DD')  ELSE NULL END) AS uc_churn_inactivity_t6,
    max(CASE WHEN d.key        = 'uc_churn_inactivity_t7' THEN to_date(d.value, 'YYYY-MM-DD')  ELSE NULL END) AS uc_churn_inactivity_t7,
    max(CASE WHEN lower(d.key) IN ('tcrm', 'uc_churn_inactivity_tcrm') THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS tcrm,
    max(CASE WHEN d.key        = 'run_type' THEN d.value ELSE NULL END) AS run_type
  INTO mjp
  FROM work.module_job_parameters AS d
  WHERE d.mod_job_id = in_mod_job_id;
  
query = 'SELECT (SELECT mt.alias_id FROM tmp.module_targets_tmp AS mt WHERE mt.mod_job_id = '||in_mod_job_id||' AND use_case_name = ''churn_inactivity'' LIMIT 1 ) IS NOT NULL';

EXECUTE query into target_check;

  
  -- Check if the tmp module_target table already contains data for this use case:
  IF target_check THEN

    RAISE EXCEPTION 'Target list has already been calculated for churn_inactivity use_case, mod_job_id %', in_mod_job_id;
    
  END IF;

  INSERT INTO tmp.module_targets_tmp ( mod_job_id, alias_id, use_case_name, target )
  SELECT 
    in_mod_job_id AS mod_job_id,
    a.alias_id,
    'churn_inactivity' AS use_case_name,
    b.churn_status AS target
  FROM data.in_crm AS a
  INNER JOIN (
    SELECT
      d.alias_id,
      max(CASE WHEN d.monday < mjp.t2 THEN 1 ELSE 0 END) AS source_period_end_activity,
      CASE 
        WHEN max(d.monday) >= mjp.uc_churn_inactivity_t6 THEN -1 -- Churner if no activity during t6 - t7
        WHEN max(d.monday) >= mjp.uc_churn_inactivity_t4 AND max(d.monday) < mjp.uc_churn_inactivity_t5 THEN 1 -- Campaign period is t4 - t5 when churner has to be active
        ELSE NULL -- Churned but wasn't active during the campaign period
      END AS churn_status    
    FROM (
     SELECT alias_id, monday FROM data.call_types_weekly WHERE direction = 'm'
     UNION SELECT charged_id AS alias_id, date_trunc('week', timestamp)::date AS monday FROM data.topup
    ) d
    LEFT JOIN (
      SELECT alias_id 
      FROM aliases.blacklist 
      WHERE validity = 
        (SELECT max(validity)
         FROM aliases.blacklist
         WHERE validity <= mjp.t2 
         ) 
      ) bl
    ON d.alias_id=bl.alias_id 
    WHERE 
      d.monday >= mjp.t2 - 7 
      AND d.monday < mjp.uc_churn_inactivity_t7
      AND bl.alias_id IS NULL
    GROUP BY d.alias_id
  ) AS b
  ON a.alias_id = b.alias_id
  WHERE a.date_inserted = mjp.tcrm
  AND (a.payment_type = 'prepaid' OR (CASE WHEN mjp.uc_churn_inactivity_include_postpaid = 1 THEN a.payment_type = 'postpaid' END))
  AND b.source_period_end_activity = 1; -- Activity during the end of source period is required

  SELECT count(mt.*) as total, SUM(CASE WHEN mt.target = 1 THEN 1 ELSE 0 END) AS pos, SUM(CASE WHEN mt.target = -1 THEN 1 ELSE 0 END) AS neg
  INTO result
  FROM tmp.module_targets_tmp AS mt 
  WHERE mt.mod_job_id = in_mod_job_id 
  AND use_case_name = 'churn_inactivity';

  IF (result.total = 0) THEN
     RAISE EXCEPTION 'Please check data for churn inactivity use case.';
  END IF;
  
  IF (mjp.run_type ~ 'Fit') THEN
    IF ((result.pos::double precision/result.total::double precision) < 0.001) THEN -- 0.1% for positive
      RAISE EXCEPTION 'Please check data for churn inactivity use case. There is not enough positive targets.';
    END IF;
    IF ((result.neg::double precision/result.total::double precision) < 0.1) THEN -- 10% for negative
      RAISE EXCEPTION 'Please check data for churn inactivity use case. There is not enough negative targets.';
    END IF;
  END IF;
  
  RETURN result.total;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.create_target_list_churn_inactivity(integer)
  OWNER TO xsl;


-- Function: work.create_target_list_churn_postpaid(integer)

-- DROP FUNCTION work.create_target_list_churn_postpaid(integer);

CREATE OR REPLACE FUNCTION work.create_target_list_churn_postpaid(in_mod_job_id integer)
  RETURNS integer AS
$BODY$

/* SUMMARY
 * This function determines the set of aliases that will be processed during a
 * certain module job in the churn_postpaid use case. If possible, the value of target 
 * variable is also calculated. The function writes to tmp.module_targets_tmp. After
 * calling the use case-specific create_target_list functions, calling work.create_target_list
 * combines the target lists into table work.module_targets.
 *
 * VERSION
 * 
 * 30.07.2013 KL Fixed to work right with new blacklists.
 * 28.06.2013 AV Check target count, positive/negative target percentages
 * 30.05.2013 KL  Changed some queries to execute to get function to work when creating apply quality charts
 * 17.05.2013 KL  Removed alias_id:s in aliases.blacklist from targets
 * 11.01.2013 HMa
 */

DECLARE

  mjp record;  
  query text;
  target_check boolean; 
  result record;

BEGIN

  SELECT DISTINCT
    in_mod_job_id AS mod_job_id,
    max(CASE WHEN d.key       IN ('t2', 'uc_churn_postpaid_t2') THEN to_date(d.value, 'YYYY-MM-DD')   ELSE NULL END) AS t2,
    max(CASE WHEN d.key        = 'uc_churn_postpaid_t4' THEN to_date(d.value, 'YYYY-MM-DD')   ELSE NULL END) AS t4,
    max(CASE WHEN d.key        = 'uc_churn_postpaid_t5' THEN to_date(d.value, 'YYYY-MM-DD')   ELSE NULL END) AS t5,
    max(CASE WHEN lower(d.key) IN ('tcrm', 'uc_churn_postpaid_tcrm') THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS tcrm,
    max(CASE WHEN d.key        = 'run_type' THEN d.value ELSE NULL END) AS run_type
  INTO mjp
  FROM work.module_job_parameters AS d
  WHERE d.mod_job_id = in_mod_job_id;

  
query = 'SELECT (SELECT mt.alias_id FROM tmp.module_targets_tmp AS mt WHERE mt.mod_job_id = '||in_mod_job_id||' AND use_case_name = ''churn_postpaid'' LIMIT 1 ) IS NOT NULL';

EXECUTE query into target_check;

  
  -- Check if the tmp module_target table already contains data for this use case:
  IF target_check THEN

    RAISE EXCEPTION 'Target list has already been calculated for churn_postpaid use_case, mod_job_id %', in_mod_job_id;
    
  END IF;

  INSERT INTO tmp.module_targets_tmp ( mod_job_id, alias_id, use_case_name, target )
  SELECT DISTINCT 
    in_mod_job_id AS mod_job_id, 
    a.alias_id, 
    'churn_postpaid' AS use_case_name,
    CASE
      WHEN b.churn_notification_date IS NOT NULL AND b.churn_notification_date <= mjp.t5 AND b.churn_notification_date >= mjp.t4 THEN 1
      WHEN b.churn_notification_date IS NOT NULL AND b.churn_notification_date < mjp.t4 THEN NULL
      ELSE -1
    END AS target
  FROM data.in_crm AS a
  LEFT JOIN (
    SELECT crm.alias_id, max(crm.churn_notification_date) AS churn_notification_date
    FROM data.in_crm AS crm
    WHERE crm.date_inserted <= date_trunc('month',mjp.t5) + '1 month'::interval -- assumes that CRM data is delivered monthly and dated on the 1st of the next month
    GROUP BY crm.alias_id
  ) AS b
  ON a.alias_id = b.alias_id
  LEFT JOIN (
      SELECT alias_id 
      FROM aliases.blacklist 
      WHERE validity = 
        (SELECT max(validity)
         FROM aliases.blacklist
         WHERE validity <= mjp.t2 
         ) 
      ) bl
  ON b.alias_id=bl.alias_id 
  WHERE a.date_inserted = mjp.tcrm
  AND bl.alias_id IS NULL
  AND a.payment_type = 'postpaid'
  AND ( a.churn_notification_date IS NULL OR a.churn_notification_date >= mjp.t2 );

  SELECT count(mt.*) as total, SUM(CASE WHEN mt.target = 1 THEN 1 ELSE 0 END) AS pos, SUM(CASE WHEN mt.target = -1 THEN 1 ELSE 0 END) AS neg
  INTO result
  FROM tmp.module_targets_tmp AS mt 
  WHERE mt.mod_job_id = in_mod_job_id 
  AND use_case_name = 'churn_postpaid';

  IF (result.total = 0) THEN
     RAISE EXCEPTION 'Please check data for churn postpaid use case.';
  END IF;
  
  IF (mjp.run_type ~ 'Fit') THEN
    IF ((result.pos::double precision/result.total::double precision) < 0.001) THEN -- 0.1% for positive
      RAISE EXCEPTION 'Please check data for churn postpaid use case. There is not enough positive targets.';
    END IF;
    IF ((result.neg::double precision/result.total::double precision) < 0.1) THEN -- 10% for negative
      RAISE EXCEPTION 'Please check data for churn postpaid use case. There is not enough negative targets.';
    END IF;
  END IF;
  
  RETURN result.total;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.create_target_list_churn_postpaid(integer)
  OWNER TO xsl;


------------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION work.create_target_list_zero_day_prediction(in_mod_job_id integer)
  RETURNS integer AS
$BODY$

/* SUMMARY
 * This function determines the set of aliases that will be processed during a
 * certain module job in the zero_day_prediction use case. The function writes to 
 * tmp.module_targets_tmp. After calling the use case-specific create_target_list 
 * functions, calling work.create_target_list combines the target lists into table 
 * work.module_targets.
 *
 * Value segment is stored as the value of the target variable so that 1 = low value,
 * 2 = medium value and 3 = high value. Changing the number of value segments will
 * require some modifications to the code. When existing model is applied (i.e. value
 * segment for new subscribers is not determined from data) target is -1.
 *
 * VERSION
 * 2013-07-30 KL Fixed to work right with new blacklists.
 * 2013-05-22 KL  Removed alias_id:s in aliases.blacklist from targets
 * 2013-01-24 JVi: Cleaned and commented
 * 2013-01-18 JVi: Created
 */

DECLARE

  --Parametres

  uc_zero_day_prediction_model_id                 int;

  t2                                              date;
  t9                                              date; 
  uc_zero_day_prediction_t4                       date;
  uc_zero_day_prediction_t5                       date;
  this_uc_zero_day_prediction_segment_limits      double precision[];
  uc_zero_day_prediction_redefine_value_segments  boolean;

  run_type                                        text;
  calculate_predictors                            boolean;
  fit_model                                       boolean;
  apply_model                                     boolean;

  --Miscellaneous  
  temp_table_sql                                  text;
  mjp                                             record;  
  tmp                                             double precision;

BEGIN

  --Fetch existing parameters from module job parameters
  t9                                             := m.value::date    FROM work.module_job_parameters m 
                                                      WHERE m.mod_job_id = in_mod_job_id AND m.key = 't9';
  t2                                             := m.value::date    FROM work.module_job_parameters m 
                                                      WHERE m.mod_job_id = in_mod_job_id AND m.key = 't2';
  run_type                                       := m.value::text    FROM work.module_job_parameters m 
                                                      WHERE m.mod_job_id = in_mod_job_id AND m.key = 'run_type';
  uc_zero_day_prediction_t4                      := m.value::date    FROM work.module_job_parameters m 
                                                      WHERE m.mod_job_id = in_mod_job_id AND m.key = 'uc_zero_day_prediction_t4';
  uc_zero_day_prediction_t5                      := m.value::date    FROM work.module_job_parameters m 
                                                      WHERE m.mod_job_id = in_mod_job_id AND m.key = 'uc_zero_day_prediction_t5';
  uc_zero_day_prediction_model_id                := m.value::int     FROM work.module_job_parameters m 
                                                      WHERE m.mod_job_id = in_mod_job_id AND m.key = 'uc_zero_day_prediction_model_id';
  uc_zero_day_prediction_redefine_value_segments := CASE WHEN (SELECT m.value::text FROM work.module_job_parameters m 
                                                      WHERE m.mod_job_id = in_mod_job_id AND m.key = 'uc_zero_day_prediction_redefine_value_segments') = 'true'
                                                      THEN true ELSE false END;
                                                      
  --Evaluate which parts of the workflow are run
  calculate_predictors := run_type ~ 'Predictors';
  fit_model := run_type ~ 'Fit';
  apply_model := run_type ~ 'Apply';

  --Existing value segment limits are used if possible. If no module job with smaller (or equal) id than 
  --current job is found that has the value segments defined, they will be calculated from the
  --current data. User can always force value segment estimation from data via setting the run parameter
  --uc_zero_day_prediction_redefine_value_segments to true (workflow configuration).
  IF uc_zero_day_prediction_redefine_value_segments AND NOT (fit_model OR (calculate_predictors AND (NOT fit_model AND NOT apply_model))) THEN
    RAISE WARNING 'Unable to redefine value segment limits, make sure eight weeks of data is available (run type is Predictors or Predictors + Fit).';
    uc_zero_day_prediction_redefine_value_segments := FALSE;
  END IF;

  IF NOT uc_zero_day_prediction_redefine_value_segments THEN 
    tmp := MAX(m.mod_job_id) FROM work.module_job_parameters m 
      WHERE m.mod_job_id <= in_mod_job_id 
      AND   m.key         = 'uc_zero_day_prediction_segment_limits' 
      AND   m.value         IS NOT NULL;
    IF tmp IS NOT NULL THEN
      this_uc_zero_day_prediction_segment_limits := string_to_array(m.value,',')::double precision[] FROM work.module_job_parameters m 
                                                    WHERE m.mod_job_id = tmp AND m.key = 'uc_zero_day_prediction_segment_limits';
    ELSIF NOT (fit_model OR (calculate_predictors AND (NOT fit_model AND NOT apply_model))) THEN
      RAISE EXCEPTION 'Unable to determine value segment limits, make sure eight weeks of training data is available for zero day prediction (run type is Predictors or Predictors + Fit).';
    END IF;
  END IF;

  -- Check if the tmp module_target table already contains data for this use case:
  IF (SELECT mt.alias_id FROM tmp.module_targets_tmp AS mt 
        WHERE mt.mod_job_id = in_mod_job_id AND use_case_name = 'zero_day_prediction' LIMIT 1) IS NOT NULL THEN
    RAISE EXCEPTION 'Target list has already been calculated for zero_day_prediction use case, mod_job_id %', in_mod_job_id;
  END IF;
  
  
  --Insert use case targets into a temporary table before assigning them value segments.
  --If model fitting is required, the table also contains total topups from first 8 weeks
  --for each subscriber. These 8 weeks are counted starting from next Monday after subscriber
  --joined (i.e., if subscriber joins on Wednesday of week 1, topups are counted from weeks 2-9).
  --Note: The temporary table creation has to be done using dynamic SQL and EXECUTE due to a PostgreSQL bug
  
  IF fit_model OR (calculate_predictors AND NOT apply_model) THEN --In practice: run_type is 'Predictors' or 'Predictors + Fit + Apply'
    --If model is fitted, actual topups from eight weeks are needed
    temp_table_sql := '
      DROP TABLE IF EXISTS temp_uc_zero_day_prediction_targets;
  
      CREATE TEMPORARY TABLE temp_uc_zero_day_prediction_targets WITHOUT OIDS AS
        SELECT --Table ranking new subscribers according to sum of their topups from first 8 weeks
          aaaa.alias_id                                           AS alias_id,
          aaaa.eight_week_topups                                  AS eight_week_topups,
          rank() OVER (ORDER BY aaaa.eight_week_topups ASC)       AS rank_of_subscriber
        FROM ( --Table containing new subscribers during the period and the sum of their topups from first 8 weeks
          SELECT DISTINCT
            aaa.alias_id                                          AS alias_id,
            SUM(coalesce(aaa.topup_cost,0)) OVER (PARTITION BY aaa.alias_id)  AS eight_week_topups
          FROM ( --Table containing new subscribers during the period and all of their topups from first 8 weeks 
            SELECT 
              aa.alias_id                                         AS alias_id,
              bb.topup_cost                                       AS topup_cost
            FROM (SELECT DISTINCT alias_id, MAX(switch_on_date) OVER (PARTITION BY alias_id)   AS switch_on_date FROM data.in_crm ) AS aa  --CRM data for left join
            LEFT JOIN (
              SELECT 
                charged_id                                        AS charged_id, 
                topup_cost                                        AS topup_cost, 
                "timestamp"                                       AS "timestamp"
              FROM data.topup b     --TOPUP data for left join
              WHERE b.timestamp >= ''' || uc_zero_day_prediction_t4::text || '''::date  --Only topups during the period (to speed up join)
            ) AS bb
            ON aa.alias_id = bb.charged_id
            WHERE aa.switch_on_date >= ''' || uc_zero_day_prediction_t4::text || '''::date     --Only new subscribers
            AND   aa.switch_on_date <  ''' || uc_zero_day_prediction_t5::text || '''::date     
            --AND   aa.date_inserted  <= ''' || t9::text || '''::date                            --Only latest CRM data used (could use <= t2 as well)
            AND   bb."timestamp" BETWEEN date_trunc(''week'', aa.switch_on_date)::date + 7 
                                     AND date_trunc(''week'', aa.switch_on_date)::date + 7 * 9 --Topups from first 8 weeks of data included
          ) AS aaa
        ) AS aaaa;
      '; --End text string temp_table_sql
  ELSE
    --If model is only applied, no topup information is needed
    temp_table_sql := '
      DROP TABLE IF EXISTS temp_uc_zero_day_prediction_targets;
    
      CREATE TEMPORARY TABLE temp_uc_zero_day_prediction_targets WITHOUT OIDS AS
        SELECT DISTINCT a.alias_id FROM data.in_crm AS a  --CRM data (Note: CRM entry for subscriber is accepted regardless of CRM file date)
        WHERE a.switch_on_date >= ''' || uc_zero_day_prediction_t4::text || '''::date     --Only subscribers from last week
        AND   a.switch_on_date <  ''' || uc_zero_day_prediction_t5::text || '''::date     
      '; --End text string temp_table_sql

  END IF; --Script for populating temporary table temp_table_sql with new subscribers created

  EXECUTE(temp_table_sql);

  --If necessary, new value segment limits are assigned here based on target group topups
  IF uc_zero_day_prediction_redefine_value_segments OR this_uc_zero_day_prediction_segment_limits IS NULL THEN

    --New value segment limits are calculated from the data and added to mod job parameters table
    tmp := COUNT(*) FROM temp_uc_zero_day_prediction_targets;

    this_uc_zero_day_prediction_segment_limits[1] := MAX(eight_week_topups) 
      FROM temp_uc_zero_day_prediction_targets AS a
      WHERE a.rank_of_subscriber <= ceiling(tmp / 3);

    this_uc_zero_day_prediction_segment_limits[2] := MAX(eight_week_topups) 
      FROM temp_uc_zero_day_prediction_targets AS a
      WHERE a.rank_of_subscriber <= ceiling(2 * tmp / 3);

  END IF;

  --Determine whether segment limit variable already exists for this mod_job_id and update using the proper command
  tmp := COUNT(*) FROM work.module_job_parameters 
           WHERE mod_job_id = in_mod_job_id AND "key" = 'uc_zero_day_prediction_segment_limits';
  
  IF tmp > 0 THEN
    UPDATE work.module_job_parameters 
      SET "value" = array_to_string(this_uc_zero_day_prediction_segment_limits,',')
      WHERE "key" = 'uc_zero_day_prediction_segment_limits'
      AND   mod_job_id = in_mod_job_id;
  ELSE
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    SELECT 
      in_mod_job_id,
      'uc_zero_day_prediction_segment_limits',
      array_to_string(this_uc_zero_day_prediction_segment_limits,',');
  END IF;
  
  PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);
    
  -- Subscribers are inserted into tmp.module_targets_tmp along with their value segments
  -- Note: Currently 3 value segments are used, adding value segments can be done but 
  --       it will require some modifications also elsewhere in code.
  
  IF fit_model OR (calculate_predictors AND NOT apply_model) THEN --In practice: run_type is 'Predictors' or 'Predictors + Fit + Apply'
  
    INSERT INTO tmp.module_targets_tmp ( mod_job_id, alias_id, use_case_name, target )
      SELECT
        in_mod_job_id         AS mod_job_id,
        a.alias_id              AS alias_id,
        'zero_day_prediction' AS use_case_name,
        CASE WHEN a.eight_week_topups <= this_uc_zero_day_prediction_segment_limits[1] THEN 1
             WHEN a.eight_week_topups >  this_uc_zero_day_prediction_segment_limits[1] 
             AND  a.eight_week_topups <= this_uc_zero_day_prediction_segment_limits[2] THEN 2
             WHEN a.eight_week_topups >  this_uc_zero_day_prediction_segment_limits[2] THEN 3
             ELSE NULL END --target (i.e. subscriber value segment) is 1, 2, 3 or NULL depending on sum of topups in first 8 weeks
          AS target
      FROM temp_uc_zero_day_prediction_targets AS a
      LEFT JOIN (
        SELECT alias_id 
        FROM aliases.blacklist
          WHERE validity = 
            (SELECT max(validity) 
             FROM aliases.blacklist 
             WHERE validity <= t2
          ) 
      ) bl
      ON a.alias_id=bl.alias_id
      WHERE bl.alias_id IS NULL; 

  ELSE --Existing model is applied, segment is not yet known so -1 is inserted as target
    
    INSERT INTO tmp.module_targets_tmp ( mod_job_id, alias_id, use_case_name, target )
      SELECT
        in_mod_job_id         AS mod_job_id,
        a.alias_id              AS alias_id,
        'zero_day_prediction' AS use_case_name,
        -1                    AS target
    FROM temp_uc_zero_day_prediction_targets AS a
      LEFT JOIN (
        SELECT alias_id 
        FROM aliases.blacklist
          WHERE validity = 
            (SELECT max(validity) 
             FROM aliases.blacklist 
             WHERE validity <= t2
          ) 
      ) bl
      ON a.alias_id=bl.alias_id
      WHERE bl.alias_id IS NULL;  

  END IF;

  DROP TABLE IF EXISTS temp_uc_zero_day_prediction_targets;
  
  --Returns number of targets for zero day prediction use case
  RETURN ( SELECT count(mt.*) FROM tmp.module_targets_tmp AS mt WHERE mt.mod_job_id = in_mod_job_id AND use_case_name = 'zero_day_prediction' );

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_target_list_zero_day_prediction(integer) OWNER TO xsl;


CREATE OR REPLACE FUNCTION work.create_target_list_product(in_mod_job_id integer)
  RETURNS integer AS
$BODY$

/* SUMMARY
 * This function determines the set of aliases that will be processed during a
 * certain module job in the Best next product use case. If possible, the value of target 
 * variable is also calculated. The function writes to tmp.module_targets_tmp. After
 * calling the use case -specific create_target_list functions, calling work.create_target_list
 * combines the target lists into table work.module_targets.
 *
 * VERSION
 * 07.08.2013 HMa Consider only outgoing traffic as activity
 * 30.07.2013 KL Fixed to work right with new blacklists.
 * 17.05.2013 KL  Removed alias_id:s in aliases.blacklist from targets
 * 12.04.2013 HMa - IFIC-126
 * 31.01.2013 HMa
 */

DECLARE

  mjp            record;  
  product_arr    text[];
  prod           text;
  ind            integer;

BEGIN

  SELECT DISTINCT
    in_mod_job_id AS mod_job_id,
    max(CASE WHEN lower(d.key) = 'tcrm'                              THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS tcrm,
    max(CASE WHEN lower(d.key) = 'uc_product_products'               THEN d.value                        ELSE NULL END) AS products,
    max(CASE WHEN lower(d.key) = 'uc_product_target_calculated_from' THEN d.value                        ELSE NULL END) AS target_calculated_from,
    max(CASE WHEN lower(d.key) = 't2'                                THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS t2,
    max(CASE WHEN lower(d.key) = 'uc_product_t4'                     THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS t4,
    max(CASE WHEN lower(d.key) = 'uc_product_t5'                     THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS t5
  INTO mjp
  FROM work.module_job_parameters AS d
  WHERE d.mod_job_id = in_mod_job_id;


  product_arr    := string_to_array(trim(mjp.products),',');
  
  FOR ind IN 1..array_upper(product_arr, 1) LOOP
    
    -- Use case name:
    prod    := product_arr[ind];

    -- Check if the tmp.module_targets_tmp table already contains data for this product:
    IF ( SELECT mt.alias_id FROM tmp.module_targets_tmp AS mt WHERE mt.mod_job_id = in_mod_job_id AND use_case_name = 'product_' || prod LIMIT 1 ) IS NOT NULL THEN
      RAISE EXCEPTION 'Target list has already been calculated for the best next product use case, product %, module_job_id %', prod, in_mod_job_id;
    END IF;

    IF mjp.target_calculated_from = 'target'  THEN
      -- Targets:
      --   * Are active during the last week of the source period 
      --   * Do not have the product during the last week of the source period
      --   * Subscribers for whom it is not possible to activate the product are excluded. 
      --     If it is not known if the product can be activated, the subscriber is included. 
      -- target =  1:  Those who bought the product during the target period
      -- target = -1: Those who did not buy the product during the target period
      -- target = NULL: Those who bought the product after the source period but before the target period
      -- In the prediction phase (when we have data until t2), all the subscribers get target value -1.
      -- The table data.product is assumed to be delivered/filled weekly and dated to the Sunday of the week
      -- It should always contain a record for all the products that are activated at least part of the week. 

      INSERT INTO tmp.module_targets_tmp ( mod_job_id, alias_id, use_case_name, target )
      SELECT 
        in_mod_job_id AS mod_job_id,
        crm.alias_id,
        'product_' || prod AS use_case_name,
        CASE 
          WHEN target = 1 THEN 1
          WHEN target IS NULL THEN -1 -- Not all the subscribers who do not have the product are necessarily in the data.product data
          WHEN target = 2 THEN NULL -- Those who buy the product before the campaign
        END AS target
      FROM data.in_crm AS crm -- select subscribers in the CRM
      INNER JOIN ( -- only active subscribers
        SELECT act.alias_id FROM (
          SELECT alias_id, monday FROM data.call_types_weekly WHERE direction = 'm'
          UNION SELECT charged_id AS alias_id, date_trunc('week', timestamp)::date AS monday FROM data.topup
        ) act
        WHERE act.monday >= mjp.t2 - 7 -- activity during the last week of the source period is required
        AND act.monday < mjp.t2
        GROUP BY act.alias_id
      ) d
      ON crm.alias_id = d.alias_id
      LEFT JOIN (
        SELECT alias_id 
        FROM aliases.blacklist
        WHERE validity = 
          (SELECT max(validity) 
           FROM aliases.blacklist
           WHERE validity <= mjp.t2)
      ) bl 
      ON d.alias_id=bl.alias_id 
      LEFT JOIN ( -- Determine target value from product taken date:
        SELECT 
          ps1.alias_id,
          max(
            CASE
                WHEN ps1.product_taken_date IS NOT NULL
                AND ps1.product_taken_date >= mjp.t4 
                AND ps1.product_taken_date <  mjp.t5 
              THEN 1
                WHEN ps1.product_taken_date IS NOT NULL
                AND ps1.product_taken_date >= mjp.t2 
                AND ps1.product_taken_date < mjp.t4
              THEN 2 -- Those who buy the product before the campaign
              ELSE NULL
            END
		  ) AS target
        FROM data.product AS ps1
        WHERE ps1.product_id = prod
        AND ps1.date_inserted >= mjp.t2
        GROUP BY ps1.alias_id
      ) AS p
      ON crm.alias_id = p.alias_id
      LEFT JOIN (  -- select subscribers who do not have the product on the last week of the source period
        SELECT
          alias_id,
          product_taken_date,
          product_churn_date,
          product_possible
        FROM data.product
        WHERE date_inserted = mjp.t2 - 1
        AND product_id = prod
      ) AS p1
      ON crm.alias_id = p1.alias_id
      WHERE crm.date_inserted = mjp.tcrm
      AND bl.alias_id IS NULL
      AND (p1.product_taken_date IS NULL OR p1.product_churn_date < mjp.t2 - 7) -- targets should not have the product during the last week of the source period
      AND (p1.product_possible OR p1.product_possible IS NULL); -- the product should be possible to activate for the targets (or product_possible status not known)
      

    ELSIF mjp.target_calculated_from = 'source'  THEN
      -- Targets: 
      --   * Are active during the end of the source period 
      --   * Subscribers for whom it is not possible to activate the product are excluded. 
      --     If it is not known if the product can be activated, the subscriber is included. 
      -- Target = 1:  Have the product in the end of the source period (last week)
      -- Target = -1: Do not have the product in the end of the source period (last week)
      -- Only subscribers with target = -1 are scored

      INSERT INTO tmp.module_targets_tmp ( mod_job_id, alias_id, use_case_name, target )
      SELECT 
        in_mod_job_id AS mod_job_id,
        crm.alias_id,
        'product_' || prod AS use_case_name,
        CASE 
          WHEN target = 1 THEN 1
          WHEN target IS NULL THEN -1 -- Not all the subscribers who do not have the product are necessarily in the data.product data
          WHEN target = 2 THEN NULL -- Those who buy the product before the campaign
        END AS target
      FROM data.in_crm AS crm -- select subscribers in the CRM
      INNER JOIN ( -- only active subscribers
        SELECT act.alias_id FROM (
          SELECT alias_id, monday FROM data.call_types_weekly WHERE direction = 'm'
          UNION SELECT charged_id AS alias_id, date_trunc('week', timestamp)::date AS monday FROM data.topup
        ) act
        WHERE act.monday >= mjp.t2 - 7 -- activity during the last week of the source period is required
        AND act.monday < mjp.t2
       GROUP BY act.alias_id
      ) d
      ON crm.alias_id = d.alias_id
      LEFT JOIN (
        SELECT alias_id 
        FROM aliases.blacklist
        WHERE validity = 
          (SELECT max(validity) 
           FROM aliases.blacklist
           WHERE validity <= mjp.t2)
      ) bl
      ON d.alias_id=bl.alias_id 
      LEFT JOIN ( -- Determine target value from product status during the last week of the source period:
        SELECT 
          ps1.alias_id,
          max(
            CASE
                WHEN ps1.product_taken_date IS NOT NULL -- The product has been activated before t2
                AND  ps1.product_churn_date IS NULL     -- The product has not been deactivated before t2
              THEN 1
              ELSE NULL
            END
		  ) AS target
        FROM data.product AS ps1
        WHERE ps1.product_id = prod
--        AND (ps1.subproduct_id = subprod OR (CASE WHEN subprod IS NULL THEN TRUE END)) -- If subproduct ID is not given, any subproduct of this product is included
        AND ps1.date_inserted = mjp.t2
        GROUP BY ps1.alias_id
      ) AS p
      ON crm.alias_id = p.alias_id
      LEFT JOIN (  -- select subscribers for whom it is possible to activate the product (or not known if possible) according to the latest product data from the source period
        SELECT
          alias_id,
          product_possible
        FROM data.product
        WHERE date_inserted = mjp.t2
        AND product_id = prod
--        AND (subproduct_id = subprod OR (CASE WHEN subprod IS NULL THEN TRUE END)) -- If subproduct ID is not given, any subproduct of this product is included
      ) AS p1
      ON crm.alias_id = p1.alias_id
      WHERE crm.date_inserted = mjp.tcrm
      AND bl.alias_id IS NULL
      AND (p1.product_possible OR p1.product_possible IS NULL); -- the product should be possible to activate for the targets (or product_possible status not known)

    ELSE
    
      RAISE EXCEPTION 'Best next product use case target needs to be calcualted either from source or target period!';

    END IF;


  END LOOP;

  RETURN ( SELECT count(mt.*) FROM tmp.module_targets_tmp AS mt WHERE mt.mod_job_id = in_mod_job_id AND use_case_name ~ 'product' );

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_target_list_product(integer) OWNER TO xsl;




-------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.model_parameters_for_fitter(
  in_mod_job_id integer,
  in_constraint_keys text[],
  in_constraint_values text[],
  in_model_copy_keys text[],
  in_module_copy_keys text[]
) RETURNS integer AS
$BODY$

/* 
 * SUMMARY
 * This function searches the most recent model that satisfies given key-value
 * constraints. If such a model cannot be found, then -1 is returned. Otherwise,
 * preprocessing parameters of the found model are replicated with a new
 * model_id. Some extra parameters are specified in this function and the
 * regression coefficients will be added by the model fitter later. This
 * function supports a specific model_type called 'submodel_list' which is a
 * list of several individual (sub)models.
 *
 * INPUTS
 * in_mod_job_id        : Identifier of module job
 * in_constraint_keys   : 1st parts of key-value constraints
 * in_constraint_values : 2nd parts of key-value constraints
 * in_model_copy_keys   : Additional keys that are copied from work.module_models table (use array[NULL] if none)
 * in_module_copy_keys  : Additional keys that are copied from work.module_job_parameters table  (use array[NULL] if none)
 * 
 * OUTPUT
 * model_id_child       : Identifier of the new model
 * 
 * VERSION
 * 04.04.2014 JVi - Added vargroup to model_copy_keys
 * 22.02.2012 MOj - Bug 837 fixed
 * 17.03.2011 TSi - Bug 470, 472 and 473 fixed
 * 05.01.2010 TSi - Bug 94 fixed
 * 07.12.2009 Sja - Bug 37 fixed
 * 02.12.2009 TSi - Bug 76 fixed
 * 26.11.2009 TSi - Bug 66 fixed
 * 10.11.2009 TSi - Bug 46 fixed
 * 25.03.2009 TSi - Bug 73 fixed
 * 05.12.2008 TSi
 */


DECLARE
  query text;
  model_id_parent integer;    -- Identifier of the most recent suitable parent model
  model_id_child integer;     -- Identifier of child model
  submodel_id_parent integer; -- Identifier of parent submodel
  submodel_id_child integer;  -- Identifier of child submodel
  model_copy_keys text[] := array_cat(in_model_copy_keys, array[
    'model_name', 'model_type', 'output_name', 'target_name', 'use_case', 'mod_job_id', 'tempmodel_id', 
    'varnum', 'varname', 'center', 'scale', 'lowercut', 'uppercut', 'nareplace',
    'rcsnum', 'preprocessfun', 'preprocessparam', 'rcsknot', 'catnum',
    'catlabel', 'where_clause', 'mulen', 'epsilonmax', 'alpha', 'beta', 'tmin',
    'maxiter', 'varlambda', 'rcslambda', 'catlambda', 'max_fitter_sample_size',
    'make_sanity_check', 'lambda_sequence_length', 'lambda_sequence_minmult',
    'indcond', 'indname', 'indlambda', 'interactcond', 'interactname', 'interactlambda', 'multinomial_classes', 'vargroup']);

BEGIN

  -- Initialize a query for parent model search.
  query := '
    SELECT a.model_id
    FROM (
      SELECT mm.model_id, to_timestamp(mm.value, ''YYYY-MM-DD HH24:MI:SS'') AS model_date
      FROM work.module_models AS mm
      WHERE mm.key = ''model_date''
      AND mm.model_id IN (
        SELECT mm1.model_id 
        FROM work.module_models AS mm1
        WHERE mm1.key = '''||in_constraint_keys[1]||'''
        AND mm1.value = '''||in_constraint_values[1]||'''';

  -- Add rest of the key-value constraints to the query.
  FOR i IN 2 .. (SELECT array_upper(in_constraint_keys, 1)) LOOP

    query := query||'
        INTERSECT
        SELECT mm'||i||'.model_id 
        FROM work.module_models AS mm'||i||'
        WHERE mm'||i||'.key = '''||in_constraint_keys[i]||''' 
        AND mm'||i||'.value = '''||in_constraint_values[i]||'''';

  END LOOP;

  -- Finalize the query.
  query := query||')
      ORDER BY model_date DESC, mm.model_id DESC
      LIMIT 1
    ) AS a';

  -- Search the most recent model that satisfies the key-value constraints.
  EXECUTE query INTO model_id_parent;

  -- Stop here if parent model was not found.
  IF model_id_parent IS NULL THEN
    RETURN -1;
  END IF;

  -- Get identifier of child model.
  SELECT core.nextval_with_partitions('work.model_sequence') INTO model_id_child;

  -- Copy some parameters of parent model and insert them into work.module_models
  -- associated with child model.
  INSERT INTO work.module_models
  (model_id, aid, bid, output_id, "key", "value")
  SELECT 
    model_id_child AS model_id, 
    mm.aid, 
    mm.bid, 
    mm.output_id, 
    CASE WHEN mm.key = 'mod_job_id' THEN 'mod_job_id_of_parent_model' ELSE mm.key END, 
    mm.value
  FROM work.module_models AS mm
  WHERE mm.model_id = model_id_parent
  AND array[mm.key] && model_copy_keys
  AND NOT (mm.key = 'model_name' AND mm.value = 'parent'); -- This condition is for backwards compatibility because model_name used to indicate parent/child information
   
  -- Insert data associated with child model into module_models.
  INSERT INTO work.module_models
  (model_id, aid, bid, output_id, "key", "value")
  VALUES
  (model_id_child, 0, 0, 0, 'mod_job_id', in_mod_job_id::text),
  (model_id_child, 0, 0, 0, 'parent_model_id', model_id_parent::text);
  
  -- For multinomial models, output classes have to be specified
  -- Note: this is done separately for submodels below
  IF (SELECT mm.value FROM work.module_models AS mm WHERE mm.model_id = model_id_parent AND mm.key = 'model_type') = 'multinomial'
  THEN FOR ind_class IN 1..(SELECT mm.value FROM work.module_models AS mm WHERE mm.model_id = model_id_parent AND mm.key = 'multinomial_classes')::integer
    LOOP 
      INSERT INTO work.module_models (model_id, aid, bid, output_id, "key", "value") 
      VALUES (model_id_child, 0, 0, ind_class, 'output_name', 'Multinomial class ' || ind_class);
    END LOOP;
  DELETE FROM work.module_models WHERE model_id = model_id_child AND key = 'output_name' AND value = 'premodel';
  END IF;

  -- Copy some key-value pairs from work.module_job_parameters and insert 
  -- them into work.module_models associated with child model.
  INSERT INTO work.module_models
  (model_id, aid, bid, output_id, "key", "value")
  SELECT model_id_child AS model_id, 0 AS aid, 0 AS bid, 0 AS output_id, mjp.key, mjp.value
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = in_mod_job_id
  AND array[mjp.key] && in_module_copy_keys;
  
  -- Check if model type is 'submodel_list'.
  IF (
    SELECT mm.value 
    FROM work.module_models AS mm 
    WHERE mm.model_id = model_id_parent AND mm.key = 'model_type'
  ) = 'submodel_list' THEN

    -- Add date into work.module_models associated with child model. 
    -- In other model types, the fitter does this.
    INSERT INTO work.module_models
    (model_id, aid, bid, output_id, "key", "value")
    SELECT model_id_child AS model_id, 0 AS aid, 0 AS bid, 0 AS output_id, 
      'model_date'::text AS "key", 
      to_char(CURRENT_TIMESTAMP(0), 'YYYY-MM-DD HH24:MI:SS') AS "value";

    -- Loop over submodels.
    FOR submodel_id_parent IN (
      SELECT mm.value
      FROM work.module_models AS mm
      WHERE mm.model_id = model_id_parent AND mm.key = 'model_id'
    ) LOOP
    
      -- Get identifier of child submodel.
      SELECT core.nextval_with_partitions('work.model_sequence') INTO submodel_id_child;
      
      -- Copy some parameters of parent submodel and insert them into work.module_models
      -- associated with child submodel.
      INSERT INTO work.module_models
      (model_id, aid, bid, output_id, "key", "value")
      SELECT 
        submodel_id_child AS model_id, 
        mm.aid, mm.bid, 
        mm.output_id, 
        CASE WHEN mm.key = 'mod_job_id' THEN 'mod_job_id_of_parent_model' ELSE mm.key END,
        mm.value
      FROM work.module_models AS mm
      WHERE mm.model_id = submodel_id_parent
      AND array[mm.key] && model_copy_keys
      AND NOT (mm.key = 'model_name' AND mm.value = 'parent'); -- This condition is for backwards compatibility because model_name used to indicate parent/child information

      -- Insert data associated with child model and child submodel into module_models.
      INSERT INTO work.module_models
      (model_id, aid, bid, output_id, "key", "value")
      VALUES
      (submodel_id_child, 0, 0, 0, 'mod_job_id', in_mod_job_id::text),
      (submodel_id_child, 0, 0, 0, 'parent_model_id', submodel_id_parent::text),
      (submodel_id_child, 0, 0, 0, 'head_model_id', model_id_child::text),
      (model_id_child, 0, 0, 0, 'model_id', submodel_id_child::text);

      -- For multinomial models, output classes have to be specified
      IF (SELECT mm.value FROM work.module_models AS mm WHERE mm.model_id = submodel_id_parent AND mm.key = 'model_type') = 'multinomial'
      THEN FOR ind_class IN 1..(SELECT mm.value FROM work.module_models AS mm WHERE mm.model_id = submodel_id_parent AND mm.key = 'multinomial_classes')::integer
        LOOP 
          INSERT INTO work.module_models (model_id, aid, bid, output_id, "key", "value") 
          VALUES (submodel_id_child, 0, 0, ind_class, 'output_name', 'Multinomial class ' || ind_class);
        END LOOP;
      DELETE FROM work.module_models WHERE model_id = submodel_id_child AND key = 'output_name' AND value = 'premodel';
      END IF;

      -- Copy some key-value pairs from work.module_job_parameters and insert 
      -- them into work.module_models associated with child submodel.
      INSERT INTO work.module_models
      (model_id, aid, bid, output_id, "key", "value")
      SELECT submodel_id_child AS model_id, 0 AS aid, 0 AS bid, 0 AS output_id, mjp.key, mjp.value
      FROM work.module_job_parameters AS mjp
      WHERE mjp.mod_job_id = in_mod_job_id 
      AND array[mjp.key] && in_module_copy_keys;

    END LOOP;
      
  END IF;

  RETURN model_id_child;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.model_parameters_for_fitter(integer, text[], text[], text[], text[]) OWNER TO xsl;


-------------------------------------------------------------------------------


-- The function is used but the content could be revised...
CREATE OR REPLACE FUNCTION work.calculate_network_roles(integer)
  RETURNS integer AS
$BODY$
   
DECLARE
   xsljobid ALIAS for $1;
   klimit integer; 
   klimitpos integer;  
   wlimit double precision;
   wlimitpos integer;
  BEGIN

  delete from work.network_roles where job_id = xsljobid;
  
  select (count(*)*0.1)::integer into klimitpos from work.out_scores where job_id = xsljobid;
  select (count(*)*0.9)::integer into wlimitpos from work.out_network where job_id = xsljobid;

  select min(k) into klimit from (select k from work.out_scores where job_id = xsljobid order by k desc limit klimitpos) aa ;
  select max(weight) into wlimit from (select weight from work.out_network where job_id = xsljobid order by weight limit wlimitpos) aa ;

  truncate tmp.nwr_linkcount;
  truncate tmp.nwr_linkcount_tmp;
  truncate tmp.hublist;
  truncate tmp.bridgelist;
  truncate tmp.network_roles_fix;

  insert into tmp.nwr_linkcount_tmp (alias_id, lc) select alias_a, case when weight > wlimit then 1 else 0 end from work.out_network where job_id = xsljobid;
  insert into tmp.nwr_linkcount_tmp (alias_id, lc) select alias_b, case when weight > wlimit then 1 else 0 end from work.out_network where job_id = xsljobid;
  insert into tmp.nwr_linkcount (alias_id, lc) select alias_id, sum(lc) from tmp.nwr_linkcount_tmp group by alias_id;

  insert into tmp.hublist select distinct alias_id from 
    (select aa.alias_id from work.out_network oo, tmp.nwr_linkcount aa, tmp.nwr_linkcount bb where job_id = xsljobid  and 
    aa.alias_id = oo.alias_a and bb.alias_id = oo.alias_b and aa.lc > klimit and bb.lc <= klimit
    union all 
    select distinct bb.alias_id from work.out_network oo, tmp.nwr_linkcount aa, tmp.nwr_linkcount bb where job_id = xsljobid  and 
    aa.alias_id = oo.alias_a and bb.alias_id = oo.alias_b and aa.lc <= klimit and bb.lc > klimit) bb;
  
  insert into tmp.network_roles_fix (job_id,alias_id, ishub, isbridge, isoutlier)
  select xsljobid,
    aa.alias_id,
    case when aa.lc > klimit then 1 else 0 end as ishub,
    case when bb.sumk > 1 then 1 else 0 end as isbridge,
        case when aa.lc < 2 then 1 else 0 end as isoutlier
   from tmp.nwr_linkcount aa, (select alias_id, sum(sumk) as sumk from 
   (select dd.alias_a as alias_id, count(*) as sumk  
    from tmp.hublist cc, work.out_network dd
    where dd.alias_b = alias_id and dd.job_id = xsljobid
    group by dd.alias_a 
    union all
    select dd.alias_b as alias_id, count(*) as sumk  
    from tmp.hublist cc, work.out_network dd 
    where dd.alias_a = alias_id and dd.job_id = xsljobid
    group by dd.alias_b 
    ) cc group by alias_id) bb 
  where aa.alias_id = bb.alias_id;

  insert into tmp.bridgelist
  select distinct alias_id from (
  select alias_id from (
  select cc.alias_id, oo1.alias_b, oo2.alias_a from tmp.network_roles_fix cc 
  join work.out_network oo1 on oo1.alias_a = cc.alias_id and oo1.job_id = xsljobid   
  join tmp.network_roles_fix cc1 on cc1.alias_id = oo1.alias_b join work.out_network oo2 on oo2.alias_b = cc.alias_id and oo2.job_id = xsljobid 
  join tmp.network_roles_fix cc2 on cc2.alias_id = oo2.alias_a 
  where cc.isbridge=1 and cc.ishub=0 and cc1.ishub=1 and cc2.ishub=1 and cc1.alias_id <> cc2.alias_id 
    and cc.job_id = xsljobid and oo1.job_id = xsljobid and oo2.job_id = xsljobid) x1
  left outer join work.out_network oo on (oo.alias_a = x1.alias_a and oo.alias_b = x1.alias_b) and oo.job_id = xsljobid 
  where oo.alias_a is null 
  union all 
  select alias_id from ( select cc.alias_id, oo1.alias_b, oo2.alias_a from tmp.network_roles_fix cc 
  join work.out_network oo1 on oo1.alias_a = cc.alias_id and oo1.job_id = xsljobid 
  join tmp.network_roles_fix cc1 on cc1.alias_id = oo1.alias_b 
  join work.out_network oo2 on oo2.alias_b = cc.alias_id and oo2.job_id = xsljobid 
  join tmp.network_roles_fix cc2 on cc2.alias_id = oo2.alias_a 
  where cc.isbridge=1 and cc.ishub=0 and cc1.alias_id <> cc2.alias_id and cc1.ishub=1 and cc2.ishub=1
    and cc.job_id = xsljobid and oo1.job_id = xsljobid and oo2.job_id = xsljobid) x2
  left outer join work.out_network oo on (oo.alias_b = x2.alias_a and oo.alias_a = x2.alias_b) and oo.job_id = xsljobid where oo.alias_a is null) x0;

  insert into work.network_roles (job_id,alias_id, ishub, isbridge, isoutlier)
  select xsljobid,
    aa.alias_id,
    aa.ishub,
    case when bb.alias_id is not null then 1 else 0 end as isbridge,
        aa.isoutlier
   from tmp.network_roles_fix aa left outer join tmp.bridgelist bb on aa.alias_id = bb.alias_id;

  return wlimit::integer;
  
  END; 
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.calculate_network_roles(integer) OWNER TO xsl;


-------------------------------------------------------------------------------

-- Function: work.fetch_aliases(integer, date, date)

-- DROP FUNCTION work.fetch_aliases(integer, date, date);

CREATE OR REPLACE FUNCTION work.fetch_aliases(in_job_id integer, in_date1 date, in_date2 date)
  RETURNS void AS
$BODY$
/* SUMMARY
 * This function reads from aliases.aliases_updated table and inserts into work.aliases
 * table such that alias_id column is unique for the given job_id. 
 * 
 * INPUTS
 * in_job_id : Identifier of job (use work.network_sequence)
 * in_date1  : Inclusive begin date of the period
 * in_date2  : Exclusive end date of the period
 *
 * Case in_date1 < in_date2 :
 *    The function fetches the aliases that have innet status in period
 *    [in_date1, in_date2). It is not required that innet status is on all the
 *    time. If innet status is found the alias is inserted with in_out_network=1
 *    into work.aliases table and, otherwise, with in_out_network=0.
 * Case in_date1 >= in_date2 :
 *    Produces error
 *
 * VERSION
 * 2013-07-30 KL  - fixed to work right with new blacklist
 * 2013-07-10 KL  - blacklist information cecked from aliases.blacklist table as aliases.aliases_updated  may not be up-to-date
 * 2013-05-02 MOj - ICIF 129 fixed
 * 2011-10-14 TSi - Bug 742 fixed
 * 2010-05-04 JTi
 */
BEGIN
  IF (in_date1 >= in_date2) THEN
    RAISE EXCEPTION 'The time period is illegal for fetch_aliases';
  END IF;

  INSERT INTO work.aliases (
    job_id, 
    alias_id, 
    on_black_list, 
    in_out_network )
  SELECT 
    in_job_id AS job_id,
    a.alias_id,
    CASE WHEN max(bl.on_black_list) IS NULL THEN 0
      ELSE max(bl.on_black_list) END AS on_black_list,
    max(a.in_out_network) AS in_out_network
  FROM (
    SELECT         
      al.alias_id,
      al.in_out_network,
      COALESCE(LEAD(al.validity) OVER(PARTITION BY al.alias_id ORDER BY al.validity), '2099-01-01'::date) AS next_validity
    FROM aliases.aliases_updated al
    WHERE al.validity < in_date2
  ) a
  LEFT JOIN 
   (SELECT alias_id, on_black_list 
      FROM aliases.blacklist
      WHERE validity = 
        (SELECT max(validity) 
        FROM aliases.blacklist
        WHERE validity < in_date2 
      ) 
    ) bl
  ON a.alias_id=bl.alias_id  
  WHERE a.next_validity > in_date1
  GROUP BY a.alias_id;

  PERFORM core.analyze('work.aliases', in_job_id);

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.fetch_aliases(integer, date, date)
  OWNER TO xsl;
--------------------------------------------------------

-----------------------------


CREATE OR REPLACE FUNCTION results.copy_from_output_to_results(module_job_id integer)
RETURNS integer AS
$BODY$

/* SUMMARY
 * This function copies model output from work.module_model_output table to
 * tmp.module_results_tmp table for the included use cases. 
 * Afterwards, results.combine_results function is called to combine the
 * results in the results.module_results table. 
 *
 * INPUTS
 * module_job_id : integer : Indentifier of module job.
 *
 * OUTPUT
 * count         : integer : The number of copied rows
 * 
 * VERSION
 * 01.02.2013 HMa - improved use-case modularity
 * 11.01.2013 HMa - support for multiple use cases
 * 05.12.2012 JTI 
 */

DECLARE

  job_use_cases text;
  
  use_cases     text[];
  use_case      text;
  uc_model_id   integer;
  
  records_found integer;
  n_rows        integer;

  product_run   boolean;
  
BEGIN

  -- Check if the results.module_results table already contains data for the given module_job_id:
  records_found := count(mr.*) FROM results.module_results AS mr WHERE mr.mod_job_id = module_job_id;
  IF records_found IS NOT NULL AND records_found > 0 THEN
    
    RAISE EXCEPTION 'Table results.module_results already contains data for mob_job_id %', module_job_id;
    
  END IF;

  job_use_cases := (
    SELECT mjp.value
    FROM work.module_job_parameters AS mjp 
    WHERE mjp.mod_job_id = module_job_id AND mjp.key = 'job_use_cases'
  );

  -- Read the use case names from job_use_cases:
  use_cases := string_to_array(trim(job_use_cases),',');
  
  -- Each product is listed separately in job_use_cases, but results.copy_from_output_to_results_product needs to be called only once. 
  product_run = FALSE; 

  FOR uc_ind IN 1..array_upper(use_cases, 1) LOOP
    
    -- Use case name:
    use_case := use_cases[uc_ind];

    -- Each product is listed separately in job_use_cases and they have their own submodels, but the submodel list model id is saved in module job parameters. 
    IF use_case ~ 'product' THEN
      use_case := 'product';
    END IF;

    -- Use case model id: 
    uc_model_id :=  mjp.value FROM work.module_job_parameters AS mjp WHERE mjp.mod_job_id = module_job_id AND mjp.key = 'uc_' || use_case || '_model_id';
    
    IF uc_model_id IS NULL THEN    
      RAISE EXCEPTION '%_model_id not found!', use_case;
    ELSIF use_case = 'churn_inactivity' THEN
      PERFORM results.copy_from_output_to_results_churn(module_job_id, uc_model_id, 'inactivity');
    ELSIF use_case = 'churn_postpaid' THEN
      PERFORM results.copy_from_output_to_results_churn(module_job_id, uc_model_id, 'postpaid');
    ELSIF use_case = 'zero_day_prediction' THEN
      PERFORM results.copy_from_output_to_results_zero_day_prediction(module_job_id, uc_model_id);
    ELSIF use_case = 'product' AND NOT product_run THEN
      PERFORM results.copy_from_output_to_results_product(module_job_id, uc_model_id);
      product_run = TRUE;
    ELSIF product_run THEN
      -- Do not raise notice
    ELSE
      RAISE NOTICE 'Copy results for use case ''%'' not supported', use_case;
    END IF;
  
  END LOOP;

  SELECT * FROM results.combine_results(module_job_id) INTO n_rows;

  RETURN n_rows;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.copy_from_output_to_results(integer) OWNER TO xsl;




------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.copy_from_output_to_results_churn(in_mod_job_id integer, in_model_id integer, churn_type text)
RETURNS void AS
$BODY$

/* SUMMARY
 * This function copies churn use case model output from work.module_model_output table to
 * tmp.module_results_tmp table. 
 *
 * INPUTS
 * in_mod_job_id : integer : Indentifier of module job.
 * in_model_id   : integer : Identifier of model id.
 * churn_type    : text    : churn type, 'inactivity' or 'postpaid'
 * 
 * VERSION
 *
 * 17.10.2013 HMa: Calculating expected total loss for postpaid as well
 * 16.05.2013 KL:  Removed influence stuff 
 * 15.04.2013 KL:  Arpu times churn propensity score added to output 
 * 06.03.2013 HMa: Read influence parameters from the relevant model id only. 
 * 01.02.2013 HMa: Compatibility with multiple use cases
 * Modified from results.copy_from_output_to_results (05.12.2012 JTI)
 */

DECLARE
  
  records_found     integer;
  is_submodel_list  text;
  model_ids         integer[];
  uc_model_id       integer;
  m_ind             integer;
    
BEGIN

  -- Check if the model id is a submodel list: 
  SELECT "value" = 'submodel_list' FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_type' INTO is_submodel_list;

  IF is_submodel_list THEN -- Insert submodel ids to the model id array
    model_ids := array(SELECT "value"::integer FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_id');
  ELSE
    model_ids := ARRAY[in_model_id]; -- Insert model id to the model id array
  END IF;

  -- Loop over submodel ids (or only the main model id if there are no submodels): 
  FOR m_ind IN 1..array_upper(model_ids,1) LOOP

    uc_model_id := model_ids[m_ind];

    -- Check if the results.module_results table already contains data for the given in_mod_job_id:
    records_found := count(mr.*) FROM tmp.module_results_tmp AS mr WHERE mr.mod_job_id = in_mod_job_id AND mr.model_id = uc_model_id; 
    IF records_found IS NOT NULL AND records_found > 0 THEN
      
      RAISE EXCEPTION 'Table results.module_results already contains data for mob_job_id %, model_id %,', in_mod_job_id, uc_model_id;
      
    END IF;
    
    -- Insert results into a tmp result table in score_name-score_value format:
    INSERT INTO tmp.module_results_tmp
    (mod_job_id, model_id, alias_id, score_name, score_value)
    SELECT
      in_mod_job_id AS mod_job_id,
      uc_model_id AS model_id,
      alias_id, 
      'churn_'||churn_type||'_propensity_score'  AS score_name,
      propensity_score AS score_value
    FROM (
      SELECT
        s.alias_id,
        s.propensity_score
      FROM (
        SELECT
          mmo.alias_id,
          max(CASE WHEN mmo.output_id = 1 THEN mmo.value ELSE NULL END) AS propensity_score
        FROM work.module_model_output AS mmo
        WHERE mmo.mod_job_id = in_mod_job_id
        AND mmo.model_id = uc_model_id
        GROUP BY mmo.alias_id
      ) AS s
      GROUP BY alias_id, propensity_score
    ) AS a; 

    IF churn_type='inactivity' THEN

      INSERT INTO tmp.module_results_tmp
      (mod_job_id, model_id, alias_id, score_name, score_value)
      SELECT
        in_mod_job_id AS mod_job_id,
        uc_model_id AS model_id,
        a.alias_id, 
        'churn_inactivity_expected_revenue_loss' AS score_name,
        a.score_value*COALESCE(b.monthly_arpu,0) AS score_value
      FROM tmp.module_results_tmp a 
      LEFT JOIN work.monthly_arpu b
      ON a.alias_id=b.alias_id 
      AND b.mod_job_id = a.mod_job_id
      WHERE a.score_name='churn_inactivity_propensity_score' 
      AND a.mod_job_id=in_mod_job_id;

    ELSIF churn_type='postpaid' THEN
  
      INSERT INTO tmp.module_results_tmp
      (mod_job_id, model_id, alias_id, score_name, score_value)
      SELECT
        in_mod_job_id AS mod_job_id,
        uc_model_id AS model_id,
        a.alias_id, 
        'churn_postpaid_expected_revenue_loss' AS score_name,
        a.score_value*b.monthly_arpu AS score_value
      FROM tmp.module_results_tmp a 
      LEFT JOIN work.monthly_arpu b
      ON a.alias_id=b.alias_id 
      AND b.mod_job_id = a.mod_job_id
      WHERE a.score_name='churn_postpaid_propensity_score' 
      AND a.mod_job_id=in_mod_job_id;

    END IF;

  END LOOP;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.copy_from_output_to_results_churn(integer, integer, text) OWNER TO xsl;


CREATE OR REPLACE FUNCTION results.copy_from_output_to_results_product(in_mod_job_id integer, in_model_id integer)
RETURNS void AS
$BODY$

/* SUMMARY
 * This function copies product use case model output from work.module_model_output table to
 * tmp.module_results_tmp table.
 *
 * INPUTS
 * in_mod_job_id : integer : Indentifier of module job.
 * in_model_id   : integer : Identifier of model id.
 * 
 * VERSION
 * 01.02.2013 HMa 
 */

DECLARE
  
  records_found integer;
  is_submodel_list  text;
  model_ids         integer[];
  uc_model_id       integer;
  m_ind             integer;
  product_uc        text;
  
BEGIN

  -- Check if the model id is a submodel list: 
  SELECT "value" = 'submodel_list' FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_type' INTO is_submodel_list;

  IF is_submodel_list THEN -- Insert submodel ids to the model id array
    model_ids := array(SELECT "value"::integer FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_id');
  ELSE
    model_ids := ARRAY[in_model_id]; -- Insert model id to the model id array
  END IF;

  -- Loop over submodel ids (or only the main model id if there are no submodels): 
  FOR m_ind IN 1..array_upper(model_ids,1) LOOP

    uc_model_id := model_ids[m_ind];
    product_uc  := "value" FROM work.module_models WHERE model_id = uc_model_id AND "key" = 'use_case';

    -- Check if the results.module_results table already contains data for the given in_mod_job_id:
    records_found := count(mr.*) FROM tmp.module_results_tmp AS mr WHERE mr.mod_job_id = in_mod_job_id AND mr.model_id = uc_model_id; 
    IF records_found IS NOT NULL AND records_found > 0 THEN
      
      RAISE EXCEPTION 'Table results.module_results already contains data for mob_job_id %, model_id %', in_mod_job_id, uc_model_id;
      
    END IF;
    
    INSERT INTO tmp.module_results_tmp
    (mod_job_id, model_id, alias_id, score_name, score_value)
    SELECT
      mmo.mod_job_id, 
      mmo.model_id,
      mmo.alias_id, 
      product_uc ||'_propensity_score' AS score_name,
      mmo.value AS score_value
    FROM work.module_model_output AS mmo
    WHERE mmo.mod_job_id = in_mod_job_id 
    AND mmo.model_id = uc_model_id
    AND mmo.output_id = 1;

  END LOOP;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.copy_from_output_to_results_product(integer, integer) OWNER TO xsl;


------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.copy_from_output_to_results_zero_day_prediction(in_mod_job_id integer, in_model_id integer)
RETURNS void AS
$BODY$

/* SUMMARY
 * This function copies zero day prediction use case model output from work.module_model_output table to
 * tmp.module_results_tmp table.
 *
 * INPUTS
 * in_mod_job_id : integer : Indentifier of module job.
 * in_model_id   : integer : Identifier of model id.
 * 
 * VERSION
 * 09.02.2012 JVi 
 */

DECLARE
  
  records_found           integer;
  is_submodel_list        text;
  model_ids               integer[];
  uc_model_id             integer;
  m_ind                   integer;
  tenure_group            integer;
  
BEGIN

  -- Check if the model id is a submodel list: 
  SELECT "value" = 'submodel_list' FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_type' INTO is_submodel_list;
  
  IF is_submodel_list THEN -- Insert submodel ids to the model id array
    model_ids := array(SELECT "value"::integer FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_id');
  ELSE
    model_ids := ARRAY[in_model_id]; -- Insert model id to the model id array
  END IF;

  -- Loop over submodel ids (or only the main model id if there are no submodels): 
  FOR m_ind IN 1..array_upper(model_ids,1) LOOP

    uc_model_id := model_ids[m_ind];
    
    -- Check if the results.module_results table already contains data for the given in_mod_job_id:
    records_found := count(mr.*) FROM tmp.module_results_tmp AS mr WHERE mr.mod_job_id = in_mod_job_id AND mr.model_id = uc_model_id; 
    IF records_found IS NOT NULL AND records_found > 0 THEN
      
      RAISE EXCEPTION 'Table results.module_results already contains data for mob_job_id %, model_id %', in_mod_job_id, uc_model_id;
      
    END IF;
  
    SELECT "value" FROM work.module_models WHERE model_id = uc_model_id AND "key" = 'tenure_group' INTO tenure_group;
  
    INSERT INTO tmp.module_results_tmp
    (mod_job_id, model_id, alias_id, score_name, score_value)
    SELECT
      mmo.mod_job_id, 
      mmo.model_id,
      mmo.alias_id,
      CASE WHEN mmo.output_id = 1 THEN 'zero_day_prediction_low_value_propensity_score' 
           WHEN mmo.output_id = 2 THEN 'zero_day_prediction_medium_value_propensity_score' 
           WHEN mmo.output_id = 3 THEN 'zero_day_prediction_high_value_propensity_score' END 
        AS score_name,
      mmo.value AS score_value
    FROM work.module_model_output AS mmo
    WHERE mmo.mod_job_id = in_mod_job_id 
    AND mmo.model_id = uc_model_id
    AND mmo.output_id BETWEEN 1 AND 3;

    INSERT INTO tmp.module_results_tmp
    (mod_job_id, model_id, alias_id, score_name, score_value)
    SELECT 
      mod_job_id,
      model_id,
      alias_id,
      'zero_day_prediction_predicted_segment' AS score_name,
      CASE WHEN max(score_low_value)    >= max(score_medium_value) AND max(score_low_value)    >= max(score_high_value)   THEN 1
           WHEN max(score_medium_value) >  max(score_low_value)    AND max(score_medium_value) >= max(score_high_value)   THEN 2
           WHEN max(score_high_value)   >  max(score_low_value)    AND max(score_high_value)   >  max(score_medium_value) THEN 3 END
           AS score_value    
      FROM (SELECT
        mmo.mod_job_id, 
        mmo.model_id,
        mmo.alias_id,
        CASE WHEN mmo.output_id = 1 THEN mmo.value ELSE NULL END AS score_low_value,
        CASE WHEN mmo.output_id = 2 THEN mmo.value ELSE NULL END AS score_medium_value,
        CASE WHEN mmo.output_id = 3 THEN mmo.value ELSE NULL END AS score_high_value 
        FROM work.module_model_output AS mmo
      WHERE mmo.mod_job_id = in_mod_job_id 
      AND mmo.model_id = uc_model_id
      AND mmo.output_id BETWEEN 1 AND 3
    ) mmo2
    GROUP BY mod_job_id, model_id, alias_id, score_name;

    INSERT INTO tmp.module_results_tmp
    (mod_job_id, model_id, alias_id, score_name, score_value)
    SELECT DISTINCT
      mmo.mod_job_id,
      mmo.model_id,
      mmo.alias_id,
      'zero_day_prediction_tenure_group' AS score_name,
      substring(trim(TRAILING FROM mm.value) FROM char_length(trim(TRAILING FROM mm.value)) FOR 1)::integer AS score_value -- Takes last character of where clause which is the tenure group
    FROM work.module_model_output AS mmo
    LEFT JOIN work.module_models AS mm
    ON mmo.model_id = mm.model_id
    AND mm."key" = 'where_clause'
    WHERE mmo.mod_job_id = in_mod_job_id
    AND mmo.model_id = uc_model_id;
    
  END LOOP;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.copy_from_output_to_results_zero_day_prediction(integer, integer) OWNER TO xsl;


------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.combine_results(in_mod_job_id integer)
RETURNS integer AS
$BODY$

/* SUMMARY
 * This function combines model output from tmp.module_results_tmp table 
 * to results.module_results table.
 *
 * INPUTS
 * in_mod_job_id : integer : Indentifier of module job.
 *
 * OUTPUT
 * count         : integer : The number of copied rows
 * 
 * VERSION
 * 01.02.2013 HMa
 */

DECLARE
  
  records_found integer;
  score         text;

  colname_string text;
  max_string     text;
  case_string    text;
  sql_string     text;
  
BEGIN

  -- Check if the results.module_results table already contains data for the given in_mod_job_id:
  records_found := count(mr.*) FROM results.module_results AS mr WHERE mr.mod_job_id = in_mod_job_id;
  IF records_found IS NOT NULL AND records_found > 0 THEN
    
    RAISE EXCEPTION 'Table results.module_results already contains data for mod_job_id %', in_mod_job_id;
    
  END IF;

  -- initialize the strings: 
  colname_string := '';
  max_string     := '';
  case_string    := '';
  
  FOR score IN (
    SELECT 
      score_name
    FROM tmp.module_results_tmp
    WHERE mod_job_id = in_mod_job_id
    GROUP BY score_name
  )
  LOOP
    
    colname_string = colname_string ||score||', ';

    max_string = max_string || 'max('||score||') AS '||score||', ';

    case_string = case_string || 'CASE WHEN mrt.score_name = '''||score||''' THEN score_value ELSE NULL END AS '||score||', ';
  
  END LOOP;
  
  -- Trim strings:
  colname_string := trim(TRAILING ', ' FROM colname_string);
  max_string     := trim(TRAILING ', ' FROM max_string);
  case_string    := trim(TRAILING ', ' FROM case_string);

  IF colname_string = '' THEN
    
    RAISE EXCEPTION 'No scores found for mod_job_id %', in_mod_job_id;
    
  END IF;

  
  -- Combine results from all the use cases and write them to results.module_results:
  sql_string :=
 'INSERT INTO results.module_results (
    mod_job_id, 
    alias_id,' ||
    colname_string || '
  ) 
  SELECT '||
    in_mod_job_id || ' AS mod_job_id, 
    alias_id, ' ||
    max_string || '
  FROM
  (
    SELECT
      mrt.alias_id, ' ||
      case_string || '
    FROM tmp.module_results_tmp AS mrt
    WHERE mrt.mod_job_id = ' || in_mod_job_id ||'
  ) a
  GROUP BY alias_id;';

  EXECUTE sql_string;

  PERFORM core.analyze('results.module_results', in_mod_job_id);

  RETURN (
    SELECT count(mr.*)
    FROM results.module_results AS mr
    WHERE mr.mod_job_id = in_mod_job_id
  );

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.combine_results(integer) OWNER TO xsl;




--------------------------------------------------------------------------------------------------


-------------------------Result verification-----------------------------------------------------



CREATE OR REPLACE FUNCTION results.verify_results(in_mod_job_id integer, use_case text, in_chart text[])
RETURNS void AS
$BODY$

/* SUMMARY
 * This function calls the use case-specific result verification function verify_results_<use_case_name>.
 *
 * INPUTS
 * in_mod_job_id : Identifier of module job
 * use_case       : Name of the use case
 * in_chart      : Array of chart names
 *                 'roc' = receiver operating characteristics curve
 *                 'lift' = lift chart (non-cumulative)
 *                 'cumulative_gains' = cumulative gains chart
 *                 'cumulative_gains_ideal' = cumulative gains chart of ideal model
 * 
 * VERSION
 * 15.01.2013 HMa - Multiple use case support: modified to call use case-specific result verification functions. 
 * 25.02.2010 TSi - Bug 137 fixed
 * 23.02.2010 TSi
 */
 
 DECLARE
  uc_model_id integer;
  query text;

BEGIN
  -- Fetch model ID from module job parameters
  query := 'SELECT mjp.value FROM work.module_job_parameters AS mjp WHERE mjp.mod_job_id = '||in_mod_job_id||' AND mjp.key = ''uc_'||use_case||'_model_id'';';
  EXECUTE query INTO uc_model_id;

  IF use_case = 'churn_inactivity' THEN
    PERFORM results.verify_results_churn(in_mod_job_id, uc_model_id, 'inactivity', in_chart);
  ELSIF use_case = 'churn_postpaid' THEN
    PERFORM results.verify_results_churn(in_mod_job_id, uc_model_id, 'postpaid', in_chart);
  ELSIF use_case = 'product' THEN
    PERFORM results.verify_results_product(in_mod_job_id, uc_model_id, in_chart);
  ELSIF use_case = 'zero_day_prediction' THEN
    PERFORM results.verify_results_zero_day_prediction(in_mod_job_id, uc_model_id, in_chart);
  ELSE
    RAISE EXCEPTION 'Use case % not supported in result verification.', use_case;
  END IF;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.verify_results(in_mod_job_id integer, use_case text, in_chart text[]) OWNER TO xsl;


--------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION results.verify_results_churn(in_mod_job_id integer, uc_model_id integer, churn_type text, in_chart text[])
RETURNS void AS
$BODY$

/* SUMMARY
 * This function calls the function results.insert_result_verification_charts to 
 * verify churn propensity scores in results.module_results table by comparing
 * them against true values in work.module_targets table. The verification results go
 * to results.module_results_verification table.
 *
 * INPUTS
 * in_mod_job_id : Identifier of module job
 * uc_model_id   : Identifier of use case model
 * churn_type    : churn type; 'inactivity' or 'postpaid'
 * in_chart      : Array of chart names
 *                 'roc' = receiver operating characteristics curve
 *                 'lift' = lift chart (non-cumulative)
 *                 'cumulative_gains' = cumulative gains chart
 *                 'cumulative_gains_ideal' = cumulative gains chart of ideal model
 * 
 * VERSION
 * 05.02.2013 HMa 
 */

DECLARE
  
  target_name text;

BEGIN

  target_name = 'churn_' || churn_type;
  PERFORM results.insert_result_verification_charts(
    in_mod_job_id, 
    in_mod_job_id,
    uc_model_id, 
    'churn_prediction',
    'target_churn_' || churn_type,
    1,
    ARRAY['churn_' || churn_type ||'_propensity_score'],
    in_chart,
    ''
  );

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.verify_results_churn(in_mod_job_id integer, uc_model_id integer, churn_type text, in_chart text[]) OWNER TO xsl;


--------------------------------------------------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION results.verify_results_product(in_mod_job_id integer, in_model_id integer, in_chart text[])
RETURNS void AS
$BODY$

/* SUMMARY
 * This function calls the function results.insert_result_verification_charts to verify 
 * the product propensity scores in results.module_results table by comparing
 * them against true values in work.module_targets table. The verification results go
 * to results.module_results_verification table.
 *
 * INPUTS
 * in_mod_job_id : Identifier of module job
 * uc_model_id   : Identifier of use case model
 * in_chart      : Array of chart names
 *                 'roc' = receiver operating characteristics curve
 *                 'lift' = lift chart (non-cumulative)
 *                 'cumulative_gains' = cumulative gains chart
 *                 'cumulative_gains_ideal' = cumulative gains chart of ideal model
 * 
 * VERSION
 * 05.02.2013 HMa
 */

DECLARE

  model_ids              integer[];
  uc_model_id            integer;
  target_name            text;
  is_submodel_list       text;
  product_uc             text;

  i                      integer;

BEGIN

  -- Check if in_model id is a submodel list: 
  SELECT "value" = 'submodel_list' FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_type' INTO is_submodel_list;

  IF is_submodel_list THEN -- Insert submodel ids to the model id array
    model_ids := array(SELECT "value"::integer FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_id');
  ELSE
    model_ids := ARRAY[in_model_id]; -- Insert model id to the model id array
  END IF;

  FOR i IN 1..array_upper(model_ids,1) 
  LOOP

    uc_model_id := model_ids[i];
    product_uc  := "value" FROM work.module_models WHERE model_id = uc_model_id AND "key" = 'use_case';
   
    PERFORM results.insert_result_verification_charts(
    in_mod_job_id, 
    in_mod_job_id,
    uc_model_id, 
    product_uc,
    'target_' || product_uc,
    1,
    ARRAY[product_uc || '_propensity_score'],
    in_chart,
    ''
  );

  END LOOP;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.verify_results_product(in_mod_job_id integer, in_model_id integer, in_chart text[]) OWNER TO xsl;



------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.verify_results_zero_day_prediction(in_mod_job_id integer, in_model_id integer, in_chart text[])
RETURNS void AS
$BODY$

/* SUMMARY
 * This function calls the function results.insert_result_verification_charts_zero_day_prediction
 * to verify the value segment propensity scores in results.module_results table by comparing
 * them against true values in work.module_targets table. The verification results go
 * to results.module_results_verification table.
 *
 * INPUTS
 * in_mod_job_id : Identifier of module job
 * uc_model_id   : Identifier of use case model
 * in_chart      : Array of chart names
 *                 'roc' = receiver operating characteristics curve
 *                 'lift' = lift chart (non-cumulative)
 *                 'cumulative_gains' = cumulative gains chart
 *                 'cumulative_gains_ideal' = cumulative gains chart of ideal model
 * 
 * VERSION
 * 28.02.2013 JVi modified so that submodel results are pooled in results.module_results_verification table under head model id
 * 20.02.2013 JVi Tested, results appear ok based on quick look
 * 09.02.2013 JVi (Not yet tested)
 */

DECLARE

  model_ids              integer[];
  target_name            text;
  is_submodel_list       text;

  i                      integer;
  tenure_group           integer;

BEGIN

  -- Low value segment
  PERFORM results.insert_result_verification_charts(
    in_mod_job_id, 
    in_mod_job_id,
    in_model_id, 
    'zero_day_prediction',
    'target_zero_day_prediction',
    1,
    ARRAY['zero_day_prediction_low_value_propensity_score',
          'zero_day_prediction_medium_value_propensity_score',
          'zero_day_prediction_high_value_propensity_score'],
    in_chart,
    ''
  );
  
  -- Medium value segment
  PERFORM results.insert_result_verification_charts(
    in_mod_job_id, 
    in_mod_job_id,
    in_model_id, 
    'zero_day_prediction',
    'target_zero_day_prediction',
    2,
    ARRAY['zero_day_prediction_low_value_propensity_score',
          'zero_day_prediction_medium_value_propensity_score',
          'zero_day_prediction_high_value_propensity_score'],
    in_chart,
    ''
  );
  
  -- High value segment
  PERFORM results.insert_result_verification_charts(
    in_mod_job_id, 
    in_mod_job_id,
    in_model_id, 
    'zero_day_prediction',
    'target_zero_day_prediction',
    3,
    ARRAY['zero_day_prediction_low_value_propensity_score',
          'zero_day_prediction_medium_value_propensity_score',
          'zero_day_prediction_high_value_propensity_score'],
    in_chart,
    ''
  );

  -- Alternative implementation possibility: If submodels are used, their results 
  -- are inserted separately (would need to modify results.insert_result_verification_charts)
  
END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.verify_results_zero_day_prediction(integer, integer, text[]) OWNER TO xsl;


------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.insert_result_verification_charts(
  in_mod_job_id_target integer, 
  in_mod_job_id_apply integer, 
  uc_model_id integer, 
  use_case text,
  target_name text, 
  target_value int, 
  score_names text[], 
  in_chart text[],
  chart_ext text
)
RETURNS void AS
$BODY$

/* SUMMARY
 * This function verifies propensity scores in results.module_results table by comparing
 * them against true values in work.module_targets table. The verification results go
 * to results.module_results_verification table. For charts ordered by score, the score bin is
 * stored in the field results.module_results_verification.id.
 *
 * The function handles also cases where the model is multinomial, and when the module_results and
 * module_targets tables are from different mod jobs, and when the use case name may be different
 * from other names. For this reason, there are several input variables.
 *
 * For multinomial models, creating a cumgain chart with target_value = x will produce a cumgain 
 * chart with labels 'target_x_share_of_target_1' ... 'target_x_share_of_target_N' for each bin.
 *
 * INPUTS
 * in_mod_job_id_target : Identifier of fitting mod job (to read work.module_targets information)
 *
 * in_mod_job_id_apply  : Identifier of apply mod job (to read work.module_results information).                        
 *                        If target and apply job ids are different, only subscribers in table 
 *                        work.target_control_groups with tcg.target = 'cg' and 
 *                        tcg.mod_job_id = in_mod_job_id_apply are used for verification.
 *
 * uc_model_id   : Identifier of use case model
 *
 * use_case      : Name of the use case (e.g. churn_inactivity, zero_day_prediction) in the
 *                 work.target_control_groups table. Only used if in_mod_job_id_target != in_mod_job_id_apply.
 *
 * target_name   : Name of the target column. The function reads the column <target_name> 
 *                 from the table work.module_targets. Example: 'target_churn_inactivity', 'target_product_product_x'.
 *                 Also the corresponding audience column name is derived from this (e.g. 'target_X' -> 'audience_X'.
 *
 * target_value  : Value of target variable in the <target_name> column for which lift and cumgain should be 
 *                 calculated. For a typical logistic model, target_value = 1. For a multinomial model, target_value 
 *                 should be an integer from 1 to n.
 *
 * score_names   : Single-value model: Text array with a single element corresponding to name of the score column in 
 *                                     results.module_results by which the subscribers should be ordered.
 *
 *                 Multinomial model:  Text array containing names of all relevant score columns, so that
 *                                     score_names[target_value] is the column name corresponding to target_value. 
 *                                     This allows calculating cumulative gain data for different classes (e.g. 
 *                                     for displaying cumulative gain of medium value subscribers when score list 
 *                                     is ordered by low value score).
 *
 * in_chart      : Array of chart names
 *                 'roc' = receiver operating characteristics curve
 *                 'lift' = lift chart (non-cumulative)
 *                 'cumulative_gains' = cumulative gains chart
 *                 'cumulative_gains_ideal' = cumulative gains chart of ideal model
 *
 * chart_ext     : Text string to append to chart name. For example, if chart_spec = '_apply', the field "chart"
 *                 in the results.module_results_verification table will get values like 'lift_apply'.
 *
 * EXAMPLE USAGE 1
 *
 *  SELECT * FROM results.insert_result_verification_charts(25, 37, 69, 'churn_prediction', 
 *   'target_churn_prediction', 1, ARRAY['churn_inactivity_propensity_score'], 
 *   ARRAY['roc','lift','cumulative_gains'], '_apply');
 *
 * EXAMPLE USAGE 2
 *
 *  SELECT * FROM results.insert_result_verification_charts(25, 25, 69, 'zero_day_prediction', 'target_zero_day_prediction', 1,
 *   ARRAY['zero_day_prediction_low_value_propensity_score', 'zero_day_prediction_medium_value_propensity_score', 
 *         'zero_day_prediction_high_value_propensity_score'], ARRAY['roc','lift','cumulative_gains'], '');
 *
 *
 * VERSION
 * 20.11.2013 JVi Added additional checks that should prevent crash when no approved target list is found
 * 22.08.2013 JVi Combined three functions into one. For this, had to add some input parameters
 *                and make some changes to the chart data format in the 
 *                results.module_results_verification table and views using the table
 * 05.02.2013 HMa
 * Modified from results.verify_results (25.02.2010 TSi)
 */

DECLARE
  lift_numbins           integer := 100;
  numsample              integer := 999;
  gapsample              double precision;

  sum_target integer;
  avg_target double precision;
  num_target integer;

  query                  text;
  cg_block               text;
  source_table_block     text;
  target_name_block      text;
  
  is_multinomial         boolean;
  n_classes              integer;
  class_counts_record    record;
  class_counts_string    text;

BEGIN

    -- Check whether multinomial model is used
    n_classes = array_upper(score_names, 1);
    IF n_classes > 1 THEN is_multinomial = TRUE; ELSE is_multinomial = FALSE; END IF;
    
    -- If target and apply mod job ids are different, the cg_block allows using
    -- only alias_ids who belong to the control group in the apply run.
    -- Control group is defined as those who are tagged with 'cg' in the 
    -- target list that has the largest target list id and for which
    -- mod job id = mod_job_id_target.
    IF in_mod_job_id_target = in_mod_job_id_apply THEN
      cg_block = '';
    ELSE
      cg_block = '
        JOIN work.target_control_groups tcg
        ON  tcg.target = ''cg''
        AND tcg.mod_job_id = mr.mod_job_id
        AND tcg.alias_id = mt.alias_id
        JOIN (
          SELECT t.mod_job_id, max(t.target_id) AS target_id 
          FROM work.target_control_groups AS t
          INNER JOIN results.approved_target_lists AS a -- only approved target lists
          ON t.target_id = a.target_id
          WHERE t.use_case = '''||use_case||'''
          GROUP BY t.mod_job_id
        ) b
        ON tcg.mod_job_id = b.mod_job_id 
        AND tcg.target_id = b.target_id       
      ';
    END IF;
    
    -- The following block is reused several times in this function.
    -- It contains the query that finds the subscribers to use in chart calculation.
    source_table_block := '
      work.module_targets AS mt
      INNER JOIN results.module_results AS mr
      USING (alias_id)
      '|| cg_block ||'
      WHERE mt.mod_job_id = '||in_mod_job_id_target||'
      AND mr.mod_job_id = '||in_mod_job_id_apply||'
      AND mt.'||regexp_replace(target_name, 'target_', 'audience_')||' IS NOT NULL
      AND mr.'||score_names[target_value]||' IS NOT NULL
    ';

    
    -- Target name block
    -- This is used with multinomial models to distinguish charts 
    -- calculated for differrent multinomial classes.
    -- It is appended to the beginning of the "key" field.
    IF is_multinomial THEN
      target_name_block = 'target_' || target_value || '_';
    ELSE
      target_name_block = '';
    END IF;
      

    -- General statistics
    -- Count and share of positive targets in whole population,
    -- include both subscribers who have target value assigned to them
    -- and those with target value NULL
    query := '
      SELECT
        sum(CASE WHEN COALESCE(mt.'||target_name||','||target_value||') = '||target_value||' THEN 1 ELSE 0 END),
        avg(CASE WHEN COALESCE(mt.'||target_name||','||target_value||') = '||target_value||' THEN 1 ELSE 0 END),
        count(mt.*)
      FROM '||source_table_block||';
      ';
    
    IF query IS NULL THEN
      RAISE NOTICE 'Unable to calculate general statistics for use case %', use_case;
    ELSE
      EXECUTE query INTO sum_target, avg_target, num_target;
    END IF;
    
    -- Counts of different multinomial classes
    -- A string is formed of form '25302,23455,23409' and used later when
    -- calculating cumulative gain
    IF is_multinomial THEN
      query := 'SELECT ';
      
      FOR i IN 1..n_classes LOOP
        query := query ||
        'SUM(CASE WHEN class = '||i||' THEN count_class ELSE NULL END) ||'',''||';
      END LOOP;
      
      query := TRIM(TRAILING '||'',''||' FROM query);
      
      query := query || ' AS counts
        FROM (
          SELECT class, COUNT(*) AS count_class
          FROM (
            SELECT generate_series(1, '||n_classes||')) classes (class)
            INNER JOIN (SELECT * FROM ' || source_table_block || ') stb
          ON classes.class = stb.'||target_name||'
          GROUP BY classes.class
        ) a
       ';

      IF query IS NULL THEN
        RAISE NOTICE 'Unable to calculate multinomial class counts for use case %', use_case;
      ELSE
        EXECUTE query INTO class_counts_record;
        SELECT INTO class_counts_string class_counts_record.counts;
      END IF;

    END IF;

    -- Reduce parameters if there are too few data points
    lift_numbins := least(num_target, lift_numbins);
    numsample := least(num_target, numsample);
    gapsample := (num_target - 1)::double precision / (numsample - 1)::double precision;

    -- Lift chart
    
    IF array['lift'] && in_chart THEN

    -- The lift chart query is almost identical in for multinomial and 
    -- non-multinomial models. The only difference is in the target_name_block.
      query :=
     'INSERT INTO results.module_results_verification
      (mod_job_id, model_id, chart, id, key, value)
      SELECT
        '||in_mod_job_id_apply||' AS mod_job_id,
        '||uc_model_id||' AS model_id, 
        ''lift'' || '''||chart_ext||''' AS chart,
        vals.group_id AS id, 
        '''|| target_name_block ||''' || keys.label AS key,
        CASE
          WHEN keys.label = ''group_target_avg_per_global_target_avg'' THEN vals.grp_avg_per_glb_avg
          WHEN keys.label = ''count'' THEN vals.grp_count
          WHEN keys.label = ''score_min'' THEN vals.grp_min_score
          WHEN keys.label = ''score_max'' THEN vals.grp_max_score
          WHEN keys.label = ''score_avg'' THEN vals.grp_avg_score
          WHEN keys.label = ''target_avg'' THEN vals.grp_avg_target
        END AS value
      FROM (
        SELECT ''group_target_avg_per_global_target_avg'' AS label
        UNION SELECT ''count'' AS label
        UNION SELECT ''score_min'' AS label
        UNION SELECT ''score_max'' AS label
        UNION SELECT ''score_avg'' AS label
        UNION SELECT ''target_avg'' AS label
      ) AS keys
      CROSS JOIN (
        SELECT
          c.group_id,
          avg(c.target) / '||avg_target||' AS grp_avg_per_glb_avg,
          avg(c.target) AS grp_avg_target,
          count(c.*) AS grp_count,
          min(c.'||score_names[target_value]||') AS grp_min_score,
          max(c.'||score_names[target_value]||') AS grp_max_score,
          avg(c.'||score_names[target_value]||') AS grp_avg_score
        FROM (
          SELECT
            mt.alias_id,
            ceiling(('||lift_numbins||' * row_number() OVER (ORDER BY mr.'||score_names[target_value]||' DESC))::double precision / '||num_target||'::double precision) AS group_id,
            CASE WHEN COALESCE(mt.'||target_name||','||target_value||') = '||target_value||' THEN 1 ELSE 0 END AS target,
            mr.'||score_names[target_value]||'
          FROM '||source_table_block||'
        ) AS c
        GROUP BY c.group_id
      ) AS vals;';
      
      IF query IS NULL THEN
        RAISE NOTICE 'Unable to calculate lift chart for use case %', use_case;
      ELSE
        EXECUTE query;
      END IF;
  
    END IF;
  
    -- ROC curve & cumulative gains chart
    -- There is hardly any work for the other chart if one is required. Let's
    -- compute both charts in any case and delete the other later, if required.
    IF array['roc', 'cumulative_gains'] && in_chart THEN
  
      -- Starting point for ROC
      INSERT INTO results.module_results_verification
      (mod_job_id, model_id, chart, id, key, value)  
      VALUES
      (in_mod_job_id_apply, uc_model_id, 'roc' || chart_ext, 1, target_name_block || 'share_of_positives', 0.0),
      (in_mod_job_id_apply, uc_model_id, 'roc' || chart_ext, 1, target_name_block || 'share_of_negatives', 0.0);
      
      -- Starting point for cumulative gain
      -- For multinomial model, separate entries are stored for each class
      -- so that for each bin, we have the gain for all classes.
      IF is_multinomial THEN
        FOR i IN 1..n_classes LOOP
          INSERT INTO results.module_results_verification
          (mod_job_id, model_id, chart, id, key, value)  
          VALUES
          (in_mod_job_id_apply, uc_model_id, 'cumulative_gains' || chart_ext, 1, 'target_' || target_value || '_share_of_target_' || i, 0.0);
        END LOOP;
      ELSE
        INSERT INTO results.module_results_verification
        (mod_job_id, model_id, chart, id, key, value)  
        VALUES
        (in_mod_job_id_apply, uc_model_id, 'cumulative_gains' || chart_ext, 1, 'share_of_positives', 0.0),
        (in_mod_job_id_apply, uc_model_id, 'cumulative_gains' || chart_ext, 1, 'share_of_all', 0.0);
      END IF;  
  
      -- Rest of the points
      -- First, "chart metadata"
      query := '
        INSERT INTO results.module_results_verification
        (mod_job_id, model_id, chart, id, key, value)
        SELECT
          '||in_mod_job_id_apply||' AS mod_job_id,
          '||uc_model_id||' AS model_id, 
          keys.chart || '''||chart_ext||''' AS chart,
          vals_sample.row_id + 1 AS id,
          keys.label AS key,
       ';
     
      -- Then, actual values - separate treatment of multinomial and non-multinomial models
      -- Values are queried from table "keys cross join vals_sample".
      
      IF is_multinomial THEN
        query := query || 'CASE 
            WHEN keys.chart = ''roc'' AND keys.label = ''target_'|| target_value ||'_share_of_positives'' THEN vals_sample.share_'||target_value||'
            WHEN keys.chart = ''roc'' AND keys.label = ''target_'|| target_value ||'_share_of_negatives'' THEN vals_sample.share_negatives
          ';
        FOR i IN 1..n_classes LOOP 
          query := query || '
            WHEN keys.chart = ''cumulative_gains'' AND keys.label = ''target_'|| target_value ||'_share_of_target_'' || '||i||' THEN vals_sample.share_'||i||'
           ';
        END LOOP;
        query := query || '
            ELSE NULL 
            END AS value
           ';
      ELSE
        query := query || '
        CASE 
          WHEN keys.chart = ''roc'' AND keys.label = ''share_of_positives'' THEN vals_sample.share_positives
          WHEN keys.chart = ''roc'' AND keys.label = ''share_of_negatives'' THEN vals_sample.share_negatives
          WHEN keys.chart = ''cumulative_gains'' AND keys.label = ''share_of_positives'' THEN vals_sample.share_positives
          WHEN keys.chart = ''cumulative_gains'' AND keys.label = ''share_of_all'' THEN vals_sample.share_all
        ELSE NULL
        END AS value
        ';
      END IF;

      -- Form table keys, which contains chart names and labels      
      IF is_multinomial THEN
        query := query || ' 
          FROM (
          SELECT       ''roc'' AS chart, ''target_'|| target_value ||'_share_of_positives'' AS label
          UNION SELECT ''roc'' AS chart, ''target_'|| target_value ||'_share_of_negatives'' AS label
        ';
        FOR i IN 1..n_classes LOOP query := query || '
            UNION SELECT ''cumulative_gains'' AS chart, ''target_'|| target_value ||'_share_of_target_'' || '||i||' AS label
          ';
        END LOOP;
        query := query || '
          ) AS keys
        ';
      ELSE
        query := query || '
          FROM (
            SELECT       ''roc'' AS chart,              ''share_of_positives'' AS label
            UNION SELECT ''roc'' AS chart,              ''share_of_negatives'' AS label
            UNION SELECT ''cumulative_gains'' AS chart, ''share_of_positives'' AS label
            UNION SELECT ''cumulative_gains'' AS chart, ''share_of_all'' AS label
          ) AS keys
        ';
      END IF;
      
      -- Form table vals_sample which contains the actual values
      -- This is done in parts to accomodate arbitrary numbers of multinomial classes
      query := query || '
        CROSS JOIN (
          SELECT
            row_number() OVER (ORDER BY vals.row_id_from_zero ASC) AS row_id,
            (vals.row_id_from_zero + 1.0) / '||num_target||' AS share_all,
            vals.share_positives,
            vals.share_negatives,';

      IF is_multinomial THEN
        FOR i IN 1..n_classes LOOP query := query || ' 
          vals.share_'||i||',';
        END LOOP;
      END IF;
      
      query := TRIM(TRAILING ',' FROM query);
      
      -- In the following, for non-multinomial models, also target value NULL is 
      -- is counted as positive. For multinomial models, only the correct target value counts.
      query := query || '  
        FROM (
          SELECT
            ((row_number() OVER (ORDER BY mr.'||score_names[target_value]||' DESC)) - 1)::double precision AS row_id_from_zero,
            (sum(CASE WHEN NOT '||CASE WHEN is_multinomial THEN 'true' ELSE 'false' END||'::boolean AND COALESCE(mt.'||target_name||','||target_value||') = '||target_value||' THEN 1 
                      WHEN     '||CASE WHEN is_multinomial THEN 'true' ELSE 'false' END||'::boolean AND mt.'||target_name||' = '||target_value||' THEN 1 
                      ELSE 0 
                      END 
            ) OVER (ORDER BY mr.'||score_names[target_value]||' DESC))::double precision / '||sum_target||' AS share_positives,
            (sum(CASE WHEN mt.'||target_name||' != '||target_value||' THEN 1 ELSE 0 END ) OVER (ORDER BY mr.'||score_names[target_value]||' DESC))::double precision / ('||num_target||' - '||sum_target||') AS share_negatives,';

      IF is_multinomial THEN
        FOR i IN 1..n_classes LOOP query := query || '
          (sum(CASE WHEN mt.'||target_name||' = '||i||' THEN 1 ELSE 0 END ) OVER (ORDER BY mr.'||score_names[target_value]||' DESC))::double precision / '||split_part(class_counts_string, ',', i)||'::numeric AS share_'||i||',';
        END LOOP;
      END IF;

      query := TRIM(TRAILING ',' FROM query);
       
      query := query || '
          FROM '||source_table_block||'
        ) AS vals
        WHERE vals.row_id_from_zero - floor(vals.row_id_from_zero / '||gapsample||') * '||gapsample||' < 1 -- Take evenly spaced samples including the first and last
      ) AS vals_sample;';
            
      IF query IS NULL THEN
        RAISE NOTICE 'Unable to calculate ROC and cumgain charts for use case %', use_case;
      ELSE
        EXECUTE query;
      END IF;
  
      -- Delete ROC curve if it is not required
      IF NOT array['roc'] && in_chart THEN
  
        DELETE FROM results.module_results_verification AS mrv
        WHERE mrv.mod_job_id = in_mod_job_id
        AND mrv.chart LIKE 'roc%';
  
      END IF;
  
      -- Delete cumulative gains chart if it is not required
      IF NOT array['cumulative_gains'] && in_chart THEN
  
        DELETE FROM results.module_results_verification AS mrv
        WHERE mrv.mod_job_id = in_mod_job_id
        AND mrv.chart LIKE 'cumulative_gains%';
  
      END IF;
    
    END IF;
  
    -- Ideal cumulative gains chart
    IF array['cumulative_gains_ideal'] && in_chart THEN
      
      INSERT INTO results.module_results_verification
      (mod_job_id, model_id, chart, id, key, value)  
      VALUES
      (in_mod_job_id_apply, uc_model_id, 'cumulative_gains_ideal' || chart_ext, 1, target_name_block || 'share_of_positives', 0.0),
      (in_mod_job_id_apply, uc_model_id, 'cumulative_gains_ideal' || chart_ext, 1, target_name_block || 'share_of_all', 0.0),
      (in_mod_job_id_apply, uc_model_id, 'cumulative_gains_ideal' || chart_ext, 2, target_name_block || 'share_of_positives', 1.0),
      (in_mod_job_id_apply, uc_model_id, 'cumulative_gains_ideal' || chart_ext, 2, target_name_block || 'share_of_all', avg_target),
      (in_mod_job_id_apply, uc_model_id, 'cumulative_gains_ideal' || chart_ext, 3, target_name_block || 'share_of_positives', 1.0),
      (in_mod_job_id_apply, uc_model_id, 'cumulative_gains_ideal' || chart_ext, 3, target_name_block || 'share_of_all', 1.0);
  
    END IF;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.insert_result_verification_charts(
  in_mod_job_id_target integer, 
  in_mod_job_id_apply integer, 
  uc_model_id integer, 
  use_case text,
  target_name text, 
  target_value int, 
  score_names text[], 
  in_chart text[],
  chart_ext text
) OWNER TO xsl;


--------------------------------------------------------------------------------------------------


------------------------------------Create modelling data--------------------------------------------------------------



CREATE OR REPLACE FUNCTION work.create_modelling_data1(in_mod_job_id integer)
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function processes modelling variables that use data.in_split_aggregates as
 * source table. If model_id is available in work.module_job_parameters table, then
 * only the variables that the model use are processed. Otherwise, all variables
 * are processed. Results go to work.modelling_data_matrix_1.
 *
 * INPUT
 * Identifier of module job
 *
 * OUTPUT
 * Query by which this function has inserted data into
 * work.modelling_data_matrix_1 table
 *
 * VERSION
 * 04.06.2014 HMa - ICIF-181 Stack removal
 * 06.03.2013 MOJ - ICIF-112
 * 25.01.2013 HMa - Read model ids of all the use cases
 * 15.01.2013 HMa - ICIF-97
 * 14.06.2010 TSi - Bug 188 fixed
 * 19.04.2010 JMa
 */

DECLARE

  t1_date date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1'
  );
  t2_date date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  override_model_variables_check boolean := (
    SELECT coalesce(lower(trim(max(mjp.value))) = 'true', FALSE)
    FROM work.module_job_parameters AS mjp 
    WHERE mjp.mod_job_id = in_mod_job_id 
    AND mjp.key = 'override_model_variables_check'
  );

  all_var_names text[];
  included_var_names text[];
  model_ids record;  
  t1_week text := core.date_to_yyyyww(t1_date);
  t2_week text := core.date_to_yyyyww(t2_date);
  num_of_weeks double precision := (t2_date - t1_date)::double precision / 7.0;
  aggr_block text := '';
  var_names text := '';
  aggr_new text;
  query text;
  n text;

BEGIN

  -- Find model ids of all the use cases in this module job:
  SELECT 
    mp.value 
  INTO model_ids 
  FROM work.module_job_parameters AS mp
  WHERE mp.mod_job_id = in_mod_job_id 
  AND mp.key LIKE '%model_id';

  all_var_names :=  array_agg(columns)
    FROM  work.get_table_columns_with_prefix('work','modelling_data_matrix_1','','mod_job_id,alias_id'); 


  IF model_ids IS NULL OR override_model_variables_check THEN
    included_var_names := all_var_names;
  ELSE
    query := 
    'SELECT array_agg(DISTINCT mm.value) AS var_name
     FROM work.module_models AS mm
     WHERE mm.value IN ('''||array_to_string(all_var_names, ''',''')||''')
     AND mm.key = ''varname''
     AND mm.model_id IN (
       SELECT 
         mp.value::integer AS model_id
       FROM work.module_job_parameters AS mp
       WHERE mp.mod_job_id = '||in_mod_job_id||'
       AND mp.key LIKE ''%model_id''
       UNION ALL
       SELECT DISTINCT mm3.value::integer
       FROM work.module_models AS mm2
       INNER JOIN work.module_models AS mm3
       ON mm2.model_id = mm3.model_id
       WHERE mm2.key = ''model_type''
       AND mm2.value = ''submodel_list''
       AND mm3.key = ''model_id'' 
       AND mm2.model_id IN (
         SELECT 
           mp.value::integer AS model_id
         FROM work.module_job_parameters AS mp
         WHERE mp.mod_job_id = '||in_mod_job_id||'
         AND mp.key LIKE ''%model_id''
       )
     );';
    EXECUTE query INTO included_var_names;
  END IF;

  n := array_upper(included_var_names, 1);

  IF n = '0' THEN
    RETURN 'WARNING: No variables processed';
  END IF;

  -- Parse some auxiliary texts
  FOR n IN (
    SELECT included_var_names[s] FROM generate_series(1,array_upper(included_var_names, 1)) AS s
  ) LOOP
    aggr_new := (SELECT CASE
      WHEN n IN (
        'voicecount', 'voicesum', 'voicecountweekday', 'voicesumweekday',
        'voicecountday', 'voicesumday', 'voicecountevening', 'voicesumevening',
        'smscount', 'smscountweekday', 'smscountday', 'smscountevening'
      ) THEN 
        'sum(sa.'||n||')::double precision / '||num_of_weeks||' AS '||n
      WHEN n = 'alias_count' THEN
        'sum(sa.mc_'||n||')::double precision / '||num_of_weeks||' AS '||n
      WHEN n = 'mr_ratio' THEN 
        'CASE
           WHEN sum(sa.rc_alias_count) > 0
           THEN avg(sa.mc_alias_count)::double precision / avg(sa.rc_alias_count)
           ELSE NULL
         END AS mr_ratio'
      WHEN n = 'mr_count_ratio' THEN
        'CASE
           WHEN sum(sa.rc_voicecount + sa.rc_smscount) > 0
           THEN avg(sa.voicecount + sa.smscount)::double precision / avg(sa.rc_voicecount + sa.rc_smscount)
           ELSE NULL
         END AS mr_count_ratio'
      WHEN n = 'callsmsratio' THEN
        'CASE
           WHEN sum(sa.smscount) > 0
           THEN avg(sa.voicecount)::double precision / avg(sa.smscount)
           ELSE NULL
         END AS callsmsratio'
      WHEN n = 'week_mean' THEN
        'CASE
           WHEN sum(sa.weight_of_week) > 0
           THEN sum(sa.weight_of_week * sa.period_scaled)::double precision / sum(sa.weight_of_week)
           ELSE NULL
         END AS week_mean'
      WHEN n = 'week_entropy' AND num_of_weeks > 1 THEN
        'CASE
           WHEN sum(sa.weight_of_week) > 0
           THEN (ln(sum(sa.weight_of_week)) - sum(sa.weight_of_week * sa.ln_weight_of_week)::double precision / sum(sa.weight_of_week)) / ln('||num_of_weeks||')
           ELSE NULL
         END AS week_entropy'
      ELSE NULL
    END);
    IF aggr_new IS NOT NULL THEN
      var_names := var_names || n || ', ';
      aggr_block := aggr_block||aggr_new||', ';
    END IF;
  END LOOP;

  -- Fine-tune the auxiliary texts
  var_names := trim(TRAILING ', ' FROM var_names);
  aggr_block := trim(TRAILING ', ' FROM aggr_block);

  -- Parse the query (part I)
  query :=
    'INSERT INTO work.modelling_data_matrix_1
     (mod_job_id, alias_id, '||var_names||')
     SELECT 
       '||in_mod_job_id||' AS mod_job_id,
        sa.alias_id, 
       '||aggr_block||'
     FROM (
       SELECT 
         sa1.*,       
         sa1.voicecount + sa1.smscount AS weight_of_week,
         ln(CASE WHEN sa1.voicecount + sa1.smscount <= 0 THEN 1.0 ELSE sa1.voicecount + sa1.smscount END) AS ln_weight_of_week,
         (core.yyyyww_to_date(sa1.period) - to_date('''||to_char(t1_date, 'YYYY-MM-DD')||''', ''YYYY-MM-DD''))::double precision / '||(t2_date - t1_date - 7)|| ' AS period_scaled
       FROM data.in_split_aggregates AS sa1
       INNER JOIN work.module_targets AS mt
       ON sa1.alias_id = mt.alias_id
       WHERE mt.mod_job_id = '||in_mod_job_id||'
       AND sa1.period >= '||t1_week||' 
       AND sa1.period < '||t2_week||'
     ) AS sa
     GROUP BY sa.alias_id';


  -- Execute and return the query, do some maintenance in between
  EXECUTE query;
  PERFORM core.analyze('work.modelling_data_matrix_1', in_mod_job_id);
  RETURN query;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data1(integer) OWNER TO xsl;




CREATE OR REPLACE FUNCTION work.create_modelling_data2(in_mod_job_id integer)
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function processes modelling variables that are aggregates over
 * neighbour data. If model_id is available in work.module_job_parameters table, then
 * only the variables that the model use are processed. Otherwise, all variables
 * are processed. Results go to work.modelling_data_matrix_2. 
 *
 * INPUT
 * Identifier of module job
 *
 * OUTPUT
 * Query by which this function has inserted data into
 * work.modelling_data_matrix_2 table
 *
 * VERSION
 * 04.06.2014 HMa - ICIF-181 Stack removal
 * 13.05.2013 KL  - Added secondary network knn_2 to modelling_variables and data
 * 06.03.2013 MOJ - ICIF-112
 * 17.01.2013 HMa - Removed job_type 
 * 15.01.2013 HMa - ICIF-97
 * 22.02.2012 TSi - Bug 841 fixed
 * 17.10.2011 TSi - Bug 742 fixed
 * 01.10.2010 TSi - Bugs 225 and 175 fixed
 * 16.09.2010 TSi - Bug 208 fixed
 * 14.06.2010 TSi - Bug 188 fixed
 * 19.04.2010 TSi - Bug 177 fixed
 * 19.04.2010 JMa
 */

DECLARE

  t1_text text := (
    SELECT DISTINCT 'to_date('''||mjp.value||''', ''YYYY-MM-DD'')'
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 't1'
  );
  t2_text text := (
    SELECT DISTINCT 'to_date('''||mjp.value||''', ''YYYY-MM-DD'')'
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 't2'
  );
  t1_week text := (
    SELECT DISTINCT core.date_to_yyyyww(mjp.value::date)::text
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 't1'
  );
  t2_week text := (
    SELECT DISTINCT core.date_to_yyyyww(mjp.value::date)::text
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 't2'
  );
  source_period_length integer := (
    SELECT
      max(CASE WHEN lower(mjp.key) = 't2' THEN mjp.value::date ELSE NULL END)
      - max(CASE WHEN lower(mjp.key) = 't1' THEN mjp.value::date ELSE NULL END)
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id
  );
  tcrm_text text := (
    SELECT DISTINCT 'to_date('''||mjp.value||''', ''YYYY-MM-DD'')'
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'tcrm'
  );
  net_job_id integer := (
    SELECT DISTINCT mjp.value::integer
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'xsl_job_id'
  );
  net_job_id_2 integer := (
    SELECT DISTINCT mjp.value::integer
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'xsl_job_id_2'
  );
  override_model_variables_check boolean := (
    SELECT coalesce(lower(trim(max(mjp.value))) = 'true', FALSE)
    FROM work.module_job_parameters AS mjp 
    WHERE mjp.mod_job_id = in_mod_job_id 
    AND mjp.key = 'override_model_variables_check'
  );
  model_ids record;  
  all_var_names text[];
  included_var_names text[];
  aggr_block text := '';
  var_names text := '';
  aggr_new text;
  query text;
  n text;

BEGIN

  -- Find model ids of all the use cases in this module job:
  SELECT 
    mp.value 
  INTO model_ids 
  FROM work.module_job_parameters AS mp
  WHERE mp.mod_job_id = in_mod_job_id 
  AND mp.key LIKE '%model_id';

  all_var_names :=  array_agg(columns) 
    FROM  work.get_table_columns_with_prefix('work','modelling_data_matrix_2','','mod_job_id,alias_id'); 


  IF model_ids IS NULL OR override_model_variables_check THEN
    included_var_names := all_var_names;
  ELSE
    query := 
    'SELECT array_agg(DISTINCT mm.value) AS var_name
     FROM work.module_models AS mm
     WHERE mm.value IN ('''||array_to_string(all_var_names, ''',''')||''')
     AND mm.key = ''varname''
     AND mm.model_id IN (
       SELECT 
         mp.value::integer AS model_id
       FROM work.module_job_parameters AS mp
       WHERE mp.mod_job_id = '||in_mod_job_id||'
       AND mp.key LIKE ''%model_id''
       UNION ALL
       SELECT DISTINCT mm3.value::integer
       FROM work.module_models AS mm2
       INNER JOIN work.module_models AS mm3
       ON mm2.model_id = mm3.model_id
       WHERE mm2.key = ''model_type''
       AND mm2.value = ''submodel_list''
       AND mm3.key = ''model_id'' 
       AND mm2.model_id IN (
         SELECT 
           mp.value::integer AS model_id
         FROM work.module_job_parameters AS mp
         WHERE mp.mod_job_id = '||in_mod_job_id||'
         AND mp.key LIKE ''%model_id''
       )
     );';
    EXECUTE query INTO included_var_names;
  END IF;

  n := array_upper(included_var_names, 1);

  IF n = '0' THEN
    RETURN 'WARNING: No variables processed';
  END IF;


  -- Parse some auxiliary texts
  FOR n IN (
    SELECT included_var_names[s] FROM generate_series(1,array_upper(included_var_names, 1)) AS s
  ) LOOP
    aggr_new := (SELECT CASE
      WHEN n = 'k_offnet' THEN
        'sum((1 - f.in_out_network) * b.weight)::double precision / greatest(sum(b.weight), 1e-16) AS k_offnet'
      WHEN n = 'k_target' THEN
        'avg(c.source_period_inactivity) AS k_target'
      WHEN n = 'knn' THEN
        'round(avg(e.k)) AS knn'
      WHEN n = 'knn_2' THEN
        'round(avg(ee.k)) AS knn_2'
      ELSE NULL
    END);
    IF aggr_new IS NOT NULL THEN
      var_names := var_names||n||', ';
      aggr_block := aggr_block||aggr_new||', ';
    END IF;
  END LOOP;

  -- Fine-tune the auxiliary texts
  var_names := trim(TRAILING ', ' FROM var_names);
  aggr_block := trim(TRAILING ', ' FROM aggr_block);

  -- Parse the query
  query := 
      'INSERT INTO work.modelling_data_matrix_2
      (mod_job_id, alias_id, '||var_names||')
       SELECT 
         '||in_mod_job_id||' AS mod_job_id,
          b.alias_a AS alias_id, 
         '||aggr_block||'
       FROM work.module_targets AS a 
       INNER JOIN work.out_network AS b
       ON b.alias_a = a.alias_id
       LEFT JOIN (
         SELECT 
           sa.alias_id, 
           1 - (core.yyyyww_to_date(max(sa.period))-'||t1_text||')::double precision / '||source_period_length-7||'::double precision AS source_period_inactivity
         FROM data.in_split_aggregates AS sa
         WHERE sa.in_out_network = 1
         AND sa.voicecount + sa.smscount > 0
         AND sa.period >= '||t1_week||' 
         AND sa.period <  '||t2_week||'
         GROUP BY sa.alias_id
       ) AS c
       ON c.alias_id = b.alias_b
       LEFT JOIN work.out_scores AS e 
       ON e.alias_id = b.alias_b
       AND e.job_id = '||net_job_id||'
       LEFT JOIN work.out_scores AS ee
       ON ee.alias_id = b.alias_b
       AND ee.job_id = '||net_job_id_2||'
       LEFT JOIN work.aliases AS f
       ON f.alias_id = b.alias_b
       AND f.job_id = '||net_job_id||'
       WHERE b.job_id = '||net_job_id||'
       AND a.mod_job_id = '||in_mod_job_id||'
       GROUP BY b.alias_a';

  -- Execute and return the query, do some maintenance in between
  EXECUTE query;
  PERFORM core.analyze('work.modelling_data_matrix_2', in_mod_job_id);
  RETURN query;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data2(integer) OWNER TO xsl;

-----------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.create_modelling_data3(in_mod_job_id integer)
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function processes modelling variables that can be found from some
 * existing table without massive processing such as aggregating. If model_id is
 * available in work.module_job_parameters table, then only the so-called necessary
 * variables and the variables that the model use are processed. Otherwise, all
 * variables are processed. Results go to work.modelling_data_matrix_3.
 *
 * INPUT
 * Identifier of module job
 *
 * OUTPUT
 * Query by which this function has inserted data into
 * work.modelling_data_matrix_3 table
 *
 * VERSION
 * 04.06.2014 HMa - ICIF-181 Stack removal
 * 13.05.2013 KL  - Added secondary network plus monthly_arpu to modelling_variables + data
 * 06.03.2013 MOJ - ICIF-112
 * 15.01.2013 HMa - Removed variable 'target'
 * 15.01.2013 HMa - ICIF-97
 * 14.09.2012 MOJ - ICIF-65 fixed
 * 22.02.2012 TSi - Bug 841 fixed
 * 13.10.2011 MOj - Bug 739 fixed
 * 16.09.2011 TSi - Bug 230 and 429 fixed
 * 11.02.2011 MOj - Bug 431 fixed
 * 03.02.2011 MOj - Bug 415 fixed
 * 26.01.2011 TSi - Bug 352 fixed
 * 14.06.2010 TSi - Bug 188 fixed
 * 19.04.2010 JMa
 */

DECLARE
  t2_text text := (
    SELECT DISTINCT 'to_date('''||mjp.value||''', ''YYYY-MM-DD'')'
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 't2'
  );
  tcrm_text text := (
    SELECT DISTINCT 'to_date('''||mjp.value||''', ''YYYY-MM-DD'')'
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'tcrm'
  );
  net_job_id integer := (
    SELECT DISTINCT mjp.value::integer
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'xsl_job_id'
  );
  net_job_id_2 integer := (
    SELECT DISTINCT mjp.value::integer
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'xsl_job_id_2'
  );
  override_model_variables_check boolean := (
    SELECT coalesce(lower(trim(max(mjp.value))) = 'true', FALSE)
    FROM work.module_job_parameters AS mjp 
    WHERE mjp.mod_job_id = in_mod_job_id 
    AND mjp.key = 'override_model_variables_check'
  );
  all_var_names text[];
  necessary_var_names text[] := array[ -- Add here variables that you want to become processed in any case
    'k',
    'separate_node'
  ];
  included_var_names text[];
  model_ids record;  
  aggr_block text := '';
  var_names text := '';
  aggr_new text;
  query text;
  tenure_query text;
  n text;

BEGIN

  -- Find model ids of all the use cases in this module job:
  SELECT 
    mp.value 
  INTO model_ids 
  FROM work.module_job_parameters AS mp
  WHERE mp.mod_job_id = in_mod_job_id 
  AND mp.key LIKE '%model_id';

  all_var_names := array_agg(columns) 
    FROM  work.get_table_columns_with_prefix('work','modelling_data_matrix_3','','mod_job_id,alias_id');   

     

  IF model_ids IS NULL OR override_model_variables_check THEN
    included_var_names := all_var_names;
  ELSE
    query := 
      'SELECT array_agg(DISTINCT z.var_name)
       FROM (
         SELECT mm.value AS var_name
         FROM work.module_models AS mm
         WHERE mm.value IN ('''||array_to_string(all_var_names, ''',''')||''') 
         AND mm.key = ''varname''
         AND mm.model_id IN (
           SELECT 
           mp.value::integer AS model_id
           FROM work.module_job_parameters AS mp
           WHERE mp.mod_job_id = '||in_mod_job_id||'
           AND mp.key LIKE ''%model_id''
           UNION ALL
           SELECT DISTINCT mm3.value::integer
           FROM work.module_models AS mm2
           INNER JOIN work.module_models AS mm3
           ON mm2.model_id = mm3.model_id
           WHERE mm2.key = ''model_type''
           AND mm2.value = ''submodel_list''
           AND mm3.key = ''model_id'' 
           AND mm2.model_id IN (
             SELECT 
               mp.value::integer AS model_id
             FROM work.module_job_parameters AS mp
             WHERE mp.mod_job_id = '||in_mod_job_id||'
             AND mp.key LIKE ''%model_id''
           )
         )
         UNION ALL SELECT '''||array_to_string(necessary_var_names, ''' AS var_name UNION ALL SELECT ''')||''' AS var_name
       ) AS z;';
    EXECUTE query INTO included_var_names;
  END IF;

  n := array_upper(included_var_names, 1);

  IF n = '0' THEN
    RETURN 'WARNING: No variables processed';
  END IF;


  -- Parse some auxiliary texts
  FOR n IN (
    SELECT included_var_names[s] FROM generate_series(1,array_upper(included_var_names, 1)) AS s
  ) LOOP
    aggr_new := (SELECT CASE
      WHEN n IN (
        'gender', 'subscriber_value', 'tariff_plan', 'handset_model', 'country', 'language',
        'subscriber_segment', 'flg', 'zip', 'churn_score', 'subscription_type', 'payment_type'
      ) THEN 
        'crm.'||n||' AS '||n
      WHEN n = 'age' THEN 
        '('||t2_text||' - crm.birth_date)::double precision / 365.0 AS age'        
      WHEN n = 'handset_age' THEN 
        '('||t2_text||' - crm.handset_begin_date)::double precision / 30.0 AS handset_age'
      WHEN n = 'contr_length' THEN
        '('||t2_text||' - crm.switch_on_date::date)::double precision / 30.0 AS contr_length'
      WHEN n = 'contr_remain' THEN
        '(crm.binding_contract_start_date + crm.binding_contract_length - ' || t2_text || ')::double precision / 30.0 AS contr_remain'
      WHEN n = 'no_churn_score' THEN
        'CASE
           WHEN crm.churn_score IS NULL THEN 1
           ELSE 0
         END AS no_churn_score'
      WHEN n IN ('alpha', 'k', 'c', 'wec', 'kshell', 'socrev', 'socrevest') THEN
        'os.'||n||' AS '||n
      WHEN n IN ('alpha_2', 'k_2', 'c_2', 'wec_2', 'kshell_2', 'socrev_2', 'socrevest_2') THEN
        'os_2.'||trim(trailing '_2' from n)||' AS '||n
      WHEN n = 'separate_node' THEN
        'CASE
           WHEN os.k = 0 OR os.k IS NULL THEN 1 
           ELSE 0 
         END AS separate_node'
      WHEN n = 'monthly_arpu' THEN
        'ma.'||n||' AS monthly_arpu'
      ELSE NULL
    END);
    IF aggr_new IS NOT NULL THEN
      var_names := var_names||n||', ';
      aggr_block := aggr_block||aggr_new||', ';
    END IF;
  END LOOP;

  -- Fine-tune the auxiliary texts
  var_names := trim(TRAILING ', ' FROM var_names);
  aggr_block := trim(TRAILING ', ' FROM aggr_block);

  -- Parse the query
  query :=
    'INSERT INTO work.modelling_data_matrix_3
     (mod_job_id, alias_id, '||var_names||')
     SELECT 
      '||in_mod_job_id||' AS mod_job_id,
       mt.alias_id, 
      '||aggr_block||'
     FROM work.module_targets AS mt
     LEFT JOIN 
       work.monthly_arpu ma
     ON mt.alias_id = ma.alias_id
     AND ma.mod_job_id='||in_mod_job_id||'
     LEFT JOIN (
       SELECT crm1.* 
       FROM data.in_crm AS crm1
       WHERE crm1.date_inserted = '||tcrm_text||'
     ) AS crm
     ON mt.alias_id = crm.alias_id
     LEFT JOIN (
       SELECT 
         os1.alias_id,
         os1.alpha,
         os1.k,
         os1.c,
         os1.wec,
         os1.kshell,
         os1.socrev,
         os1.socrevest
       FROM work.out_scores AS os1
       WHERE os1.job_id = '||net_job_id||'
     ) AS os
     ON mt.alias_id = os.alias_id     
     LEFT JOIN (
       SELECT os1.*
       FROM work.out_scores AS os1
       WHERE os1.job_id = '||net_job_id_2||'
     ) AS os_2
     ON mt.alias_id = os_2.alias_id
     WHERE mt.mod_job_id = '||in_mod_job_id;

 -- query for tenure_days segmentation  
  tenure_query :=
    'INSERT INTO work.tenure_days 
     (mod_job_id, alias_id, tenure_days, segmentation)
     SELECT 
      '||in_mod_job_id||' AS mod_job_id,
       mdt.alias_id, 
       ceiling(mdt.contr_length * 30) AS tenure_days,
	   CASE WHEN ceiling(mdt.contr_length * 30)  < 30 AND ceiling(mdt.contr_length * 30)  >= 0  THEN ''Less than a month''
            WHEN ceiling(mdt.contr_length * 30) >= 30 AND ceiling(mdt.contr_length * 30)  <  91  THEN ''One to three months''
            WHEN ceiling(mdt.contr_length * 30) >= 92 AND ceiling(mdt.contr_length * 30)  <  365 THEN ''Three months to one year''
            WHEN ceiling(mdt.contr_length * 30) >= 365  THEN ''More than one year''
		   ELSE NULL END
           AS segmentation  	
      FROM work.modelling_data_matrix_3 AS mdt
      WHERE mdt.mod_job_id = '||in_mod_job_id;
	 
  -- Execute and return the query, do some maintenance in between
  EXECUTE query;  
  PERFORM core.analyze('work.modelling_data_matrix_3', in_mod_job_id);
  EXECUTE tenure_query;
  RETURN query;

END;



$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data3(integer) OWNER TO xsl;

--------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.create_modelling_data4_made(in_mod_job_id integer, in_lag_length integer, in_lag_count integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates time-lagged aggregates for different made call
 * types. data.call_types_weekly is used as source table. Results go to
 * work.modelling_data_matrix_4_made.
 *
 * INPUT
 * in_mod_job_id : Identifier of module job
 * in_lag_length : Length of one lag unit in days
 * in_lag_count  : Number of lag units
 *
 * VERSION
 * 2014-06-04 HMa - ICIF-181 Stack removal (modified from work.create_modelling_data4)
 * 2013-01-07 JVi - Commented out low-utility predictors using --070113 to conserve
 *                  computing power. Find and Replace '--070113' to '' to take 
 *                  these predictors into use. (e.g. if data includes lots of mms and video).
 *                  Also need to remove similar comments from work.modelling_data_matrix
 * 2012-12-21 JVi - Added new predictors based on HMa's x81 specific version
 * 2011-09-16 TSi - Bug 230 fixed
 * 2011-09-13 TSi - Bug 393 fixed
 */

DECLARE

  t2 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  lag1_date date := (t2 - (in_lag_count * in_lag_length || ' days')::interval)::date;
  lag2_date date := t2;
  call_type_map record;
  call_direction_made character(1) := 'm';
  case_text text;
  var_names text;
  query text;
  
BEGIN

  -- Change this if the identifiers of call types differ from the default values
  SELECT 1 AS voice, 2 AS sms, 3 AS video, 4 AS mms, 5 AS "data" INTO call_type_map;

/* -------------- Start processing made calls. -------------- */
  
  SELECT
    array_to_string(array_agg(case_t),' '),
    array_to_string(array_agg(var_n),', ')
  INTO case_text, var_names
  FROM (
    SELECT
      'MAX(CASE WHEN lag_id = ' || lag_id || ' THEN ' || base_name || ' END) AS ' || b.base_name || c.lag_id || ', ' AS case_t,
       b.base_name || c.lag_id AS var_n
    FROM (
      SELECT 'daily_voice_activity'       AS base_name UNION ALL
      SELECT 'daily_sms_activity'         AS base_name UNION ALL
      SELECT 'daily_data_activity'        AS base_name UNION ALL
      SELECT 'weekly_data_usage'          AS base_name UNION ALL 
      SELECT 'weekly_voice_neigh_count'   AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_voice_count'         AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_sms_count'           AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_data_count'          AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_voice_duration'      AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_voice_cost'          AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_sms_cost'            AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_cost'                AS base_name UNION ALL -- New variable 2012-12-21 JVi
      SELECT 'weekly_cell_id_count'       AS base_name           -- New variable 2012-12-21 JVi
    ) AS b
    CROSS JOIN (
      SELECT generate_series(1, in_lag_count) AS lag_id
    ) AS c
    UNION
    SELECT
      'MIN(inact) AS inact, ' AS case_t,
       'inact' AS var_n
  ) AS d;

  case_text := trim(TRAILING ', ' FROM case_text);

  query := '
  INSERT INTO work.modelling_data_matrix_4_made
  (mod_job_id, alias_id, '||var_names||')
  SELECT
    '||in_mod_job_id||' AS mod_job_id,
    vals.alias_id,
    '||case_text||'
  FROM (
    SELECT
      d.alias_id,
      d.lag_id,
      (sum(d.voice_daycount)        OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision       AS daily_voice_activity, -- proportion of days when has had voice activity
      (sum(d.sms_daycount)          OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision       AS daily_sms_activity,
      (sum(d.data_daycount)         OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision       AS daily_data_activity,
      (sum(d.sum_data_usage)        OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_data_usage, -- average weekly data amount (data)
      (sum(d.sum_voice_neigh_count) OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_voice_neigh_count, -- average number of alias_b weekly (voice)
      (sum(d.sum_voice_count)       OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_voice_count, -- average number of transactios weekly 
      (sum(d.sum_sms_count)         OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_sms_count,
      (sum(d.sum_data_count)        OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_data_count,
      (sum(d.voice_duration)        OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_voice_duration, -- average voice duration daily
      (sum(d.voice_cost)            OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_voice_cost, -- average cost weekly (voice)
      (sum(d.sms_cost)              OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_sms_cost,
      (sum(d.sum_cost)              OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_cost, 
      (sum(d.sum_cell_id_count)     OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_cell_id_count, -- average weekly cell id count
      (extract(''days'' FROM inact) + extract(''hours'' FROM inact) / 24.0 + extract(''minutes'' FROM inact) / 1440.0 + extract(''seconds'' FROM inact) / 86400.0) AS inact
    FROM (
      SELECT
        mt.alias_id,
        lags.lag_id,
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'   THEN tt.day_count         ELSE NULL END) AS voice_daycount,        -- activity days proportion
        sum(CASE WHEN tt.call_type = '||call_type_map.sms||'     THEN tt.day_count         ELSE NULL END) AS sms_daycount,
        sum(CASE WHEN tt.call_type = '||call_type_map.data||'    THEN tt.day_count         ELSE NULL END) AS data_daycount,
        sum(CASE WHEN tt.call_type = '||call_type_map.data||'    THEN tt.sum_call_duration ELSE NULL END) AS sum_data_usage,        -- data usage (data)
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'   THEN tt.neigh_count       ELSE NULL END) AS sum_voice_neigh_count, -- neighbor count 
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'   THEN tt.row_count         ELSE NULL END) AS sum_voice_count,       -- transaction count (voice)
        sum(CASE WHEN tt.call_type = '||call_type_map.sms||'     THEN tt.row_count         ELSE NULL END) AS sum_sms_count, 
        sum(CASE WHEN tt.call_type = '||call_type_map.data||'    THEN tt.row_count         ELSE NULL END) AS sum_data_count, 
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'   THEN tt.sum_call_duration ELSE NULL END) AS voice_duration,        -- duration (voice only)
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'   THEN tt.sum_call_cost     ELSE NULL END) AS voice_cost,            -- cost (voice)
        sum(CASE WHEN tt.call_type = '||call_type_map.sms||'     THEN tt.sum_call_cost     ELSE NULL END) AS sms_cost, 
        sum(tt.sum_call_cost)                                                                             AS sum_cost,              -- cost (all calltypes)
        sum(tt.cell_id_count)                                                                             AS sum_cell_id_count,      -- cell_id count, all calltypes
        justify_hours('''||t2||'''::timestamp without time zone - max(tt.max_call_time))                  AS inact
      FROM work.module_targets AS mt     
      CROSS JOIN (
        SELECT generate_series(1, '||in_lag_count||') AS lag_id
      ) AS lags
      LEFT JOIN (
        SELECT
          ctw.alias_id,
          ceiling(('''||t2||'''::date - ctw.monday)::double precision / '||in_lag_length||')::integer AS lag_id,
          ctw.call_type,
          ctw."row_count", 
          ctw.day_count,
          ctw.neigh_count,
          ctw.sum_parameter1,
          ctw.sum_call_duration, 
          ctw.sum_call_cost,
          ctw.cell_id_count,
          ctw.max_call_time
        FROM data.call_types_weekly AS ctw
        WHERE (
          ctw.call_type = '||call_type_map.voice||' OR
          ctw.call_type = '||call_type_map.sms||' OR
          ctw.call_type = '||call_type_map.video||' OR
          ctw.call_type = '||call_type_map.mms||' OR
          ctw.call_type = '||call_type_map.data||'
        )
        AND ctw.monday < '''||lag2_date||'''::date
        AND ctw.monday >= '''||lag1_date||'''::date
        AND ctw.direction = '''||call_direction_made||'''
      ) AS tt
      ON mt.alias_id = tt.alias_id
      AND lags.lag_id = tt.lag_id
      WHERE mt.mod_job_id = '||in_mod_job_id||'
      GROUP BY mt.alias_id, lags.lag_id
    ) AS d   
  ) AS vals
  GROUP BY vals.alias_id;';

  EXECUTE query;

  PERFORM core.analyze('work.modelling_data_matrix_4_made', in_mod_job_id);

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data4_made(integer, integer, integer) OWNER TO xsl;






CREATE OR REPLACE FUNCTION work.create_modelling_data4_rec(in_mod_job_id integer, in_lag_length integer, in_lag_count integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates time-lagged aggregates for different received call
 * types. data.call_types_weekly is used as source table. Results go to
 * work.modelling_data_matrix_4_rec.
 *
 * INPUT
 * in_mod_job_id : Identifier of module job
 * in_lag_length : Length of one lag unit in days
 * in_lag_count  : Number of lag units
 *
 * VERSION
 * 2014-06-05 HMa - ICIF-181 Stack removal (modified from work.create_modelling_data4)
 * 2013-01-07 JVi - Commented out low-utility predictors using --070113 to conserve
 *                  computing power. Find and Replace '--070113' to '' to take 
 *                  these predictors into use. (e.g. if data includes lots of mms and video).
 *                  Also need to remove similar comments from work.modelling_data_matrix
 * 2012-12-21 JVi - Added new predictors based on HMa's x81 specific version
 * 2011-09-16 TSi - Bug 230 fixed
 * 2011-09-13 TSi - Bug 393 fixed
 */

DECLARE

  t2 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  lag1_date date := (t2 - (in_lag_count * in_lag_length || ' days')::interval)::date;
  lag2_date date := t2;
  call_type_map record;
  call_direction_received character(1) := 'r';
  case_text text;
  var_names text;
  query text;
  
BEGIN

  -- Change this if the identifiers of call types differ from the default values
  SELECT 1 AS voice, 2 AS sms, 3 AS video, 4 AS mms, 5 AS "data" INTO call_type_map;


  SELECT
    array_to_string(array_agg(case_t),' '),
    array_to_string(array_agg(var_n),', ')
  INTO case_text, var_names
  FROM (
    SELECT
      'MAX(CASE WHEN lag_id = ' || lag_id || ' THEN ' || base_name || ' END) AS ' || b.base_name || c.lag_id || ', ' AS case_t,
       b.base_name || c.lag_id AS var_n
    FROM (
      SELECT 'daily_voice_activity_rec'    AS base_name UNION ALL
      SELECT 'daily_sms_activity_rec'      AS base_name UNION ALL
      SELECT 'weekly_voice_count_rec'      AS base_name UNION ALL
      SELECT 'weekly_sms_count_rec'        AS base_name UNION ALL 
      SELECT 'weekly_voice_duration_rec'   AS base_name UNION ALL 
      SELECT 'weekly_cell_id_count_rec'    AS base_name
    ) AS b
    CROSS JOIN (
      SELECT generate_series(1, in_lag_count) AS lag_id
    ) AS c
    UNION
    SELECT
      'MIN(inact_rec) AS inact_rec, ' AS case_t,
       'inact_rec' AS var_n
  ) AS d;

  case_text := trim(TRAILING ', ' FROM case_text);



  query := '
  INSERT INTO work.modelling_data_matrix_4_rec
  (mod_job_id, alias_id, '||var_names||')
  SELECT
    '||in_mod_job_id||' AS mod_job_id,
    vals.alias_id,
    '||case_text||'
  FROM (
    SELECT
      d.alias_id,
      d.lag_id,
      (sum(d.voice_daycount)        OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision       AS daily_voice_activity_rec, -- proportion of days when has had voice activity
      (sum(d.sms_daycount)          OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision       AS daily_sms_activity_rec,
      (sum(d.sum_voice_count)       OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_voice_count_rec, -- average number of transactios weekly 
      (sum(d.sum_sms_count)         OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_sms_count_rec,
      (sum(d.voice_duration)        OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_voice_duration_rec, -- average voice duration daily
      (sum(d.sum_cell_id_count)     OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC)) / ('||in_lag_length||' * d.lag_id)::double precision * 7.0 AS weekly_cell_id_count_rec,  -- average weekly cell id count
      (extract(''days'' FROM inact_rec) + extract(''hours'' FROM inact_rec) / 24.0 + extract(''minutes'' FROM inact_rec) / 1440.0 + extract(''seconds'' FROM inact_rec) / 86400.0 ) AS inact_rec
     FROM (
      SELECT
        mt.alias_id,
        lags.lag_id,
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'  THEN tt.day_count           ELSE NULL END) AS voice_daycount, -- activity days proportion
        sum(CASE WHEN tt.call_type = '||call_type_map.sms||'    THEN tt.day_count           ELSE NULL END) AS sms_daycount,
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'  THEN tt.row_count           ELSE NULL END) AS sum_voice_count, -- transaction count (voice)
        sum(CASE WHEN tt.call_type = '||call_type_map.sms||'    THEN tt.row_count           ELSE NULL END) AS sum_sms_count, 
        sum(CASE WHEN tt.call_type = '||call_type_map.voice||'  THEN tt.sum_call_duration   ELSE NULL END) AS voice_duration, -- duration (voice only)
        sum(tt.cell_id_count)                                                                              AS sum_cell_id_count, -- cell_id count, all calltypes
        justify_hours('''||t2||'''::timestamp without time zone - max(tt.max_call_time))                   AS inact_rec
      FROM work.module_targets AS mt     
      CROSS JOIN (
        SELECT generate_series(1, '||in_lag_count||') AS lag_id
      ) AS lags
      LEFT JOIN (
        SELECT
          ctw.alias_id,
          ceiling(('''||t2||'''::date - ctw.monday)::double precision / '||in_lag_length||')::integer AS lag_id,
          ctw.call_type,
          ctw."row_count", 
          ctw.day_count,
          ctw.neigh_count,
          ctw.sum_parameter1,
          ctw.sum_call_duration, 
          ctw.cell_id_count,
          ctw.max_call_time
        FROM data.call_types_weekly AS ctw
        WHERE (
          ctw.call_type = '||call_type_map.voice||' OR
          ctw.call_type = '||call_type_map.sms||' 
        )
        AND ctw.monday < '''||lag2_date||'''::date
        AND ctw.monday >= '''||lag1_date||'''::date
        AND ctw.direction = '''||call_direction_received||'''
      ) AS tt
      ON mt.alias_id = tt.alias_id
      AND lags.lag_id = tt.lag_id
      WHERE mt.mod_job_id = '||in_mod_job_id||'
      GROUP BY mt.alias_id, lags.lag_id
    ) AS d   
  ) AS vals
  GROUP BY vals.alias_id;';

  EXECUTE query;

  PERFORM core.analyze('work.modelling_data_matrix_4_rec', in_mod_job_id);

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data4_rec(integer, integer, integer) OWNER TO xsl;




--------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.create_modelling_data_topup1(in_mod_job_id integer, in_lag_length integer, in_lag_count integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates time-lagged aggregates from data.toupup table. 
 * Results go to work.modelling_data_matrix_topup1. 
 * 
 * INPUT
 * in_mod_job_id : Identifier of module job
 * in_lag_length : Length of one lag unit in days
 * in_lag_count  : Number of lag units
 *
 * VERSION
 * 2014-06-05 HMa - ICIF-181 Stack removal (modified from work.create_modelling_data5 and work.create_modelling_data6)
 * 2012-05-14 MOj - Bug 888 fixed
 * 2011-09-15 TSi - Bug 394 and 528 fixed
 */
DECLARE

  t2 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  lag1_time timestamp without time zone := (t2 - (in_lag_count * in_lag_length || ' days')::interval)::timestamp without time zone;
  lag2_time timestamp without time zone := t2::timestamp without time zone;
  is_credit_amount boolean := (
    SELECT count(d.*) > 0
    FROM (
      SELECT *
      FROM data.topup AS t
      WHERE NOT t.is_credit_transfer
      AND t.timestamp < lag2_time
      AND t.timestamp >= lag1_time
      AND coalesce(t.credit_amount, 0) > 0
      LIMIT 1
    ) AS d
  );
  is_topup_cost boolean := (
    SELECT count(d.*) > 0
    FROM (
      SELECT *
      FROM data.topup AS t
      WHERE NOT t.is_credit_transfer
      AND t.timestamp < lag2_time
      AND t.timestamp >= lag1_time
      AND coalesce(t.topup_cost, 0) > 0
      LIMIT 1
    ) AS d
  );
  case_text text;
  var_names text;
  query text;

BEGIN

  IF NOT (is_credit_amount OR is_topup_cost) THEN
    RETURN;
  END IF;


  SELECT
    array_to_string(array_agg(case_t),' '),
    array_to_string(array_agg(var_n),', ')
  INTO case_text, var_names
  FROM (
    SELECT
      'MAX(CASE WHEN lag_id = ' || lag_id || ' THEN ' || base_name || ' END) AS ' || b.base_name || c.lag_id || ', ' AS case_t,
       b.base_name || c.lag_id AS var_n
    FROM (
      SELECT is_credit_amount                   AS is_available, 'topup_amount_avg'           AS base_name UNION ALL
      SELECT is_credit_amount AND is_topup_cost AS is_available, 'topup_bonus_avg'            AS base_name UNION ALL
      SELECT is_credit_amount AND is_topup_cost AS is_available, 'topup_bonus_per_amount_avg' AS base_name UNION ALL
      SELECT is_credit_amount OR  is_topup_cost AS is_available, 'topup_count_weekly'         AS base_name UNION ALL
      SELECT is_credit_amount AND is_topup_cost AS is_available, 'topup_free_avg'             AS base_name UNION ALL
      SELECT is_credit_amount AND is_topup_cost AS is_available, 'topup_free_count_weekly'    AS base_nam
    ) AS b
    CROSS JOIN (
      SELECT generate_series(1, in_lag_count) AS lag_id
    ) AS c
    WHERE b.is_available
  ) AS d;

  case_text := trim(TRAILING ', ' FROM case_text);


  query := '
  INSERT INTO work.modelling_data_matrix_topup1
  (mod_job_id, alias_id, '||var_names||')
  SELECT
    '||in_mod_job_id||' AS mod_job_id,
    vals.alias_id,
    '||case_text||'
  FROM (
    SELECT
      d.alias_id,
      d.lag_id,
      sum(d.sum_amount)                    OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) / sum(d.sum_count)          OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) AS topup_amount_avg,
      sum(d.sum_bonus_if_bonus)            OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) / sum(d.sum_count_if_bonus) OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) AS topup_bonus_avg,
      sum(d.sum_bonus_per_amount_if_bonus) OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) / sum(d.sum_count_if_bonus) OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) AS topup_bonus_per_amount_avg,
      sum(d.sum_count)                     OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) / ('||in_lag_length||' * d.lag_id) * 7.0                                         AS topup_count_weekly,
      sum(d.sum_free_topup)                OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) / sum(d.count_free_topup)   OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) AS topup_free_avg,
      sum(d.count_free_topup)              OVER (PARTITION BY d.alias_id ORDER BY d.lag_id ASC) / ('||in_lag_length||' * d.lag_id) * 7.0                                         AS topup_free_count_weekly
    FROM (
      SELECT
        mt.alias_id,
        lags.lag_id,
        sum(tt.amount) AS sum_amount,
        sum(tt.count)  AS sum_count,
        sum(CASE WHEN tt.amount > tt.cost THEN tt.amount - tt.cost ELSE NULL END)               AS sum_bonus_if_bonus,
        sum(CASE WHEN tt.amount > tt.cost THEN (tt.amount - tt.cost) / tt.amount ELSE NULL END) AS sum_bonus_per_amount_if_bonus,
        sum(CASE WHEN tt.amount > tt.cost THEN tt.count ELSE NULL END)                          AS sum_count_if_bonus,
        sum(CASE WHEN tt.cost = 0 AND tt.amount > 0 THEN tt.amount ELSE NULL END)               AS sum_free_topup,
        sum(CASE WHEN tt.cost = 0 AND tt.amount > 0 THEN tt.count ELSE NULL END)                AS count_free_topup
      FROM work.module_targets AS mt      
      CROSS JOIN (
        SELECT generate_series(1, '||in_lag_count||') AS lag_id
      ) AS lags
      LEFT JOIN (
        SELECT
          t.charged_id AS alias_id,
          ceiling(('''||t2||'''::date - t.timestamp::date)::double precision / '||in_lag_length||'::double precision)::integer AS lag_id,
          greatest(t.credit_amount, 0)::double precision AS amount,
          greatest(t.topup_cost, 0)::double precision AS cost,
          1::double precision AS "count" -- Use greatest(t.count, 0)::integer if several topups can be reported on the same line
        FROM data.topup AS t
        WHERE NOT t.is_credit_transfer
        AND t.timestamp < '''||lag2_time||'''::date
        AND t.timestamp >= '''||lag1_time||'''::date
      ) AS tt
      ON mt.alias_id = tt.alias_id
      AND lags.lag_id = tt.lag_id
      WHERE mt.mod_job_id = '||in_mod_job_id||'
      GROUP BY mt.alias_id, lags.lag_id
    ) AS d    
  ) AS vals
  GROUP BY vals.alias_id;';

  EXECUTE query;

  PERFORM core.analyze('work.modelling_data_matrix_topup1', in_mod_job_id);

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data_topup1(integer, integer, integer) OWNER TO xsl;






CREATE OR REPLACE FUNCTION work.create_modelling_data_topup2(in_mod_job_id integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates predictors from data.toupup table. 
 * Results go to work.create_modelling_data_topup2. 
 * 
 * INPUT
 * in_mod_job_id : Identifier of module job
 *
 * VERSION
 * 2014-06-05 HMa - ICIF-181 Stack removal (modified from work.create_modelling_data5)
 * 2012-05-14 MOj - Bug 888 fixed
 * 2011-09-15 TSi - Bug 394 and 528 fixed
 */
DECLARE

  t1 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1'
  );
  t2 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  tcrm date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'tcrm'
  );

BEGIN

  INSERT INTO work.modelling_data_matrix_topup2
  (mod_job_id, alias_id, topup_days_from_last, topup_days_to_next1, topup_days_to_next2, topup_days_interval)
  SELECT
    in_mod_job_id AS mod_job_id,
    z.alias_id,
    z.time_from_last1 AS topup_days_from_last,
    CASE
      WHEN z.sum_credit_amount > 0
      THEN z.credit_amount1 * (t2 - greatest(c.switch_on_date, t1)) / z.sum_credit_amount - z.time_from_last1
      ELSE NULL
    END AS topup_days_to_next1, -- This assumes that credit balance was zero at the moment of the last topup
    CASE
      WHEN z.sum_credit_amount > 0
      THEN z.credit_amount2 * (t2 - greatest(c.switch_on_date, t1)) / z.sum_credit_amount - z.time_from_last2
      ELSE NULL
    END AS topup_days_to_next2, -- This assumes that credit balance was zero at the moment of the second last topup
    extract('days' FROM z.avg_interval) 
    + extract('hours' FROM z.avg_interval) / 24.0
    + extract('minutes' FROM z.avg_interval) / 1440.0 
    + extract('seconds' FROM z.avg_interval) / 86400.0 AS topup_days_interval
  FROM (
    SELECT
      d.alias_id,
      sum(CASE WHEN d.row_id < 2 THEN d.credit_amount ELSE 0 END) AS credit_amount1,
      sum(CASE WHEN d.row_id < 3 THEN d.credit_amount ELSE 0 END) AS credit_amount2,
      t2 - max(d.timestamp)::date AS time_from_last1,
      t2 - max(CASE WHEN d.row_id > 1 THEN d.timestamp ELSE NULL END)::date AS time_from_last2,
      sum(d.credit_amount) AS sum_credit_amount,
      justify_hours(avg(d.interval)) AS avg_interval
    FROM (
      SELECT
        mt.alias_id,
        row_number() OVER (PARTITION BY mt.alias_id ORDER BY t.timestamp DESC) AS row_id,
        t.timestamp,
        greatest(t.credit_amount, 0.0) AS credit_amount,
        t.timestamp - lag(t.timestamp, 1, NULL) OVER (PARTITION BY mt.alias_id ORDER BY t.timestamp ASC) AS "interval"
      FROM work.module_targets AS mt
      INNER JOIN data.topup AS t
      ON t.charged_id = mt.alias_id
      WHERE NOT t.is_credit_transfer
      AND t.timestamp < t2
      AND t.timestamp >= t1
      AND mt.mod_job_id = in_mod_job_id
    ) AS d
    GROUP BY d.alias_id
  ) AS z
  LEFT JOIN data.in_crm AS c
  ON z.alias_id = c.alias_id
  WHERE c.date_inserted = tcrm;

  PERFORM core.analyze('work.modelling_data_matrix_topup2', in_mod_job_id);

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data_topup2(integer) OWNER TO xsl;






--------------------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION work.create_modelling_data_topup3(in_mod_job_id integer, in_lag_length integer, in_lag_count integer)
  RETURNS void AS
$BODY$
/* SUMMARY
 * This function calculates predictors from topup behavior during different times of day.
 * Calculation of predictors combining topup and CDR data has been disabled until implemented differently. 
 * Results go to work.modelling_data_matrix_topup3. 
 * 
 * INPUT
 * in_mod_job_id : Identifier of module job
 *
 * VERSION
 * 2014-06-05 HMa - ICIF-181 Stack removal (modified from work.create_modelling_data6)
 * 2012-11-30 JVi - Created from HMa's x81 function, added new topup predictors 
 *                  and removed those shared with create_modelling_data5
 * 2012-09-04 HMa - x81 specific version [of create_modelling_data5 -JVi]
 * 2012-05-14 MOj - Bug 888 fixed
 * 2011-09-15 TSi - Bug 394 and 528 fixed
 */
DECLARE

  t2 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  lag1_time timestamp without time zone := (t2 - (in_lag_count * in_lag_length || ' days')::interval)::timestamp without time zone;
  lag2_time timestamp without time zone := t2::timestamp without time zone;
  case_text text;
  var_names text;
  query text;

BEGIN

  
  /* Temporary table topup_aliases_and_timestamps_temp is used for calculating 
   * some of the topup variables. It contains the alias ids and topup timestamps
   * as well as the lag period id corresponding to each topup timestamp.
   */
  CREATE TEMPORARY TABLE topup_aliases_and_timestamps_temp WITHOUT OIDS AS
  SELECT 
    mt.alias_id AS alias_id,
    lags.lag_id AS lag_id,
    tt.topup_timestamp AS topup_timestamp
  FROM
    work.module_targets AS mt
  CROSS JOIN (
    SELECT generate_series(1, in_lag_count) AS lag_id
  ) AS lags
  INNER JOIN (
    SELECT
      t.receiving_id,
      ceiling((t2 - t.timestamp::date)::double precision / in_lag_length::double precision)::integer AS lag_id,
      t.timestamp AS topup_timestamp
    FROM
      data.topup 
      AS t
    WHERE NOT t.is_credit_transfer
    AND t.timestamp < lag2_time
    AND t.timestamp >= lag1_time
  ) AS tt
  ON mt.alias_id = tt.receiving_id
  AND lags.lag_id = tt.lag_id
  WHERE mt.mod_job_id = in_mod_job_id
  GROUP BY mt.alias_id, lags.lag_id, tt.topup_timestamp;

  

  
  /* ------------ Start calculating time-of-day / time-of-week topup variables. ------------ */
  

  SELECT
    array_to_string(array_agg(case_t),' '),
    array_to_string(array_agg(var_n),', ')
  INTO case_text, var_names
  FROM (
    SELECT
      'MAX(CASE WHEN lag_id = ' || lag_id || ' THEN ' || base_name || ' END) AS ' || b.base_name || c.lag_id || ', ' AS case_t,
       b.base_name || c.lag_id AS var_n
    FROM (
      SELECT 'topup_count_weekly_daytime'    AS base_name UNION ALL
      SELECT 'topup_count_weekly_evening'    AS base_name UNION ALL
      SELECT 'topup_count_weekly_nighttime'  AS base_name UNION ALL
      SELECT 'topup_count_weekly_weekend'    AS base_name 
    ) AS b
    CROSS JOIN (
      SELECT generate_series(1, in_lag_count) AS lag_id
    ) AS c
  ) AS d;

  case_text := trim(TRAILING ', ' FROM case_text);


  query := '
  INSERT INTO work.modelling_data_matrix_topup3
  (mod_job_id, alias_id, '||var_names||')
  SELECT
    '||in_mod_job_id||' AS mod_job_id,
    vals.alias_id,
    '||case_text||'
  FROM (
        SELECT
          tt.alias_id,
          tt.lag_id,
          tt.topup_count_daytime::double precision   / (tt.lag_id * '||in_lag_length||' ) * 7    AS topup_count_weekly_daytime,
          tt.topup_count_evening::double precision   / (tt.lag_id * '||in_lag_length||' ) * 7    AS topup_count_weekly_evening,
          tt.topup_count_nighttime::double precision / (tt.lag_id * '||in_lag_length||' ) * 7    AS topup_count_weekly_nighttime,
          tt.topup_count_weekend::double precision   / (tt.lag_id * '||in_lag_length||' ) * 7    AS topup_count_weekly_weekend
        FROM (
          SELECT 
            alias_id,
            lag_id,
            sum(is_daytime)    OVER (PARTITION BY t.alias_id ORDER BY t.lag_id ASC) AS topup_count_daytime, 
            sum(is_evening)    OVER (PARTITION BY t.alias_id ORDER BY t.lag_id ASC) AS topup_count_evening, 
            sum(is_nighttime)  OVER (PARTITION BY t.alias_id ORDER BY t.lag_id ASC) AS topup_count_nighttime, 
            sum(is_weekend)    OVER (PARTITION BY t.alias_id ORDER BY t.lag_id ASC) AS topup_count_weekend
          FROM (
            SELECT
              alias_id,
              lag_id,
              topup_timestamp,
              CASE WHEN (EXTRACT(dow FROM topup_timestamp) BETWEEN 1 AND 5)  AND  (EXTRACT(hour FROM topup_timestamp)    BETWEEN      7  AND 16)                       THEN 1 ELSE 0 END  AS is_daytime,
              CASE WHEN (EXTRACT(dow FROM topup_timestamp) BETWEEN 1 AND 4)  AND  (EXTRACT(hour FROM topup_timestamp)    BETWEEN     17  AND 21)                       THEN 1 ELSE 0 END  AS is_evening,
              CASE WHEN (EXTRACT(dow FROM topup_timestamp) BETWEEN 1 AND 4)  AND  (EXTRACT(hour FROM topup_timestamp)    NOT BETWEEN  7  AND 21)                       THEN 1 ELSE 0 END  AS is_nighttime,
              CASE WHEN (EXTRACT(dow FROM topup_timestamp) IN (0,6))         OR   (EXTRACT(dow FROM topup_timestamp) = 5 AND EXTRACT(hour FROM topup_timestamp) > 16)  THEN 1 ELSE 0 END  AS is_weekend
              FROM topup_aliases_and_timestamps_temp
          ) AS t
        ) AS tt
    ) AS vals
  GROUP BY vals.alias_id;';

  EXECUTE query;

  PERFORM core.analyze('work.modelling_data_matrix_topup3', in_mod_job_id);

  /* ------------ Finish calculating time-of-day / time-of-week topup variables. ------------ */


  /* ------------ Start calculating statistics combining topup and CDR data. ------------ */
  
/*
  SELECT
    array_to_string(array_agg(case_t),' '),
    array_to_string(array_agg(var_n),', ')
  INTO case_text, var_names
  FROM (
    SELECT
      'MAX(CASE WHEN lag_id = ' || lag_id || ' AND vals.call_type = '|| call_t ||' THEN ' || base_name || ' END) AS ' || b.base_name || '_' || CASE WHEN call_t = 1 THEN 'call' WHEN call_t = 2 THEN 'sms' END || '_avg' || c.lag_id || ', ' AS case_t,
       b.base_name || '_' || CASE WHEN call_t = 1 THEN 'call' WHEN call_t = 2 THEN 'sms' END || '_avg' || c.lag_id AS var_n
    FROM (
      SELECT 1 AS call_t, 'topup_interval_from_last' AS base_name UNION ALL
      SELECT 2 AS call_t, 'topup_interval_from_last' AS base_name UNION ALL
      SELECT 1 AS call_t, 'topup_interval_to_next'   AS base_name UNION ALL
      SELECT 2 AS call_t, 'topup_interval_to_next'   AS base_name 
    ) AS b
    CROSS JOIN (
      SELECT generate_series(1, in_lag_count) AS lag_id
    ) AS c
  ) AS d;

  case_text := trim(TRAILING ', ' FROM case_text);

  
  query := '
  INSERT INTO work.modelling_data_matrix_topup3a
  (mod_job_id, alias_id, '||var_names||')
  SELECT
    '||in_mod_job_id||' AS mod_job_id,
    vals.alias_id,
    '||case_text||'
  FROM (
      SELECT
        tt.alias_id,
        tt.lag_id,
        tt.call_type,
        extract(epoch from avg(tt.topup_interval_from_last_call)) AS topup_interval_from_last,
        extract(epoch from avg(tt.topup_interval_to_next_call))   AS topup_interval_to_next
      FROM (
        SELECT
          t.alias_id,
          t.lag_id,
          t.topup_timestamp,
          c.call_type,
          t.topup_timestamp - max(CASE WHEN c.call_time < t.topup_timestamp THEN c.call_time ELSE NULL END) AS topup_interval_from_last_call,
          min(CASE WHEN c.call_time > t.topup_timestamp THEN c.call_time ELSE NULL END) - t.topup_timestamp AS topup_interval_to_next_call
        FROM 
          topup_aliases_and_timestamps_temp
        AS t
        LEFT JOIN (
          SELECT
            alias_a,
            call_type,
            call_time
            FROM data.cdr
            WHERE call_time >= '''||lag1_time||'''::date
            AND   call_time <  '''||lag2_time||'''::date
          ) AS c
        ON t.alias_id = c.alias_a
        GROUP BY t.alias_id, t.lag_id, t.topup_timestamp, c.call_type
      ) AS tt
      GROUP BY tt.alias_id, tt.lag_id, tt.call_type
    ) AS vals 
  GROUP BY vals.alias_id;';

  EXECUTE query;
*/


  PERFORM core.analyze('work.modelling_data_matrix_topup3a', in_mod_job_id);

  
  /* ------------ Finish calculating statistics combining topup and CDR data. ------------ */



  DROP TABLE IF EXISTS topup_aliases_and_timestamps_temp;
  

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data_topup3(in_mod_job_id integer, in_lag_length integer, in_lag_count integer) OWNER TO xsl;

--------------------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION work.create_modelling_data_topup_channels(in_mod_job_id integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates predictors from the topup channels used. 
 * Results go to work.modelling_data_matrix_topup_channel.
 * The function needs to be customized for each customer (add channels to topup_channels array).  
 * 
 * INPUT
 * in_mod_job_id : Identifier of module job
 *
 * VERSION
 * 2014-06-05 HMa - ICIF-181 Stack removal (modified from work.create_modelling_data5)
 * 2012-05-14 MOj - Bug 888 fixed
 * 2011-09-15 TSi - Bug 394 and 528 fixed
 */
DECLARE

  topup_channels text[] := array[NULL]; -- Add case specific channels here! As default, the frequency profile of topup channels is not computed.
  t1 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1'
  );
  t2 date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  i integer;
  n text;
  agg_text text;
  var_names text; 

BEGIN


  IF array_upper(topup_channels, 1) > 1 OR coalesce(char_length(topup_channels[1]), 0) > 0 THEN

    var_names := '';
    agg_text  := '';

    FOR i IN 1..array_upper(topup_channels, 1) LOOP

      n := regexp_replace(lower(trim(topup_channels[i])), '[^a-z0-9]', '_', 'g');

      var_names := var_names || n || ', ';
      agg_text  := agg_text||'max(CASE WHEN d.topup_channel = '''||n||''' THEN d.count ELSE NULL END) / greatest(sum(d.count), 1e-12) AS profile_'||n||', ';
 
    END LOOP;

    agg_text  := trim(TRAILING ', ' FROM agg_text);
    var_names := trim(TRAILING ', ' FROM var_names);
     
    EXECUTE '
    INSERT INTO work.modelling_data_matrix_topup_channel
    (mod_job_id, alias_id, '||var_names||')
    SELECT 
      '||in_mod_job_id||' AS mod_job_id,
       d.alias_id, 
      '||agg_text||'
    FROM (
      SELECT
        mt.alias_id,
        regexp_replace(lower(trim(t.channel)), ''[^a-z0-9]'', ''_'', ''g'') AS topup_channel,
        count(*)::double precision AS "count"
      FROM work.module_targets AS mt
      INNER JOIN data.topup AS t
      ON t.charged_id = mt.alias_id
      WHERE NOT t.is_credit_transfer
      AND t.timestamp < '''||to_char(t2, 'YYYY-MM-DD HH24:MI:SS')||'''::timestamp without time zone
      AND t.timestamp >= '''||to_char(t1, 'YYYY-MM-DD HH24:MI:SS')||'''::timestamp without time zone
      AND mt.mod_job_id = '||in_mod_job_id||'
      GROUP BY mt.alias_id, topup_channel
    ) AS d
    GROUP BY d.alias_id;';

  END IF;

  PERFORM core.analyze('work.modelling_data_matrix_topup_channel', in_mod_job_id);

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data_topup_channels(integer) OWNER TO xsl;


--------------------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION work.create_modelling_data_handset_model(in_mod_job_id integer, in_lda_output_id integer)
  RETURNS void AS
$BODY$
/*
 * A function to add handset topics obtained from LDA runner to predictors.
 * Reads from work.lda_output
 * Inserts into work.modelling_data_matrix_handset_topic.
 * 
 * Parameters: 
 * in_mod_job_id: module job id
 * in_lda_output_id: LDA output id 
 * 
 * VERSION 
 * 05.06.2014 HMa - ICIF-181 Stack removal
 * 30.11.2012 HMa
 */


DECLARE

  tcrm_date date := (
    SELECT DISTINCT to_date(mjp.value, 'YYYY-MM-DD')
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'tcrm'
  );
  i integer;
  n_topics integer;
  var_names text := '';
  aggr_block text := '';
  query text;

BEGIN

  IF in_lda_output_id = -1 THEN
    RAISE NOTICE 'in_lda_output_id % is not valid, quitting.',in_lda_output_id;
    RETURN;
  END IF;

  SELECT value::integer FROM work.lda_output_parameters WHERE lda_output_id = in_lda_output_id AND key = 'n_topics' INTO n_topics;

  
  FOR i IN 1..(n_topics-1) LOOP -- leave one topic out because of collinearity
    var_names := var_names || 'handset_topic_' || i || ', ';
    aggr_block := aggr_block || 'MAX(CASE WHEN topic = ''handset_topic_' || i || ''' THEN value END) AS handset_topic_' || i || ', '; 
  END LOOP;
  
  var_names  := trim(TRAILING ', ' FROM var_names);
  aggr_block := trim(TRAILING ', ' FROM aggr_block);

  -- Values of new variables added to work.modelling_data (stack)
  query := 
  'INSERT INTO work.modelling_data_matrix_handset_topic
  ( mod_job_id, alias_id, '||var_names||' )
      SELECT
        '||in_mod_job_id||' AS mod_job_id,
        vals.alias_id,    
        '||aggr_block||'
      FROM (
        SELECT 
          mt.alias_id, 
          hmp.topic,
          hmp.value
        FROM work.module_targets AS mt 
        LEFT JOIN 
          data.in_crm AS b
        ON mt.alias_id = b.alias_id
        LEFT JOIN 
          work.lda_output AS hmp
        ON lower(replace(b.handset_model,'' '','''')) = lower(replace(hmp.term,'' '','''')) -- because of possible formatting differences, remove spaces and convert to lowercase
        WHERE mt.mod_job_id = '||in_mod_job_id ||'
        AND b.date_inserted = '''||tcrm_date||'''::date
        AND hmp.lda_id = '||in_lda_output_id||'
      ) AS vals
      GROUP BY alias_id;';

  EXECUTE query;

  PERFORM core.analyze('work.modelling_data_matrix_handset_topic', in_mod_job_id);
END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.create_modelling_data_handset_model(integer, integer) OWNER TO xsl;

-----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.get_table_columns_with_prefix(table_schema1 text, table_name1 text, table_prefix text, omit_column_names text )
  RETURNS TABLE (columns text, prefix text) AS
$$ 
   /*
 * A helper function to retrieve column names and omitting some of them. To make fuctions bit less hardcoded. Used mainly in modelling_data_matrix_functions and combining matrices. 
 * 
 * INPUT: 
 * table_schema1:        schema for the input table
 * table_name1:          name of the input table (withput schema)
 * prefix:               prefix given the columns (useful if the columns are used in joins)
 * omit_column_names:    the columns of the table one doesn't want to return in a string (separated with comma)
 * 
 * OUTPUT:
 * columns:              the names of the columns in a table
 * prefix: 	         the prefix 
 *
 * VERSION  * 16.06.2014 KL 
 */ 
   SELECT  column_name::text, $3||'.'
     FROM information_schema.columns 
     WHERE table_schema= $1 
     AND table_name=$2
     AND column_name::text NOT IN (SELECT trim(unnest(string_to_array($4,','))))
$$
LANGUAGE sql;
ALTER FUNCTION work.get_table_columns_with_prefix(text, text, text, text) OWNER TO xsl;

--------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.combine_modelling_data(in_mod_job_id integer, in_target_table text, in_table_full_names text, in_join_columns text)
  RETURNS void AS
$BODY$
/*
 * A function to combine modelling data from different submatrices into
 * one matrix. Currently, it is assumed that the first table contains the mod job id column
 * for applying the where clause (e.g. WHERE table1.mod_job_id = in_mod_job_id).
 * 
 * Parameters: 
 * in_mod_job_id: module job id
 * in_target_table: table to insert combined data into, e.g. 'work.modelling_data_matrix'
 * in_table_full_names: tables to combine separated with comma, e.g. 'work.modelling_data_matrix_base_1a,work.modelling_data_matrix_base_1b'
 * in_join_columns: columns to use for joining separated with comma, e.g. 'mod_job_id,alias_id'
 * 
 * VERSION 
 * 24.07.2014 JVi: Allow arbitrary tables
 * 05.06.2014 HMa 
 */


DECLARE

  query text;
  columns_string1 text;
  columns_string2 text;
  from_clause text;
  target_schema text := substring(in_target_table from E'^(.*)\\.');
  target_table text  := substring(in_target_table from E'\\.(.*)$' );
  schema_names text[] := string_to_array(regexp_replace(in_table_full_names, E'(\\.[^,]*|\\s)', '', 'g'), ',');
  table_names text[]  := string_to_array(regexp_replace(in_table_full_names, E'([^.,]*?[.]|\\s)', '', 'g'), ',');
  
  i integer;
  n_tables integer := array_upper(string_to_array(in_table_full_names, ','), 1);
  target_table_column_array text[];

BEGIN

  --Arrange target table columns in alphabetical order
  columns_string1 :=
    array_to_string(core.array_sort(array_agg(column_name::text)),',')
    FROM information_schema.columns 
    WHERE table_schema = target_schema 
    AND table_name = target_table;

  columns_string1 := regexp_replace(columns_string1, E'\\s', '', 'g'); --Remove whitespace
  columns_string1 := regexp_replace(columns_string1, ',$', ''); --Remove trailing comma
  target_table_column_array := string_to_array(columns_string1, ',');
  
  columns_string2 := in_join_columns || ',';
  from_clause := '';
  
  FOR i IN 1..n_tables LOOP
    columns_string2 := columns_string2 || array_to_string(array_agg(prefix||columns),',') || ',' from
      (SELECT * FROM work.get_table_columns_with_prefix(schema_names[i], table_names[i], 'm_' || i, in_join_columns)) a;
    from_clause := from_clause || '
    LEFT JOIN ' || schema_names[i] || '.' || table_names[i] || ' m_' || i || ' USING (' || in_join_columns || ')'; 
  END LOOP;

  columns_string2 := regexp_replace(columns_string2, E'\\s', '', 'g'); --Remove whitespace
  columns_string2 := regexp_replace(columns_string2, ',$', '');        --Remove trailing comma
  
  -- Remove from columns_string1 columns that are not present in source tables
  -- (Note: could probably be done more elegantly - this is a bit silly)
  FOR i IN 1..array_upper(target_table_column_array, 1) LOOP
    IF NOT string_to_array(regexp_replace(columns_string2, E'[^,]*\\.', '', 'g'), ',') @> ARRAY[target_table_column_array[i]] THEN
      columns_string1 := regexp_replace(columns_string1, '^' || target_table_column_array[i] || '$', '');
      columns_string1 := regexp_replace(columns_string1, '^' || target_table_column_array[i] || ',', '');
      columns_string1 := regexp_replace(columns_string1, '(,' || target_table_column_array[i] || ')(?=,)', '');
      columns_string1 := regexp_replace(columns_string1, ',' || target_table_column_array[i] || '$', '');
    END IF;
  END LOOP;

  --Arrange source table columns in alphabetical order
  columns_string2 = array_to_string(array_agg(b), E',\n    ')
    FROM (
      SELECT b FROM (
        SELECT unnest(string_to_array(columns_string2, ','))
      ) a(b) 
      ORDER BY regexp_replace(b, E'[^,]*?\\.', '' ,'g') -- Order by table name without prefix
    ) c;

  from_clause := regexp_replace(from_clause, 'LEFT JOIN', 'FROM'); -- Replacing the first left join with from
  from_clause := regexp_replace(from_clause, E'USING \\(.*?\\)', '');    -- Removing unnecessary using clause from first row
  
  query := 'INSERT INTO ' || in_target_table || ' ('
    ||columns_string1||  
  ')
  SELECT '
    ||columns_string2||'
 '  || from_clause || '
  WHERE m_1.mod_job_id = ' || in_mod_job_id;

  EXECUTE query;

  PERFORM  core.analyze(in_target_table,in_mod_job_id);

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.combine_modelling_data(integer, text, text, text) OWNER TO xsl;


------------------------- Use case specific predictors -------------------------------

CREATE OR REPLACE FUNCTION work.uc_zero_day_prediction_create_predictors(in_mod_job_id integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function will create predictors for zero day value segment prediction from raw CDR data.
 * Predictors that are currently not implemented in SQL and/or workflow: 
 * those based on received calls, LDA predictors, something else?
 *
 * INPUT
 * in_mod_job_id : Identifier of module job
 *
 * VERSION
 * 2014-06-06 HMa - ICIF-181 Stack removal
 * 2013-02-23 JVi - Moved to modules db schema functions
 * 2013-01-29 JVi - Created
 */

DECLARE

  max_var_id                 integer;
  uc_zero_day_prediction_t4  date;
  uc_zero_day_prediction_t5  date;
  call_type_map              record;


BEGIN

  -- Change this if the identifiers of call types differ from the default values
  SELECT 1 AS voice, 2 AS sms, 3 AS video, 4 AS mms, 5 AS "data" INTO call_type_map;

  uc_zero_day_prediction_t4 := m.value::date FROM work.module_job_parameters m 
                               WHERE m.mod_job_id = in_mod_job_id AND m.key = 'uc_zero_day_prediction_t4';
  uc_zero_day_prediction_t5 := m.value::date FROM work.module_job_parameters m 
                               WHERE m.mod_job_id = in_mod_job_id AND m.key = 'uc_zero_day_prediction_t5';

  -- First, insert predictors formed directly from raw CDR data

  
  INSERT INTO work.modelling_data_matrix_zdp1 (
    mod_job_id, 
    alias_id, 
    uc_zero_day_prediction_inactive_to_active_days_ratio,
    uc_zero_day_prediction_weekend_to_weekday_voice_count_ratio,
    uc_zero_day_prediction_evening_to_daytime_ratio_voice_made,
    uc_zero_day_prediction_nighttime_to_daytime_ratio_voice_made,
    uc_zero_day_prediction_average_duration_daytime_voice_made,
    uc_zero_day_prediction_average_duration_evening_voice_made,
    uc_zero_day_prediction_average_duration_nighttime_voice_made,
    uc_zero_day_prediction_average_duration_weekend_voice_made,
    uc_zero_day_prediction_average_duration_weekday_voice_made,
    uc_zero_day_prediction_average_daily_count_weekday_voice_made,
    uc_zero_day_prediction_average_daily_count_weekend_voice_made,
    uc_zero_day_prediction_average_daily_count_sms_made,
    uc_zero_day_prediction_sms_to_voice_ratio,
    uc_zero_day_prediction_average_first_voice_hour,
    uc_zero_day_prediction_average_daily_voice_hour
  )
  SELECT
    in_mod_job_id AS mod_job_id,
    ccdr.alias_id,
    ((uc_zero_day_prediction_t5 - min(ccdr."date"))::integer - count(*))::real 
      / CASE WHEN count(*) > 0 THEN count(*) ELSE NULL END                                                   AS inactive_to_active_days_ratio,
    sum(CASE WHEN weekday IN (0,6) AND count_voice_made IS NOT NULL THEN count_voice_made ELSE NULL END) 
      / sum(CASE WHEN weekday BETWEEN 1 AND 5 AND count_voice_made > 0
                 THEN count_voice_made ELSE NULL END)
      *
    sum(CASE WHEN weekday BETWEEN 1 AND 5 THEN 1 ELSE 0 END)::real
      / sum(CASE WHEN weekday IN (0,6) THEN 1 ELSE NULL END)                                         AS weekend_to_weekday_voice_count_ratio, --Weighted for weekdays and weekend days included in data
    coalesce(sum(count_evening_voice_made), 0)::real
      / sum(CASE WHEN count_daytime_voice_made > 0 THEN count_daytime_voice_made ELSE NULL END)      AS evening_to_daytime_ratio_voice_made,
    coalesce(sum(count_nighttime_voice_made), 0)::real
     / sum(CASE WHEN count_daytime_voice_made > 0 THEN count_daytime_voice_made ELSE NULL END)       AS nighttime_to_daytime_ratio_voice_made,
    sum(duration_daytime_voice_made) 
      / sum(CASE WHEN count_daytime_voice_made > 0 THEN count_daytime_voice_made ELSE NULL END)      AS average_duration_daytime_voice_made, 
    sum(duration_evening_voice_made) 
      / sum(CASE WHEN count_evening_voice_made > 0 THEN count_evening_voice_made ELSE NULL END)      AS average_duration_evening_voice_made, 
    sum(duration_nighttime_voice_made) 
      / sum(CASE WHEN count_nighttime_voice_made > 0 THEN count_nighttime_voice_made ELSE NULL END)  AS average_duration_nighttime_voice_made, 
    sum(duration_weekend_voice_made) 
      / sum(CASE WHEN count_weekend_voice_made > 0 THEN count_weekend_voice_made ELSE NULL END)      AS average_duration_weekend_voice_made, 
    sum(duration_weekday_voice_made) 
      / sum(CASE WHEN count_weekday_voice_made > 0 THEN count_weekday_voice_made ELSE NULL END)      AS average_duration_weekday_voice_made,
    coalesce(sum(count_weekday_voice_made), 0)::real 
      / sum(CASE WHEN count_weekday_voice_made > 0 THEN 1 ELSE NULL END)                             AS average_daily_count_weekday_voice_made,
    coalesce(sum(count_weekend_voice_made), 0)::real
      / sum(CASE WHEN count_weekend_voice_made > 0 THEN 1 ELSE NULL END)                             AS average_daily_count_weekend_voice_made,
    coalesce(sum(count_sms_made), 0)::real 
      / (uc_zero_day_prediction_t5 - min(ccdr."date"))                                               AS average_daily_count_sms_made,
    coalesce(sum(count_sms_made), 0)::real 
      / (CASE WHEN sum(count_voice_made) > 0 THEN sum(count_voice_made) ELSE NULL END)               AS sms_to_voice_ratio,
    avg(first_voice_hour)                                                                            AS average_first_voice_hour,
    sum(average_voice_hour * count_voice_made) 
      / (CASE WHEN sum(count_voice_made) > 0 THEN sum(count_voice_made) ELSE NULL END)               AS average_daily_voice_hour
  FROM ( --ccdr -- Note: this is effectively a daily aggregates table and can be replaced with one that is filled in the data loading phase
    SELECT DISTINCT
      cdr.alias_a                                                                                        AS alias_id,
      cdr."date"                                                                                         AS date,
      sum(CASE WHEN cdr.call_type = call_type_map.sms   THEN 1 ELSE NULL END)::integer                   AS count_sms_made,
      sum(CASE WHEN cdr.is_daytime_voice = 1     THEN 1 ELSE NULL END )::integer                         AS count_daytime_voice_made,
      sum(CASE WHEN cdr.is_evening_voice = 1     THEN 1 ELSE NULL END )::integer                         AS count_evening_voice_made,
      sum(CASE WHEN cdr.is_nighttime_voice = 1   THEN 1 ELSE NULL END )::integer                         AS count_nighttime_voice_made,
      sum(CASE WHEN cdr.is_weekend_voice = 1     THEN 1 ELSE NULL END )::integer                         AS count_weekend_voice_made, --Note:Friday evening included
      sum(CASE WHEN cdr.is_weekday_voice = 1     THEN 1 ELSE NULL END )::integer                         AS count_weekday_voice_made, --Note:Friday evening not included
      sum(CASE WHEN cdr.is_daytime_voice = 1     THEN call_length ELSE NULL END )::real                  AS duration_daytime_voice_made,
      sum(CASE WHEN cdr.is_evening_voice = 1     THEN call_length ELSE NULL END )::real                  AS duration_evening_voice_made,
      sum(CASE WHEN cdr.is_nighttime_voice = 1   THEN call_length ELSE NULL END )::real                  AS duration_nighttime_voice_made,
      sum(CASE WHEN cdr.is_weekend_voice = 1     THEN call_length ELSE NULL END )::real                  AS duration_weekend_voice_made, --Note:Friday evening included
      sum(CASE WHEN cdr.is_weekday_voice = 1     THEN call_length ELSE NULL END )::real                  AS duration_weekday_voice_made,
      min(CASE WHEN cdr.call_type = call_type_map.voice THEN cdr.call_hour ELSE NULL END)::integer       AS first_voice_hour,
      avg(CASE WHEN cdr.call_type = call_type_map.voice THEN cdr.call_hour ELSE NULL END)::integer       AS average_voice_hour,
      count(CASE WHEN cdr.call_type = call_type_map.voice THEN 1 ELSE NULL END)::integer                 AS count_voice_made,
      EXTRACT("dow" FROM cdr."date")::integer                                                            AS weekday
    FROM --cdr 
    work.module_targets AS mt
    LEFT JOIN ( 
      SELECT 
        *,
        CASE WHEN c.call_type = call_type_map.voice 
      AND  EXTRACT("hour" FROM c.call_time) BETWEEN 8 AND 16
      AND  EXTRACT("dow" FROM c.call_time)  BETWEEN 1 AND  5
             THEN 1 ELSE 0 END                                                          AS is_daytime_voice,
        CASE WHEN c.call_type = call_type_map.voice 
             AND  EXTRACT("hour" FROM c.call_time) BETWEEN 17 AND 21
             AND  EXTRACT("dow" FROM c.call_time)  BETWEEN 1 AND  4
             THEN 1 ELSE 0 END                                                          AS is_evening_voice,
        CASE WHEN c.call_type = call_type_map.voice 
                AND (    EXTRACT("hour" FROM c.call_time) NOT BETWEEN 8 AND 21
                     AND EXTRACT("dow" FROM c.call_time)  BETWEEN 1 AND  4 )
                OR  (    EXTRACT ("hour" FROM c.call_time) < 8
                     AND EXTRACT("dow" FROM c.call_time)   = 5 )
             THEN 1 ELSE 0 END                                                          AS is_nighttime_voice,
        CASE WHEN c.call_type = call_type_map.voice 
                AND  ((  EXTRACT("dow"  FROM c.call_time)  BETWEEN 1 AND 5 )
                     AND NOT (    EXTRACT("dow"  FROM c.call_time)  = 5  
                              AND EXTRACT("hour" FROM c.call_time)  > 16 ))
            THEN 1 ELSE 0 END                                                           AS is_weekday_voice,
        CASE WHEN c.call_type = call_type_map.voice 
                AND  ((  EXTRACT("dow"  FROM c.call_time)  NOT BETWEEN 1 AND 5 )
                      OR (    EXTRACT("dow"  FROM c.call_time)  = 5  
                          AND EXTRACT("hour" FROM c.call_time)  > 16 ))
            THEN 1 ELSE 0 END                                                           AS is_weekend_voice,
        c.call_time::date                                                               AS date,
        EXTRACT("hour" FROM c.call_time)                                                AS call_hour
      FROM data.cdr AS c
    ) AS cdr
    ON mt.alias_id = cdr.alias_a
    WHERE mt.mod_job_id = in_mod_job_id 
    AND mt.audience_zero_day_prediction IS NOT NULL 
    AND cdr.call_time >= uc_zero_day_prediction_t4 AND cdr.call_time < uc_zero_day_prediction_t5
    GROUP BY cdr.alias_a, cdr."date"
  ) AS ccdr
  GROUP BY ccdr.alias_id;


        
  -- Then, insert predictors formed from topup data
  
  INSERT INTO work.modelling_data_matrix_zdp2 (
    mod_job_id, 
    alias_id, 
    uc_zero_day_prediction_average_daily_topup_count,
    uc_zero_day_prediction_total_topup_count_capped_at_two,
    uc_zero_day_prediction_average_topup_cost
  )
  SELECT
    in_mod_job_id                                                                              AS mod_job_id                          ,
    tt.alias_id                                                                                AS alias_id                            ,
    coalesce(sum(tt.topup_count),0) / (uc_zero_day_prediction_t5 - switch_on_date)             AS average_daily_topup_count           ,     
    least(coalesce(sum(tt.topup_count),0), 2)                                                  AS total_topup_count_capped_at_two     ,
    sum(tt.total_daily_topup_cost) 
      / sum(CASE WHEN tt.topup_count > 0 THEN tt.topup_count ELSE NULL END)                    AS average_topup_cost             
  FROM (
    SELECT
      t.charged_id           AS alias_id                ,
      count(*)               AS topup_count             ,
      sum(t.topup_cost)      AS total_daily_topup_cost  ,
      t."timestamp"::date    AS topup_date              ,
      min(mt.switch_on_date) AS switch_on_date 
    FROM (
      SELECT mt0.alias_id, min(c.switch_on_date) AS switch_on_date --If for some reason there are more than one switch on date
      FROM
        work.module_targets AS mt0
        LEFT JOIN data.in_crm  AS c
        ON mt0.alias_id = c.alias_id
        WHERE mt0.mod_job_id = in_mod_job_id
        AND mt0.audience_zero_day_prediction IS NOT NULL
        AND c.switch_on_date BETWEEN uc_zero_day_prediction_t4 AND uc_zero_day_prediction_t5 - 1 -- t5 - 1 day because BETWEEN is inclusive
        GROUP BY mt0.alias_id
    ) mt
    LEFT JOIN data.topup AS t
    ON mt.alias_id = t.charged_id
    WHERE t."timestamp" >= uc_zero_day_prediction_t4 AND t."timestamp" < uc_zero_day_prediction_t5
    GROUP BY t.charged_id, topup_date
  ) as tt
  GROUP BY tt.alias_id, switch_on_date;




  --Insert predictors from CRM data 

  INSERT INTO work.modelling_data_matrix_zdp4 (
    mod_job_id, 
    alias_id, 
    uc_zero_day_prediction_tenure,
    uc_zero_day_prediction_tenure_group,
    uc_zero_day_prediction_activation_weekday
  )
  SELECT
    in_mod_job_id AS mod_job_id,
    cc.alias_id,
    uc_zero_day_prediction_t5 - cc.switch_on_date AS tenure,
    CASE WHEN uc_zero_day_prediction_t5 - cc.switch_on_date BETWEEN 0 AND 2 THEN 1 
         WHEN uc_zero_day_prediction_t5 - cc.switch_on_date BETWEEN 3 AND 5 THEN 2 
         WHEN uc_zero_day_prediction_t5 - cc.switch_on_date BETWEEN 6 AND 7 THEN 3
         ELSE NULL END AS tenure_group,
    CASE EXTRACT("dow" FROM cc.switch_on_date)
      WHEN 0 THEN 'Sunday'
      WHEN 1 THEN 'Monday'
      WHEN 2 THEN 'Tuesday'
      WHEN 3 THEN 'Wednesday'
      WHEN 4 THEN 'Thursday'
      WHEN 5 THEN 'Friday'
      WHEN 6 THEN 'Saturday'
      ELSE NULL
      END AS activation_weekday
  FROM (
    SELECT DISTINCT
      c.alias_id,
      min(c.switch_on_date) AS switch_on_date --If for some reason there are more than one switch on date
    FROM data.in_crm AS c
    LEFT JOIN work.module_targets AS mt
    ON c.alias_id = mt.alias_id
    WHERE mt.mod_job_id = in_mod_job_id 
    AND mt.audience_zero_day_prediction IS NOT NULL 
    AND c.switch_on_date BETWEEN uc_zero_day_prediction_t4 AND uc_zero_day_prediction_t5 - 1 -- t5 - 1 day because BETWEEN is inclusive
    GROUP BY c.alias_id
  ) AS cc;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.uc_zero_day_prediction_create_predictors(integer) OWNER TO xsl;



--------------------------------------------------------------------------------------





CREATE OR REPLACE FUNCTION work.initialize_wf_muc_common(
  source_period_length integer,
  post_source_period_length integer,  
  lag_count1 integer,
  max_data_age integer, 
  crm_data_age integer, 
  n_handset_topics integer, 
  override_model_variables_check boolean,  
  in_use_cases text[], 
  in_included_use_cases boolean[], 
  in_run_type text, 
  in_t2 date, 
  in_mod_job_id integer, 
  in_run_descvar text, 
  in_descvar_interval_weeks integer, 
  in_lda_model_options text, 
  in_lda_output_id_old integer, 
  in_lda_input_options text, in_lda_input_id integer, 
  in_keys text[], 
  in_vals text[],
  in_check_data boolean
  )
  RETURNS SETOF text AS
$BODY$
/* SUMMARY:
 * This function initializes common parameters for the multiple use case (MUC) work flow.
 * Initially based on initialize_wf_prepaid_churn.
 *
 * Inputs:
 * source_period_length  
 * post_source_period_length ,  
 * lag_count1 ,
 * max_data_age , -- CDR and top-up data can not be older
 * crm_data_age, -- CRM data can not be older
 * n_handset_topics , -- Number of topics needs to be predefined, because the corresponding columns are needed in work.modelling_data_matrix
 * override_model_variables_check boolean,  -- Force all predictors to be calculated in work.create_modelling_data1/2/3
 * 
 * in_use_cases              All use cases included in the workflow, e.g. ARRAY['churn_inactivity', 'zero_day_prediction']
 * in_included_use_cases     Flags use cases to include in this mod_job, e.g. ARRAY[false, true]
 * in_run_type               'Predictors' or 'Predictors + Apply' or 'Predictors + Fit + Apply'
 * in_t2                     Source period end date
 * in_mod_job_id             Module job id; if <0, a new job is created
 * 
 * in_run_descvar            Run type of descriptive variables: 'Run', 'Run every given interval', or 'Do not run'
 * in_descvar_interval_weeks If in_run_descvar = 'Run every given interval', descriptive variables are calculated every 'descvar_interval_weeks' weeks
 * 
 * in_lda_model_options      LDA parameters - see LDA documentation or ask HMa
 * in_lda_output_id_old
 * in_lda_input_options
 * in_lda_input_id
 *
 * in_keys                   Optional keys and values to include in work.module_job_parameters table
 * in_vals
 *
 * 2014-06-06 QYu: Move the hard coded parameters to a separate table: work.scoring_muc_parameters
 * 2013-08-13 HMa: Moved LDA initialization to its own function
 * 2013-05-13 KL : Added secondary network initialization
 * 15.04.2013 KL Added arpu_query for network scorer as a parameter to be initialized
 * 2013-03-08 MOj: Fixed a bug in data quality check and usage of t9
 * 2013-02-21 HMa: Added the option to run only descriptive variables
 * 2013-01-10 JVi: Created from "work".initialize_wf_prepaid_churn
 */

DECLARE
  --Definitions
  lag_length                  int := ceil(source_period_length::double precision / lag_count1);
  lag_count                   int;
  
  --Variables and parameters
  t1                          date; --source period start
  t2                          date; --source period end
  t9                          date; --end of data period (i.e. t2 + post_source_period_length if model is fitted, otherwise same as t2)
  tcrm                        date;
  job_use_cases               text;

  this_mod_job_id             int;
  network_id                  int;
  network_2_id                int;
  network_weeks               text;
  
  calculate_predictors        boolean;
  fit_model                   boolean;
  apply_model                 boolean;
  run_type                    text := in_run_type;
  calculate_only_descvar      boolean;
  calculate_targets           boolean; --this means only target audience (no data needed after t2)

  calculate_targets_text      text;

  --Descriptive variables specific
  calculate_descvar           boolean;
  force_new_network           boolean;
  max_t2_descvar              date;

  --LDA-specific
  lda_params                  integer[];
  run_lda                     int;
  calculate_lda_input_data    int;
  calculate_lda_predictors    int;
  lda_input_id                int;
  lda_out_id                  int;
  lda_output_id_old           int;

  data_check_end              date;
  data_status                 record;
  cdr_latest                  date;
  topup_latest                date;
  date_latest                 date;
  topup_data				  boolean;

  arpu_query                  text; --query for network scorer


  --Miscellaneous
  keys text[] := in_keys;
  vals text[] := in_vals;
  tmp int; --Used in for loops etc.

BEGIN
 
  
  calculate_predictors := run_type ~ 'Predictors' AND in_mod_job_id < 1;
  fit_model := run_type ~ 'Fit';
  apply_model := run_type ~ 'Apply';
  calculate_only_descvar := run_type ~ 'Descriptive variables only';
  calculate_targets := calculate_predictors OR (calculate_only_descvar AND in_mod_job_id < 1); --this means only target audience (no data needed after t2)

  --Remove 'Predictors' from run type if mod_job_id is given
  IF run_type ~ 'Predictors' AND in_mod_job_id > 0 THEN
    run_type := regexp_replace(run_type, E'Predictors \\+ ', '');
    run_type := regexp_replace(run_type, 'Predictors', '');
  END IF;
 
  --Parameters for calculate predictors
  IF calculate_predictors OR calculate_targets THEN 

    -- Evaluate which use cases are run
    tmp := count(*) from unnest(in_use_cases);
    job_use_cases := '';
    FOR i IN 1..tmp LOOP
      IF in_included_use_cases[i] THEN
        job_use_cases := job_use_cases || in_use_cases[i] || ',';
      END IF;
    END LOOP;
    job_use_cases := TRIM(TRAILING ',' FROM job_use_cases);
    
    --Create a new mod_job_id
    this_mod_job_id := core.nextval_with_partitions('work.module_sequence');

    --Evaluate source period end date
    IF in_t2 IS NOT NULL THEN
      t2 := date_trunc('week', in_t2);
      t9 := t2;
      IF fit_model OR (NOT fit_model AND NOT apply_model AND NOT calculate_only_descvar) THEN --Fit or only predictors -> assume fitting
        t9 := t9 + post_source_period_length;
      END IF;
    ELSE --Use maximum possible end date
      cdr_latest := coalesce(max(data_date),'1900-01-01'::date) FROM data.data_quality WHERE data_source='cdr' AND status = 2;
      topup_latest := coalesce(max(data_date),'1900-01-01'::date) FROM data.data_quality WHERE data_source = 'topup' AND status = 2;
      -- Use Monday of the last week for which all the data are available: 
	  topup_data := (job_use_cases ~ 'churn_inactivity' OR job_use_cases ~ 'zero_day_prediction');
	  
	  date_latest := cdr_latest;
	  IF (topup_data) THEN
	    date_latest := least(date_latest, topup_latest);
      END IF;
      IF extract(DOW FROM date_latest) = 0 THEN -- Sunday -> we have all data from that week
        t9 := date_latest + 1; -- Monday next week
      ELSE -- other than Sunday -> we do not have all data from that week
        t9 := date_trunc('week', date_latest);
      END IF;
      t2 := t9;
      IF fit_model OR (NOT fit_model AND NOT apply_model AND NOT calculate_only_descvar) THEN --Fit or only predictors -> assume fitting
        t2 := t2 - post_source_period_length;
      END IF;
    END IF;

    -- Evaluate source period start date and last customer record date
    t1 := t2 - source_period_length;  
    tcrm := max(m.date_inserted) 
            FROM data.data_quality dq
            JOIN data.crm_snapshot_date_inserted_mapping m
            ON dq.data_date = m.snapshot_date
            WHERE dq.data_source = 'crm' AND dq.status = 2
            AND m.date_inserted <= t2;

    -- Data availability check
    data_check_end := t9; 

    -- Availability check
    -- CDR
    SELECT min(coalesce(qual.status,0)) AS min_status, max(coalesce(qual.status,100)) AS max_status INTO data_status
    FROM (
      SELECT t1 + generate_series(0,(data_check_end - t1 - 1)) AS data_date
    ) dates
    LEFT JOIN (
      SELECT data_date, status 
      FROM data.data_quality 
      WHERE data_source = 'cdr'
    ) qual
    ON qual.data_date = dates.data_date;

    IF (data_status.min_status < 2) THEN
      RAISE EXCEPTION 'Part or all of the CDR data is not available for period % to %.', t1, data_check_end;
    END IF;

	IF (data_status.max_status > 2) THEN
      RAISE EXCEPTION 'Part or all of the CDR data is not valid for period % to %.', t1, data_check_end;
    END IF;

   -- TOPUP data is checked only in case of churn_inactivity and zero day prediction
	IF (topup_data) THEN
  	  SELECT min(coalesce(qual.status,0)) AS min_status, max(coalesce(qual.status,100)) AS max_status INTO data_status
        FROM (
          SELECT t1 + generate_series(0,(data_check_end - t1 - 1)) AS data_date
        ) dates
      LEFT JOIN (
        SELECT data_date, status
        FROM data.data_quality 
	    WHERE data_source = 'topup'
      ) qual
      ON qual.data_date = dates.data_date;

	  IF (data_status.min_status < 2) THEN
        RAISE EXCEPTION 'Part or all of the TOPUP data is not available for period % to %.', t1, data_check_end;
      END IF;
	  IF (data_status.max_status > 2) THEN
        RAISE EXCEPTION 'Part or all of the TOPUP data is not valid for period % to %.', t1, data_check_end;
      END IF;
	END IF;

    -- CRM
    IF tcrm IS NULL OR t2 - tcrm > crm_data_age THEN
      RAISE EXCEPTION 'CRM data is older than allowed (% days) or not available.', crm_data_age;
    END IF;

    -- Too old data 
    IF in_check_data THEN
      -- CDR & TOPUP
      IF in_t2 IS NULL AND (now()::date - data_check_end > max_data_age) THEN -- Do not check it if the t2 is given
        RAISE EXCEPTION 'Latest data (%) is older then allowed (% days).', data_check_end , max_data_age;
      END IF;
    END IF;  

    --Parse network_id
    IF keys && ARRAY['xsl_job_id'] THEN
      network_id := d.value::integer FROM (SELECT unnest(keys) AS key, unnest(vals) AS value) AS d WHERE d.key = 'xsl_job_id';
    ELSE
      network_id := core.nextval_with_partitions('work.network_sequence');
    END IF;    

    --Parse secondary network_id
    IF keys && ARRAY['xsl_job_id_2'] THEN
      network_2_id := d.value::integer FROM (SELECT unnest(keys) AS key, unnest(vals) AS value) AS d WHERE d.key = 'xsl_job_id_2';
    ELSE
      network_2_id := core.nextval_with_partitions('work.network_sequence');
    END IF; 

    --Store the values
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES 
     (this_mod_job_id, 't1',            t1::text),
     (this_mod_job_id, 't2',            t2::text),
     (this_mod_job_id, 't9',            t9::text),
     (this_mod_job_id, 'tCRM',          tcrm::text),
     (this_mod_job_id, 'lag_count',     lag_count1::text),
     (this_mod_job_id, 'lag_length',    lag_length::text),
     (this_mod_job_id, 'xsl_job_id',    network_id::text),
     (this_mod_job_id, 'job_type',      'multiple_use_cases'),
     (this_mod_job_id, 'job_use_cases', job_use_cases),
     (this_mod_job_id, 'xsl_job_id_2',  network_2_id::text);


    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES (this_mod_job_id, 'time_inserted', now()::text);

    SELECT
      *
    FROM work.initialize_lda(this_mod_job_id, in_lda_model_options, in_lda_output_id_old, in_lda_input_options, in_lda_input_id)
    INTO lda_params;

    run_lda := lda_params[1];
    calculate_lda_input_data := lda_params[2];
    calculate_lda_predictors := lda_params[3];
    lda_input_id := lda_params[4];
    lda_out_id := lda_params[5];
    lda_output_id_old := lda_params[6];

  ELSE 
    --If predictors are not calculated, mod_job_id is given.    
    this_mod_job_id := in_mod_job_id; 

    --The t2 date should not be given.
    IF in_t2 IS NOT NULL THEN 
      RAISE EXCEPTION 'The source period end date t2 should not be given because existing predictors and the corresponding t2 date are used.';
    END IF;    

    IF in_run_type != run_type THEN 
      RAISE WARNING 'Resetting the run_type and calculate_targets in work.module_job_parameters';
      UPDATE work.module_job_parameters mjp SET "value" = run_type WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'run_type';
      UPDATE work.module_job_parameters mjp SET "value" = 'false' WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'calculate_targets';
    END IF;
    
    --Get the output parameters from db 
    t1            := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't1';
    t2            := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't2';
    t9            := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't9';
    tcrm          := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND lower(mjp.key) = 'tcrm';
    lag_count     := mjp.value::int  FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'lag_count';
    lag_length    := mjp.value::int  FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'lag_length';    
    network_id    := mjp.value::int  FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'xsl_job_id';
    job_use_cases := mjp.value       FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'job_use_cases';
  END IF;

  --Evaluate if descriptive variables are calculated
  IF calculate_only_descvar THEN
    calculate_descvar = TRUE;
  ELSIF in_run_descvar = 'Run' THEN
    calculate_descvar = TRUE;
  ELSIF in_run_descvar = 'Run every given interval' THEN

    -- Find the latest t2 for a descriptive variables run 
    SELECT max(to_date(b.value, 'YYYY-MM-DD'))
    FROM work.module_job_parameters AS a
    INNER JOIN work.module_job_parameters AS b
    ON a.mod_job_id = b.mod_job_id
    INNER JOIN results.descriptive_variables_subscriber_matrix AS dv
    ON a.mod_job_id = dv.mod_job_id
    WHERE a.key = 'job_type' 
    AND a.value = 'descriptive_variables' 
    AND b.key = 't2'
    INTO max_t2_descvar;

    IF max_t2_descvar IS NULL THEN
      calculate_descvar = TRUE;
    ELSIF t2 - max_t2_descvar >= 7 * in_descvar_interval_weeks THEN
      calculate_descvar = TRUE;
    ELSE 
      calculate_descvar = FALSE;
    END IF;

  ELSE
    calculate_descvar = FALSE;
  END IF;

  force_new_network = FALSE;

  --Evaluate source period weeks
  network_weeks := core.concatenate_yyyyww(t1, t2, ',');

  IF calculate_targets THEN
    calculate_targets_text := 'true';
  ELSE 
    calculate_targets_text := 'false';
  END IF;

  --Add parameters to keys and values
  keys := keys || ARRAY['run_type','calculate_targets','override_model_variables_check'];
  vals := vals || ARRAY[run_type,calculate_targets_text, (CASE WHEN override_model_variables_check THEN 'true' ELSE 'false' END)];
  
  --Check if some keys are already in the work.module_job_parameters and raise a warning
  tmp := count(*) FROM ( SELECT unnest(keys) INTERSECT SELECT mjp.key FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id ) a;
  IF tmp > 0 THEN 
    RAISE WARNING 'Some of the keys are already in the work.module_job_parameters table and they are not stored even if they differ.';
  END IF;

  --Insert all key-value pairs to work.module_job_parameters that are not there yet
  INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
  SELECT this_mod_job_id AS mod_job_id, d.key, d.value
  FROM (SELECT unnest(keys) AS key, unnest(vals) AS value) AS d
  LEFT OUTER JOIN work.module_job_parameters mjp 
  ON mjp.mod_job_id = this_mod_job_id AND mjp.key = d.key
  WHERE d.key IS NOT NULL
  AND mjp.key IS NULL;

  arpu_query='SELECT alias_id, monthly_arpu AS arpu
              FROM work.monthly_arpu
              WHERE mod_job_id = '|| this_mod_job_id ||';'; 
       


  PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);

  RETURN NEXT this_mod_job_id::text; ------------------------------------------------ 0
  RETURN NEXT network_id::text;  ---------------------------------------------------- 1
  RETURN NEXT network_weeks; -------------------------------------------------------- 2
  RETURN NEXT t1::text; ------------------------------------------------------------- 3
  RETURN NEXT t2::text; ------------------------------------------------------------- 4
  RETURN NEXT lag_count1::text; ------------------------------------------------------ 5
  RETURN NEXT lag_length::text; ----------------------------------------------------- 6
  RETURN NEXT source_period_length::text; ------------------------------------------- 7
  RETURN NEXT CASE WHEN calculate_predictors THEN 'true' ELSE 'false' END; ---------- 8
  RETURN NEXT CASE WHEN fit_model THEN 'true' ELSE 'false' END; --------------------- 9
  RETURN NEXT CASE WHEN apply_model THEN 'true' ELSE 'false' END; ------------------- 10
  RETURN NEXT CASE WHEN run_lda = 1 THEN 'true' ELSE 'false' END;---------------------11
  RETURN NEXT CASE WHEN calculate_lda_input_data = 1 THEN 'true' ELSE 'false' END;----12
  RETURN NEXT CASE WHEN calculate_lda_predictors = 1 THEN 'true' ELSE 'false' END;----13
  RETURN NEXT lda_input_id::text; ----------------------------------------------------14
  RETURN NEXT lda_output_id_old::text; -----------------------------------------------15
  RETURN NEXT lda_out_id::text; ------------------------------------------------------16
  RETURN NEXT n_handset_topics::text; ------------------------------------------------17
  RETURN NEXT job_use_cases::text; ---------------------------------------------------18
  RETURN NEXT tcrm::text; ------------------------------------------------------------19
  RETURN NEXT CASE WHEN calculate_descvar THEN 'true' ELSE 'false' END;---------------20
  RETURN NEXT CASE WHEN force_new_network THEN 'true' ELSE 'false' END;---------------21
  RETURN NEXT CASE WHEN calculate_targets THEN 'true' ELSE 'false' END;---------------22
  RETURN NEXT arpu_query;-------------------------------------------------------------23
  RETURN NEXT network_2_id;-----------------------------------------------------------24
  RETURN;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION "work".initialize_wf_muc_common(
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  boolean,
  text[], 
  boolean[], 
  text, 
  date, 
  integer, 
  text, 
  integer, 
  text, 
  integer, 
  text, 
  integer, 
  text[], 
  text[], 
  boolean
) OWNER TO xsl;



  
CREATE OR REPLACE FUNCTION "work".initialize_churn_inactivity(
  gap_length  integer,
  target_period_length  integer,
  gap2_length integer,
  evaluation_period_length  integer,
  in_mod_job_id integer, 
  in_uc_churn_inactivity_model_id integer, 
  in_uc_churn_inactivity_include_postpaid boolean, 
  redefine_preprocessing boolean
)
  RETURNS SETOF text AS
$BODY$
/* SUMMARY:
 * This function initializes use case specific parameters churn inactivity use case 1.
 * Initially based on initialize_wf_prepaid_churn.
 *
 * 2014-06-06 QYu: Move the hard coded parameters to a separate table: work.scoring_muc_parameters
 * 2013-02-21 HMa: Insert usecase-specific parameters if targets are calculated
 * 2013-01-11 JVi: Created from "work".initialize_wf_prepaid_churn
 */

DECLARE
  --Definitions
  

  lag_count                            int;
  lag_length                           int;

  --Variables
  this_mod_job_id                      int := in_mod_job_id; -- this_mod_job_id is legacy, can be replaced with in_mod_job_id everywhere in this function
  this_uc_churn_inactivity_model_id    int;  
  template_model_id                    int;
  uc_churn_inactivity_include_postpaid smallint;

  t1                                   date; --source period start
  t2                                   date; --source period end
  uc_churn_inactivity_t4               date; --last-activity period start
  uc_churn_inactivity_t5               date; --last-activity period end
  uc_churn_inactivity_t6               date; --inactivity period start
  uc_churn_inactivity_t7               date; --inactivity period end
  tcrm                                 date;
  
  run_type                             text;
  calculate_predictors                 boolean;
  fit_model                            boolean;
  apply_model                          boolean;
  calculate_targets                    text;

  tmp                                  int;

BEGIN

  -- Get existing required parameters from work.module_job_parameters

  t2                := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't2';
  tcrm              := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND lower(mjp.key) = 'tcrm';
  run_type          := mjp.value::text FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'run_type';
  calculate_targets := mjp.value::text FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'calculate_targets';

  --Evaluate which parts of the workflow are run
  calculate_predictors := run_type ~ 'Predictors';
  fit_model := run_type ~ 'Fit';
  apply_model := run_type ~ 'Apply';
  
  --Parameters for calculate predictors
  IF calculate_predictors OR calculate_targets = 'true' THEN 
    --Evaluate the parameters for prepaid inactivity churn calculation
    -- Evaluate source period start date and last customer record date
    uc_churn_inactivity_t4 := t2 + gap_length;
    uc_churn_inactivity_t5 := uc_churn_inactivity_t4 + target_period_length;
    uc_churn_inactivity_t6 := uc_churn_inactivity_t5 + gap2_length;
    uc_churn_inactivity_t7 := uc_churn_inactivity_t6 + evaluation_period_length;

    IF in_uc_churn_inactivity_include_postpaid THEN 
      uc_churn_inactivity_include_postpaid = 1;
    ELSE 
      uc_churn_inactivity_include_postpaid = 0;
    END IF;

    --Store the values
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES
      (this_mod_job_id, 'uc_churn_inactivity_include_postpaid', uc_churn_inactivity_include_postpaid),
      (this_mod_job_id, 'uc_churn_inactivity_t4',               uc_churn_inactivity_t4),
      (this_mod_job_id, 'uc_churn_inactivity_t5',               uc_churn_inactivity_t5),
      (this_mod_job_id, 'uc_churn_inactivity_t6',               uc_churn_inactivity_t6),
      (this_mod_job_id, 'uc_churn_inactivity_t7',               uc_churn_inactivity_t7);

    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES (this_mod_job_id, 'uc_churn_inactivity_time_inserted', now()::text);

  END IF;

  --Parameters for fit and apply
  IF fit_model OR apply_model THEN
    --Check if the work.module_job_parameters contains already a model_id
    tmp := count(*) FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'uc_churn_inactivity_model_id';
  
    IF in_uc_churn_inactivity_model_id > 0 THEN --Use the given model_id
      this_uc_churn_inactivity_model_id := in_uc_churn_inactivity_model_id;
    ELSIF tmp = 1 AND NOT redefine_preprocessing THEN --Read the model_id from work.module_job_parameters
      this_uc_churn_inactivity_model_id := mjp.value::int FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'uc_churn_inactivity_model_id';
    ELSIF fit_model THEN --Create a new model_id for fitting

      -- First, if new preprocessing parameters are not requested, look for the latest parent model: 
      IF NOT redefine_preprocessing THEN 
        this_uc_churn_inactivity_model_id := work.model_parameters_for_fitter(this_mod_job_id, 
          ARRAY['use_case'        , 'is_parent'], 
          ARRAY['churn_inactivity', 'true'     ], 
          ARRAY[NULL],
          ARRAY['t1', 't2', 'uc_churn_inactivity_t4', 'uc_churn_inactivity_t5', 'uc_churn_inactivity_t6', 'uc_churn_inactivity_t7', 'tCRM']);
      END IF;

      -- If new preprocesssing parameters are requested or no parent model is found, find the latest template model: 
      IF redefine_preprocessing OR this_uc_churn_inactivity_model_id IS NULL OR this_uc_churn_inactivity_model_id < 1 THEN 
        template_model_id := max(model_id) FROM work.module_template_models WHERE key = 'use_case' AND value = 'churn_inactivity';
        IF template_model_id IS NULL THEN 
          RAISE EXCEPTION 'No template model was found for the churn_inactivity use case.';
        END IF;
        this_uc_churn_inactivity_model_id = NULL;
      END IF;

    ELSE --Use maximum model_id for applying
      this_uc_churn_inactivity_model_id := max(mm.model_id) FROM work.module_models mm WHERE mm.key = 'use_case' AND mm.value = 'churn_inactivity';
      
      IF this_uc_churn_inactivity_model_id IS NULL OR this_uc_churn_inactivity_model_id < 1 THEN
        RAISE EXCEPTION 'No parent model was found for the churn_inactivity use case.';
      END IF; 

    END IF;

    --Store the values
    IF tmp = 1 THEN --Update the model_id in work.module_job_parameters
      IF in_uc_churn_inactivity_model_id > 0 THEN 
        RAISE WARNING 'Resetting the model_id in work.module_job_parameters';
      END IF;
      UPDATE work.module_job_parameters mjp SET "value" = this_uc_churn_inactivity_model_id::text WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'uc_churn_inactivity_model_id';
    ELSIF this_uc_churn_inactivity_model_id IS NOT NULL THEN
      INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
      VALUES (this_mod_job_id, 'uc_churn_inactivity_model_id', this_uc_churn_inactivity_model_id::text);    
    END IF;
  END IF;

  PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);

  RETURN NEXT this_uc_churn_inactivity_model_id::text; ------------------------------------ 0
  RETURN NEXT template_model_id::text; ---------------------------------------------------- 1
  RETURN;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION "work".initialize_churn_inactivity(
  integer, 
  integer,
  integer, 
  integer,
  integer, 
  integer, 
  boolean,
  boolean
) OWNER TO xsl;


--------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION "work".initialize_churn_postpaid(
  gap_length integer,
  target_period_length integer,
  in_mod_job_id integer, 
  in_uc_churn_postpaid_model_id integer,
  redefine_preprocessing boolean
)
  RETURNS SETOF text AS
$BODY$
/* SUMMARY:
 * This function initializes use case specific parameters for churn postpaid usecase.
 *
 * 2014-06-06 QYu: Move the hard coded parameters to a separate table: work.scoring_muc_parameters
 * 2013-01-25 HMa: Created from "work".initialize_wf_churn_inactivity
 */

DECLARE
  --Definitions
 

  lag_count                int;
  lag_length               int;

  --Variables
  this_mod_job_id          int := in_mod_job_id; -- this_mod_job_id is legacy, can be replaced with in_mod_job_id everywhere in this function
  this_uc_churn_postpaid_model_id int;  
  template_model_id        int;

  t1                       date; --source period start
  t2                       date; --source period end
  uc_churn_postpaid_t4     date; --target period start
  uc_churn_postpaid_t5     date; --target period end
  tcrm                     date;
  
  run_type text;
  calculate_predictors     boolean;
  fit_model                boolean;
  apply_model              boolean;
  calculate_targets        text;
 
  tmp int;

BEGIN

  -- Get existing required parameters from work.module_job_parameters

  t2                := mjp.value::date    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't2';
  tcrm              := mjp.value::date    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND lower(mjp.key) = 'tcrm';
  run_type          := mjp.value::text    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'run_type';
  calculate_targets := mjp.value::text    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'calculate_targets';

  --Evaluate which parts of the workflow are run
  calculate_predictors := run_type ~ 'Predictors';
  fit_model := run_type ~ 'Fit';
  apply_model := run_type ~ 'Apply';
  
  --Parameters for calculate predictors
  IF calculate_predictors OR calculate_targets = 'true' THEN 
    --Evaluate the parameters for postpaid churn calculation
    uc_churn_postpaid_t4 := t2 + gap_length;
    uc_churn_postpaid_t5 := uc_churn_postpaid_t4 + target_period_length;

    --Store the values
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES
      (this_mod_job_id, 'uc_churn_postpaid_t4', uc_churn_postpaid_t4),
      (this_mod_job_id, 'uc_churn_postpaid_t5', uc_churn_postpaid_t5);

    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES (this_mod_job_id, 'uc_churn_postpaid_time_inserted', now()::text);

  END IF;

  --Parameters for fit and apply
  IF fit_model OR apply_model THEN
    --Check if the work.module_job_parameters contains already a model_id
    tmp := count(*) FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'uc_churn_postpaid_model_id';
  
    IF in_uc_churn_postpaid_model_id > 0 THEN --Use the given model_id
      this_uc_churn_postpaid_model_id := in_uc_churn_postpaid_model_id;
    ELSIF tmp = 1 AND NOT redefine_preprocessing THEN --Read the model_id from work.module_job_parameters
      this_uc_churn_postpaid_model_id := mjp.value::int FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'uc_churn_postpaid_model_id';
    ELSIF fit_model THEN --Create a new model_id for fitting

      -- First, if new preprocessing parameters are not requested, look for the latest parent model: 
      IF NOT redefine_preprocessing THEN 
        this_uc_churn_postpaid_model_id := work.model_parameters_for_fitter(this_mod_job_id, 
          ARRAY['use_case'        , 'is_parent'], 
          ARRAY['churn_postpaid', 'true'     ], 
          ARRAY[NULL],
          ARRAY['t1', 't2', 'uc_churn_postpaid_t4', 'uc_churn_postpaid_t5', 'uc_churn_postpaid_t6', 'uc_churn_postpaid_t7', 'tCRM']);
      END IF;

      -- If new preprocesssing parameters are requested or no parent model is found, find the latest template model: 
      IF redefine_preprocessing OR this_uc_churn_postpaid_model_id IS NULL OR this_uc_churn_postpaid_model_id < 1 THEN 
        template_model_id := max(model_id) FROM work.module_template_models WHERE key = 'use_case' AND value = 'churn_postpaid';
        IF template_model_id IS NULL THEN 
          RAISE EXCEPTION 'No template model was found for the churn_postpaid use case.';
        END IF;
        this_uc_churn_postpaid_model_id = NULL;
      END IF;

    ELSE --Use maximum model_id for applying
      this_uc_churn_postpaid_model_id := max(mm.model_id) FROM work.module_models mm WHERE mm.key = 'use_case' AND mm.value = 'churn_postpaid';

      IF this_uc_churn_postpaid_model_id IS NULL OR this_uc_churn_postpaid_model_id < 1 THEN
        RAISE EXCEPTION 'No parent model was found.';
      END IF;

    END IF;

    --Store the values
    IF tmp = 1 THEN --Update the model_id in work.module_job_parameters
      IF in_uc_churn_postpaid_model_id > 0 THEN 
        RAISE WARNING 'Resetting the model_id in work.module_job_parameters';
      END IF;
      UPDATE work.module_job_parameters mjp SET "value" = this_uc_churn_postpaid_model_id::text WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'uc_churn_postpaid_model_id';
    ELSIF this_uc_churn_postpaid_model_id IS NOT NULL THEN
      INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
      VALUES (this_mod_job_id, 'uc_churn_postpaid_model_id', this_uc_churn_postpaid_model_id::text);    
    END IF;
  END IF;

  PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);

  RETURN NEXT this_uc_churn_postpaid_model_id::text; ------------------------------------ 0
  RETURN NEXT template_model_id::text; -------------------------------------------------- 1
  RETURN;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION "work".initialize_churn_postpaid(
  integer, 
  integer,
  integer, 
  integer,
  boolean
) OWNER TO xsl;





--------------------------------------------------------------------------------------------------------------------




CREATE OR REPLACE FUNCTION "work".initialize_zero_day_prediction(
  uc_zero_day_prediction_evaluation_period_length integer,  --Number of days during which customer value segment is determined
  in_mod_job_id integer, 
  in_uc_zero_day_prediction_model_id integer, 
  in_uc_zero_day_prediction_redefine_value_segments boolean, 
  redefine_preprocessing boolean
)
  RETURNS SETOF text AS
$BODY$
/* SUMMARY:
 * This function initializes use case specific parameters for zero day prediction.
 * Parameters initialized are model id and dates fulfilling data requirements.
 * Note: only tested with data generator data and run type 'predictors'
 *
 * 2014-06-06 QYu: Move the hard coded parameters to a separate table: work.scoring_muc_parameters
 * 2013-01-24 JVi: Cleaned and commented
 * 2013-01-21 JVi: Created from "work".initialize_churn_inactivity
 */

DECLARE

  --Parametres

  this_uc_zero_day_prediction_model_id  int;  
  template_model_id                     int;
  t1                                    date; --source period start
  t2                                    date; --source period end
  t9                                    date; --last date of input data
  uc_zero_day_prediction_t4             date; --customer has to have joined on or after this day to be evaluated
  uc_zero_day_prediction_t5             date; --customer has to have joined before this day to be evaluated
  
  run_type                              text;
  calculate_predictors                  boolean;
  fit_model                             boolean;
  apply_model                           boolean;
  calculate_targets                     text;

  --Miscellaneous
  tmp                                   int;

BEGIN

  -- Get existing required parameters from work.module_job_parameters

  t1                := mjp.value::date    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1';
  t2                := mjp.value::date    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2';
  t9                := mjp.value::date    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't9';
  run_type          := mjp.value::text    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'run_type';
  calculate_targets := mjp.value::text    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'calculate_targets';

  --Evaluate which parts of the workflow are run
  calculate_predictors := run_type ~ 'Predictors';
  fit_model := run_type ~ 'Fit';
  apply_model := run_type ~ 'Apply';
  
  --Parameters for calculating predictors
  IF calculate_predictors OR calculate_targets = 'true' THEN 
    --Evaluate dates t4 and t5 between which subscriber must have joined to be included in predictor calculation for model
    IF fit_model OR (NOT fit_model AND NOT apply_model) THEN --Fit or only Predictors -> assume fitting
      uc_zero_day_prediction_t4 := t2 - 7;
      uc_zero_day_prediction_t5 := t2;
    ELSE
      uc_zero_day_prediction_t4 := t9 - 7;
      uc_zero_day_prediction_t5 := t9;
    END IF;
    --Store the values
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    SELECT 
      in_mod_job_id,
      unnest(ARRAY['uc_zero_day_prediction_t4', 'uc_zero_day_prediction_t5']         || ARRAY['uc_zero_day_prediction_time_inserted']) ,
      unnest(ARRAY[ uc_zero_day_prediction_t4 ,  uc_zero_day_prediction_t5 ]::text[] || ARRAY[ now()          ]::text[]);

    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    SELECT 
      in_mod_job_id,
      'uc_zero_day_prediction_redefine_value_segments',
      CASE WHEN in_uc_zero_day_prediction_redefine_value_segments THEN 'true' ELSE 'false' END;

  END IF;

  --Parameters for fit and apply
  IF fit_model OR apply_model THEN
    --Check if the work.module_job_parameters contains already a model_id
    tmp := count(*) FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'uc_zero_day_prediction_model_id';

    --If a model id was given as input, use that model id  
    IF in_uc_zero_day_prediction_model_id > 0 THEN 
      this_uc_zero_day_prediction_model_id := in_uc_zero_day_prediction_model_id;
    --Otherwise, use the model id in module job parameters if available
    ELSIF tmp = 1 AND NOT redefine_preprocessing THEN
      this_uc_zero_day_prediction_model_id := mjp.value::int FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'uc_zero_day_prediction_model_id';
    --Otherwise, if fitting was required, create a new model id
    ELSIF fit_model THEN

      IF NOT redefine_preprocessing THEN 
        this_uc_zero_day_prediction_model_id := work.model_parameters_for_fitter(in_mod_job_id, 
          ARRAY['use_case'           , 'is_parent'], 
          ARRAY['zero_day_prediction', 'true'     ], 
          ARRAY[NULL],
          ARRAY['t1', 't2', 'uc_zero_day_prediction_t4', 'uc_zero_day_prediction_t5', 't9']);
      END IF;

      -- If new preprocesssing parameters are requested or no parent model is found, find the latest template model: 
      IF redefine_preprocessing OR this_uc_zero_day_prediction_model_id IS NULL OR this_uc_zero_day_prediction_model_id < 1 THEN 
        template_model_id := max(model_id) FROM work.module_template_models WHERE key = 'use_case' AND value = 'zero_day_prediction';
        IF template_model_id IS NULL THEN 
          RAISE EXCEPTION 'No template model was found for the zero_day_prediction use case.';
        END IF;
        this_uc_zero_day_prediction_model_id = NULL;
      END IF;

    --Otherwise, use the maximum model id available for zero day prediction
    ELSE
      this_uc_zero_day_prediction_model_id := max(mm.model_id) FROM work.module_models mm WHERE mm.key = 'use_case' AND mm.value = 'zero_day_prediction';

      IF this_uc_zero_day_prediction_model_id IS NULL OR this_uc_zero_day_prediction_model_id < 1 THEN
        RAISE EXCEPTION 'No parent model was found.';
      END IF;    

    END IF;

    --Store the model id
    IF tmp = 1 THEN --Update the model_id in work.module_job_parameters
      IF in_uc_zero_day_prediction_model_id > 0 THEN 
        RAISE WARNING 'Resetting the model_id in work.module_job_parameters';
      END IF;
      UPDATE work.module_job_parameters mjp SET "value" = this_uc_zero_day_prediction_model_id::text 
      WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'uc_zero_day_prediction_model_id';
    ELSIF this_uc_zero_day_prediction_model_id IS NOT NULL THEN
      INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
      VALUES (in_mod_job_id, 'uc_zero_day_prediction_model_id', this_uc_zero_day_prediction_model_id::text);    
    END IF;
  END IF;  

  PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);

  RETURN NEXT this_uc_zero_day_prediction_model_id::text; ------------------------------------ 0
  RETURN NEXT template_model_id::text; ------------------------------------------------------- 1
  RETURN;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION "work".initialize_zero_day_prediction(
  integer,
  integer, 
  integer, 
  boolean,
  boolean
) OWNER TO xsl;



--------------------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION "work".initialize_product(
  gap_length integer,
  target_period_length integer,
  in_mod_job_id integer, 
  in_model_id integer, 
  redefine_preprocessing boolean
)
  RETURNS SETOF text AS
$BODY$
/* SUMMARY:
 * This function initializes use case specific parameters for the product recommendation use case.
 *
 * 2014-06-06 QYu: Move the hard coded parameters to a separate table: work.scoring_muc_parameters
 * 2013-01-31 HMa
 */

DECLARE
  -- Module job parameters
  run_type                             text;
  calculate_predictors                 boolean;
  fit_model                            boolean;
  apply_model                          boolean;
  calculate_targets                    text;

  t2                                   date;
  job_use_cases                        text;

  -- Usecase-specific parameters
  product_id_array                     text[];
  products                             text;
  uc_product_t4                        date;
  uc_product_t5                        date;
  model_id                             integer;
  template_model_id                    integer;
  target_calculated_from               text; 
 
  tmp                                  int;
  i                                    int;

BEGIN

  /* Case-specific parameters start */

  -- Products included in the use case:
  product_id_array    = ARRAY['product_x','product_y'];

  -- Insert product id - product name mapping and other information of the products if not added already:
  INSERT INTO data.product_information
  SELECT 
    aa.* 
  FROM
  (
    SELECT 
      'product_x' AS product_id, 
      '' AS subproduct_id, 
      'Product X' AS product_name, 
      1.99 AS product_cost
    UNION
    SELECT
      'product_y' AS product_id, 
      '' AS subproduct_id, 
      'Product Y' AS product_name, 
      20.00 AS product_cost
  ) aa
  LEFT JOIN data.product_information bb
  ON aa.product_id = bb.product_id
  WHERE bb.product_id IS NULL; -- make sure we do not try to insert same product ID may times
  
  -- The Best next product use case has two different definitions: 
  -- 1) target_calculated_from = 'source':
  --    * Those who have the product during the last week of the source period are positive targets. 
  --    * The aim is to find those subscribers without the product that are similar to the ones that already have a product. 
  -- 2) target_calculated_from = 'target':
  --    * Those who buy the product during the target period (t4...t5) are positive targets. 
  --    * The aim is to find those subscribers that are likely to buy the product during the target period. 
  target_calculated_from = 'target'; -- 'source' or 'target'

  /* Case-specific parameters end */
 
  -- Get run type from work.module_job_parameters
  run_type          := mjp.value::text    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'run_type';
  calculate_targets := mjp.value::text    FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'calculate_targets';

  --Evaluate which parts of the workflow are run
  calculate_predictors := run_type ~ 'Predictors'; 
  fit_model := run_type ~ 'Fit';
  apply_model := run_type ~ 'Apply';

  IF calculate_predictors OR calculate_targets = 'true' THEN 

    products       =  array_to_string(product_id_array,',');
    
    job_use_cases := mjp.value::text FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'job_use_cases';
    t2            := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2';

    -- Replace 'produt' with 'product_<productname_x>,product_<productname_y>...' in mod job parameter 'job_use_cases':
    FOR i IN 1..array_upper(product_id_array,1)
    LOOP
      product_id_array[i] = 'product_' || product_id_array[i];
    END LOOP;
    job_use_cases := regexp_replace(job_use_cases, 'product', array_to_string(product_id_array,','));
    UPDATE work.module_job_parameters mjp SET "value" = job_use_cases WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'job_use_cases';

    --Evaluate the target period dates for 
    uc_product_t4 := t2 + gap_length;
    uc_product_t5 := uc_product_t4 + target_period_length;

    --Store the usecase-specific values
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES
      (in_mod_job_id, 'uc_product_products',               products),
      (in_mod_job_id, 'uc_product_target_calculated_from', target_calculated_from),
      (in_mod_job_id, 'uc_product_t4',                     uc_product_t4),
      (in_mod_job_id, 'uc_product_t5',                     uc_product_t5);

    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES (in_mod_job_id, 'uc_product_time_inserted', now()::text);

  END IF;

  --Parameters for fit and apply
  IF fit_model OR apply_model THEN
    --Check if the work.module_job_parameters already contains a model_id
    tmp := count(*) FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'uc_product_model_id';
  
    IF in_model_id > 0 THEN --Use the given model_id
      model_id := in_model_id;
    ELSIF tmp = 1 AND NOT redefine_preprocessing THEN --Read the model_id from work.module_job_parameters
      model_id := mjp.value::int FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'uc_product_model_id';
    ELSIF fit_model THEN --Create a new model_id for fitting

      -- First, if new preprocessing parameters are not requested, look for the latest parent model: 
      IF NOT redefine_preprocessing THEN 
        model_id := work.model_parameters_for_fitter(in_mod_job_id, 
          ARRAY['use_case', 'is_parent'], 
          ARRAY['product' , 'true'     ], 
          ARRAY[NULL],
          array[NULL]);
      END IF;

      -- If new preprocesssing parameters are requested or no parent model is found, find the latest template model: 
      IF redefine_preprocessing OR model_id IS NULL OR model_id < 1 THEN 
        template_model_id := max(mm.model_id) FROM work.module_template_models mm WHERE mm.key = 'use_case' AND mm.value = 'product';
        IF template_model_id IS NULL THEN 
          RAISE EXCEPTION 'No template model was found for the product use case.';
        END IF;
        model_id = NULL;
      END IF;

    ELSE --Use maximum model_id for applying
      model_id := max(mm.model_id) FROM work.module_models mm WHERE mm.key = 'use_case' AND mm.value = 'product';

      IF model_id IS NULL OR model_id < 1 THEN
        RAISE EXCEPTION 'No parent model was found.';
      END IF;  

    END IF;


    --Store the values
    IF tmp = 1 THEN --Update the uc_product_model_id in work.module_job_parameters
      IF in_model_id > 0 THEN 
        RAISE WARNING 'Resetting the uc_product_model_id in work.module_job_parameters';
      END IF;
      UPDATE work.module_job_parameters mjp SET "value" = model_id::text WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'uc_product_model_id';
    ELSIF model_id IS NOT NULL THEN
      INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
      VALUES (in_mod_job_id, 'uc_product_model_id', model_id::text);    
    END IF;
  END IF;


  PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);

  RETURN NEXT model_id::text; ------------------------------------ 0
  RETURN NEXT template_model_id::text; --------------------------- 1
  RETURN;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION "work".initialize_product(
  gap_length integer,
  target_period_length integer,
  in_mod_job_id integer, 
  in_model_id integer, 
  redefine_preprocessing boolean
) OWNER TO xsl;





--------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION results.calculate_score_list_approval_stats(in_mod_job_id integer)
  RETURNS void AS
$BODY$ 
/* SUMMARY:
 * This function calculates approval statistics for scored subscribers. 
 * Do not take the score into account, just other properties.
 * Note that most of the statistics use median as the value. 
 * Sometimes this can have unwanted side effects like zeros for all. 
 *
 * 2012-09-28 MOj: ICIF-59
 * 2012-08-10 TSi: Original version
 */
DECLARE
  t1 date; 
  t2 date; 

  job_use_cases text;
  use_cases text[];
  use_case text;
  sql_string text;
  
BEGIN
  t1 := mjp.value::date FROM work.module_job_parameters AS mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1';
  t2 := mjp.value::date FROM work.module_job_parameters AS mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'; 

  -- Common statistic
  INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
  SELECT in_mod_job_id AS mod_job_id, 'SCORING' AS stat_name, 'ALL' AS group_name, now()::date AS data_date, 'SUBSCRIBERS' AS var_name, count(*) AS var_value, 1 AS order_id
  FROM work.modelling_data_matrix AS d
  WHERE d.mod_job_id = in_mod_job_id;
 
  INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
  SELECT DISTINCT
    in_mod_job_id AS mod_job_id,
    'SCORING' AS stat_name,
    'ALL' AS group_name, 
    now()::date AS data_date,
    var_name.name,
    CASE
      WHEN var_name.name = 'ACTIVE_SUBSCRIBERS'     THEN vals.count
      WHEN var_name.name = 'BIDIRECT_CONNECTIONS'   THEN vals.k
      WHEN var_name.name = 'WEEKLY_SMSES'           THEN vals.sms
      WHEN var_name.name = 'WEEKLY_VOICE_CALLS'     THEN vals.voice
      WHEN var_name.name = 'WEEKLY_TOPUPS'          THEN vals.topup
      WHEN var_name.name = 'TENURE'                 THEN vals.tenure
      ELSE NULL
    END AS var_value,
    CASE
      WHEN var_name.name = 'ACTIVE_SUBSCRIBERS'     THEN 2
      WHEN var_name.name = 'BIDIRECT_CONNECTIONS'   THEN 3
      WHEN var_name.name = 'WEEKLY_SMSES'           THEN 4
      WHEN var_name.name = 'WEEKLY_VOICE_CALLS'     THEN 5
      WHEN var_name.name = 'WEEKLY_TOPUPS'          THEN 6
      WHEN var_name.name = 'TENURE'                 THEN 7
      ELSE NULL
    END AS order_id
  FROM (
    SELECT 'ACTIVE_SUBSCRIBERS' AS name   UNION ALL
    SELECT 'BIDIRECT_CONNECTIONS' AS name UNION ALL
    SELECT 'WEEKLY_SMSES' AS name         UNION ALL
    SELECT 'WEEKLY_VOICE_CALLS' AS name   UNION ALL
    SELECT 'WEEKLY_TOPUPS' AS name        UNION ALL
    SELECT 'TENURE'
  ) AS var_name
  CROSS JOIN (
    SELECT
      count(*) AS count,
      avg(z.k     ) FILTER (WHERE z.k_row_num      BETWEEN floor(z.half_count) AND ceil(z.half_count)) AS k,
      avg(z.sms   ) FILTER (WHERE z.sms_row_num    BETWEEN floor(z.half_count) AND ceil(z.half_count)) AS sms,
      avg(z.voice ) FILTER (WHERE z.voice_row_num  BETWEEN floor(z.half_count) AND ceil(z.half_count)) AS voice,
      avg(z.topup ) FILTER (WHERE z.topup_row_num  BETWEEN floor(z.half_count) AND ceil(z.half_count)) AS topup,
      avg(z.tenure) FILTER (WHERE z.tenure_row_num BETWEEN floor(z.half_count) AND ceil(z.half_count)) AS tenure
    FROM (
      SELECT
        ((count(*) OVER ())::double precision - 1.0) / 2.0 + 1.0 AS half_count,
        row_number() OVER (ORDER BY da.k     ) AS k_row_num,
        row_number() OVER (ORDER BY da.sms   ) AS sms_row_num,
        row_number() OVER (ORDER BY da.voice ) AS voice_row_num,
        row_number() OVER (ORDER BY da.topup ) AS topup_row_num,
        row_number() OVER (ORDER BY da.tenure) AS tenure_row_num,
        da.k,
        da.sms,
        da.voice,
        da.topup,
        da.tenure
      FROM (
        SELECT
          coalesce(d.k,0) AS k,
          coalesce(d.smscount,0) AS sms,
          coalesce(d.voicesum,0) AS voice,
          coalesce(d.topup_count_weekly3*d.topup_amount_avg3,0) AS topup, 
          coalesce(d.contr_length,0) AS tenure
        FROM work.modelling_data_matrix AS d
        INNER JOIN (
          SELECT ctw.alias_id
          FROM data.call_types_weekly AS ctw
          WHERE coalesce(ctw.direction,'m') = 'm' -- Made calls, SMSs, etc.
          AND ctw.monday >= t1
          AND ctw.monday < t2
          GROUP BY ctw.alias_id
          HAVING count(DISTINCT ctw.monday) >= (t2-t1) / 7.0 -- Activity in every week of source period is required
        ) AS a
        ON d.alias_id = a.alias_id
        WHERE d.mod_job_id = in_mod_job_id
      ) AS da
    ) AS z
  ) AS vals;

  -- Use case score distribution 
  job_use_cases := (
    SELECT value
    FROM work.module_job_parameters 
    WHERE mod_job_id = in_mod_job_id AND key = 'job_use_cases'
  );

  job_use_cases := regexp_replace(job_use_cases, 'zero_day_prediction', 'zero_day_prediction_low_value,zero_day_prediction_medium_value,zero_day_prediction_high_value');
  use_cases := string_to_array(trim(job_use_cases),',');
  
  FOR uc_ind IN 1..array_upper(use_cases, 1) LOOP
    
    use_case := use_cases[uc_ind];
    
    sql_string := 'INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id) 
    SELECT '||
    in_mod_job_id || ' AS mod_job_id, 
    ''SCORE_DISTRIBUTION'' AS stat_name,
    ''' || use_case || ''' AS group_name, 
    now() AS data_date,
    var_name, 
    var_value, 
    NULL AS order_id
    FROM
    (SELECT 
      ROUND(' || use_case || '_propensity_score::numeric, 2)::text AS var_name,
      count(*) AS var_value
      FROM results.module_results 
      WHERE mod_job_id = ' || in_mod_job_id || '
      AND ' || use_case || '_propensity_score IS NOT NULL
      GROUP BY var_name
    ) aa;';

    EXECUTE sql_string;
  END LOOP;
  
  PERFORM core.analyze('charts.chart_data', in_mod_job_id);
 
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION results.calculate_score_list_approval_stats(integer) OWNER TO xsl;

--------------------------------------------------------------------------------------




CREATE OR REPLACE FUNCTION work.handset_lda_input_data(in_mod_job_id integer) RETURNS SETOF integer
    AS $$ 
/* 
 * A function to calculate the input data for LDA that calculates handset topics from the location profiles
 * 
 * Parameters: in_mod_job_id: module job id defining the input data period (t1...t2)
 * 
 * Reads cell_id data from data.cdr and handset model data form data.crm. 
 * Generates a new LDA input ID
 * Writes to work.lda_input and work.lda_input_parameters
 * 
 * VERSION
 * 30.11.2012 HMa
 * 
 */


DECLARE
  lda_inp_id INTEGER;  
  t1_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1'
  );  
  t2_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  tcrm_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'tcrm'
  );
--  t_start text := greatest(to_date(t1_text,'YYYY-MM-DD'),(to_date(t2_text,'YYYY-MM-DD')-'3 month'::interval))::text; -- Calculate from the last month of the source period (or shorter if shorter source period)
 
  
BEGIN
  
  SELECT core.nextval_with_partitions('work.lda_input_sequence') INTO lda_inp_id;
  
  INSERT INTO work.lda_input_parameters 
  (lda_input_id, "key", "value")
  VALUES 
  (lda_inp_id, 'LDA type', 'handset_topic_from_cellid'),
  (lda_inp_id, 'doc', 'cell_id'),
  (lda_inp_id, 'term', 'handset_model'),
  (lda_inp_id, 't_start', t1_text),
  (lda_inp_id, 't_end', t2_text);

  INSERT INTO work.lda_input_parameters
  (lda_input_id, "key", "value")
  SELECT lda_inp_id AS lda_job_id, 'input_data_inserted' AS "key", to_char(CURRENT_TIMESTAMP(0), 'YYYY-MM-DD HH24:MI:SS') AS "value";

    -- Calculate the LDA input data:
    INSERT INTO work.lda_input (lda_id, doc, term, n) 
    SELECT 
      lda_inp_id,
      cell_id::text as doc, 
      trim(lower(handset_model)) as term, 
      count(distinct alias_id) AS n
    FROM (
      SELECT 
        crm.alias_id, 
        cdr.a_cell_id AS cell_id, 
        crm.handset_model
      FROM data.in_crm AS crm
      INNER JOIN data.cdr AS cdr
      ON crm.alias_id = cdr.alias_a -- select all innet subs who made a call and whose handset model and current cell_id is known
      WHERE crm.date_inserted = to_date(tcrm_text,'YYYY-MM-DD')
      AND crm.handset_model IS NOT NULL
      AND cdr.a_cell_id IS NOT NULL
      AND cdr.call_time >= to_timestamp(t1_text,'YYYY-MM-DD') 
      AND cdr.call_time < to_timestamp(t2_text,'YYYY-MM-DD') 
      AND cdr.call_time >= crm.handset_begin_date
      UNION ALL
      SELECT 
        crm.alias_id, 
        cdr.b_cell_id AS cell_id, 
        crm.handset_model
      FROM data.in_crm AS crm
      INNER JOIN data.cdr AS cdr
      ON crm.alias_id = cdr.alias_b -- select all innet subs who received a call and whose handset model and current cell_id is known
      WHERE crm.date_inserted = to_date(tcrm_text,'YYYY-MM-DD')
      AND crm.handset_model IS NOT NULL
      AND cdr.b_cell_id IS NOT NULL
      AND cdr.call_time >= to_timestamp(t1_text,'YYYY-MM-DD') 
      AND cdr.call_time < to_timestamp(t2_text,'YYYY-MM-DD') 
      AND cdr.call_time >= crm.handset_begin_date
    ) c
    GROUP BY cell_id, handset_model;  

  RETURN NEXT lda_inp_id;

END;

$$
    LANGUAGE plpgsql;
ALTER FUNCTION work.handset_lda_input_data(integer) OWNER TO xsl;







--automatic preprocessing starts 


CREATE OR REPLACE FUNCTION core.commacat_ignore_nulls(acc text, instr double precision) RETURNS text AS $$
  BEGIN
    IF acc IS NULL OR acc = '' THEN
      RETURN instr;
    ELSIF instr IS NULL  THEN
     RETURN acc;
    ELSE
      RETURN acc || ',' || instr::text;
    END IF;
  END;
$$ LANGUAGE plpgsql;
ALTER FUNCTION core.commacat_ignore_nulls(text, double precision) OWNER TO xsl;

--DROP AGGREGATE core.commacat_all(double precision);

DROP AGGREGATE IF EXISTS core.commacat_all(double precision);

CREATE AGGREGATE core.commacat_all(
  basetype    = double precision,
  sfunc       = core.commacat_ignore_nulls,
  stype       = text,
  initcond    = ''
);

------------------------------------------------------------


  CREATE OR REPLACE FUNCTION work.insert_campaign_target_list_churn_inactivity(in_mod_job_id integer, in_target_id integer, ctrl_grp_size double precision, target_limit text, blocklist_id text, arpu_seg text[], tenure_seg text[] )
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function inserts the churn inactivity use case target list required in the
 * SL targeting page to results.module_export_churn_inactivity
 *
 * INPUT
 * in_mod_job_id:   Identifier of module job
 * in_target_id:    Identifier of the targeting job in the SL UI
 * ctrl_grp_size: (%) The size of control group included in the target list
 * target_limit:    Number of scores written to the target list (in form 'LIMIT 1000')
 * blocklist_id:    Identifier of the block list that is used to exclude subscribers from the target list. NULL if no blocklist selected. 
 * arpu_seg:  Segmentations of arpu written to the target list
 * tenure_seg: Segmentations of tenure days written to the target list
 *
 * OUTPUT
 * The query that will be used to fetch target list data
 *
 * VERSION
 * 08.07.2014 QYu Add arpu and tenure_days segmentations to target list
 * 23.05.2013 KL Modified to work in standard flow based on robi-version
 * 05.04.2013 HMa
 */

DECLARE  

  query           text;
  query_control   text;
  join_blocklist  text;
  join_arpu       text;
  join_tenure     text;
  where_blocklist text;
  return_query    text;
  sql_from_and_where_control text;
  sql_from_and_where text;
  arpu_count      integer;
  tenure_count    integer;
  arpu_string     text;
  tenure_string     text;
  arpu_allseg    boolean;
  tenure_allseg  boolean; 
  
BEGIN

  IF blocklist_id != '' THEN
    join_blocklist := 'LEFT JOIN (SELECT msisdn FROM data.blocklist WHERE id IN ('''||replace(blocklist_id, ',', ''',''')||''') ) b ON a.string_id = b.msisdn';
    where_blocklist := 'AND b.msisdn IS NULL';
  ELSE
    join_blocklist := '';
    where_blocklist := '';
  END IF;
  
  arpu_count:= count(*) from unnest(arpu_seg);
  tenure_count:= count(*) from unnest(tenure_seg);
  
 -- check arpu and tenure_days segmentations required

  arpu_string := array_to_string(arpu_seg, ', ');
  tenure_string := array_to_string(tenure_seg, ', ');
    
  arpu_allseg := arpu_string ~ 'All';
  tenure_allseg := tenure_string ~ 'All';
  
  IF  arpu_allseg THEN
  
     join_arpu := ''; 
   
  ELSEIF NOT arpu_allseg AND arpu_count=3 THEN
  
     join_arpu := '';	 
	  
  ELSEIF NOT arpu_allseg  AND arpu_count<=2 AND arpu_count>=1  THEN
	 
 	 join_arpu := 'INNER JOIN (SELECT alias_id, segmentation AS arpu_segmentation FROM work.monthly_arpu WHERE mod_job_id = ' ||in_mod_job_id|| ' AND segmentation IN ('''||array_to_string(arpu_seg, ''',''')||''')  ) ar ON  mr.alias_id = ar.alias_id' ;
	 
  ELSEIF arpu_count<1 THEN
     RETURN 
     'ERROR: Please choose at least one Arpu segmentation';     
  
  END IF;
  
  IF  tenure_allseg THEN
  
     join_tenure := ''; 
   
  ELSEIF NOT tenure_allseg AND tenure_count=4 THEN
  
     join_tenure := '';	 
	  
  ELSEIF NOT tenure_allseg AND tenure_count<=3 AND tenure_count>=1 THEN
	 
 	 join_tenure := 'INNER JOIN (SELECT alias_id, segmentation AS tenure_days_segmentation FROM work.tenure_days WHERE mod_job_id = ' ||in_mod_job_id|| ' AND segmentation IN ('''||array_to_string(tenure_seg, ''',''')||''') ) td ON  mr.alias_id = td.alias_id' ;
	 
  ELSEIF tenure_count<1 THEN
     RETURN 
     'ERROR: Please choose at least one Tenure days segmentation';     
  
  END IF;
  
  
  sql_from_and_where_control := '
  FROM results.module_results mr
  INNER JOIN aliases.string_id a
  ON mr.alias_id = a.alias_id
  ' || join_blocklist || '
  ' || join_arpu || '
  ' || join_tenure || '
  WHERE mr.mod_job_id = '||in_mod_job_id||'
  ' || where_blocklist || '
  AND churn_inactivity_propensity_score IS NOT NULL';

  query_control :='
    INSERT INTO work.target_control_groups
    SELECT '
      ||in_mod_job_id||' AS mod_job_id,
      '||in_target_id||' AS target_id, 
      a.alias_id, 
      CASE 
      WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
      ELSE ''cg'' END as target,
      ''churn_inactivity'' AS use_case '
    ||sql_from_and_where_control|| ';';


  EXECUTE query_control;

  PERFORM core.analyze('work.target_control_groups', in_mod_job_id);
   
  sql_from_and_where := '
  FROM results.module_results mr
  INNER JOIN aliases.string_id a
  ON mr.alias_id = a.alias_id
  INNER JOIN work.target_control_groups c
  ON mr.mod_job_id = c.mod_job_id AND mr.alias_id = c.alias_id
 ' || join_blocklist || '
  WHERE mr.mod_job_id = '||in_mod_job_id||'
    AND c.target_id = ' ||in_target_id||' '
   || where_blocklist || '
  AND churn_inactivity_propensity_score IS NOT NULL';

  query := '
     INSERT INTO results.module_export_churn_inactivity
     SELECT '
       ||in_mod_job_id||' AS mod_job_id, '
       ||in_target_id||' AS target_id, 
       a.string_id AS msisdn, 
       mr.churn_inactivity_propensity_score, 
       mr.churn_inactivity_expected_revenue_loss,
       c.target, 
       NULL AS group_id,
       CURRENT_DATE AS delivery_date, 
       row_number() over(order by churn_inactivity_propensity_score DESC) AS order_id
     ' || sql_from_and_where || '
     ORDER BY churn_inactivity_propensity_score DESC
     ' || (select work.calculateLimit(target_limit, sql_from_and_where));




  EXECUTE query;

  PERFORM core.analyze('results.module_export_churn_inactivity', in_mod_job_id);

  return_query := 
    'SELECT 
       msisdn, 
       churn_inactivity_propensity_score, 
       churn_inactivity_expected_revenue_loss, 
       target, 
       delivery_date
     FROM results.module_export_churn_inactivity
     WHERE mod_job_id ='||in_mod_job_id||'
     AND target_id = '||in_target_id||'
     ORDER BY order_id';

  IF (SELECT value FROM work.module_job_parameters AS mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'calculate_vargroup_output' AND mjp.value = 'true' LIMIT 1) IS NOT NULL THEN
    return_query = (SELECT * FROM work.append_variable_group_output_to_target_query(in_mod_job_id, return_query, 'churn_inactivity'));
  END IF;
     
  RETURN return_query;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.insert_campaign_target_list_churn_inactivity(integer, integer, double precision, text, text,text[],text[])
  OWNER TO xsl;  


-- Function: work.insert_campaign_target_list_churn_postpaid(integer, integer, double precision, text, text)

-- DROP FUNCTION work.insert_campaign_target_list_churn_postpaid(integer, integer, double precision, text, text);

CREATE OR REPLACE FUNCTION work.insert_campaign_target_list_churn_postpaid(in_mod_job_id integer, in_target_id integer, ctrl_grp_size double precision, target_limit text, blocklist_id text, arpu_seg text[], tenure_seg text[])
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function inserts the churn inactivity use case target list required in the
 * SL targeting page to results.module_export_churn_postpaid
 *
 * INPUT
 * in_mod_job_id:   Identifier of module job
 * in_target_id:    Identifier of the targeting job in the SL UI
 * ctrl_grp_size: (%) The size of control group included in the target list
 * target_limit:    Number of scores written to the target list (in form 'LIMIT 1000')
 * blocklist_id:    Identifier of the block list that is used to exclude subscribers  *                   from the target list. NULL if no blocklist selected. 
 * arpu_seg:  Segmentations of arpu written to the target list
 * tenure_seg: Segmentations of tenure days written to the target list
 *
 * OUTPUT
 * The query that will be used to fetch target list data
 *
 * VERSION
 * 08.07.2014 QYu Add arpu and tenure_days segmentations to target list
 * 23.05.2013 KL Modified to work in standard flow based on robi-version
 * 05.04.2013 HMa
 */

DECLARE  

  query           text;
  query_control   text;
  join_blocklist  text;
  join_arpu       text;
  join_tenure     text;
  where_blocklist text;
  return_query    text;
  sql_from_and_where_control text;
  sql_from_and_where text;
  arpu_count      integer;
  tenure_count    integer;
  arpu_string     text;
  tenure_string     text;
  arpu_allseg    boolean;
  tenure_allseg  boolean; 

BEGIN

  IF blocklist_id != '' THEN
    join_blocklist := 'LEFT JOIN (SELECT msisdn FROM data.blocklist WHERE id IN ('''||replace(blocklist_id, ',', ''',''')||''') ) b ON a.string_id = b.msisdn';
    where_blocklist := 'AND b.msisdn IS NULL';
  ELSE
    join_blocklist := '';
    where_blocklist := '';
  END IF;

  arpu_count:= count(*) from unnest(arpu_seg);
  tenure_count:= count(*) from unnest(tenure_seg);
  
 -- check arpu and tenure_days segmentations required

  arpu_string := array_to_string(arpu_seg, ', ');
  tenure_string := array_to_string(tenure_seg, ', ');
  
  arpu_allseg := arpu_string ~ 'All';
  tenure_allseg := tenure_string ~ 'All';
  
  IF  arpu_allseg THEN
  
     join_arpu := ''; 
   
   ELSEIF NOT arpu_allseg AND arpu_count=3 THEN
  
     join_arpu := '';	 
	  
   ELSEIF NOT arpu_allseg  AND arpu_count<=2 AND arpu_count>=1  THEN
	 
 	 join_arpu := 'INNER JOIN (SELECT alias_id, segmentation AS arpu_segmentation FROM work.monthly_arpu WHERE mod_job_id = ' ||in_mod_job_id|| ' AND segmentation IN ('''||array_to_string(arpu_seg, ''',''')||''')  ) ar ON  mr.alias_id = ar.alias_id' ;
	 
   ELSEIF arpu_count<1 THEN
   
     RETURN 
     'ERROR: Please choose at least one Arpu segmentation';     
  
  END IF;
  
  IF  tenure_allseg THEN
  
     join_tenure := ''; 
   
  ELSEIF NOT tenure_allseg AND tenure_count=4 THEN
  
     join_tenure := '';	 
	  
  ELSEIF NOT tenure_allseg AND tenure_count<=3 AND tenure_count>=1 THEN
	 
 	 join_tenure := 'INNER JOIN (SELECT alias_id, segmentation AS tenure_days_segmentation FROM work.tenure_days WHERE mod_job_id = ' ||in_mod_job_id|| ' AND segmentation IN ('''||array_to_string(tenure_seg, ''',''')||''') ) td ON  mr.alias_id = td.alias_id' ;
	 
  ELSEIF tenure_count<1 THEN
     RETURN 
     'ERROR: Please choose at least one Tenure days segmentation';     
  
  END IF;
  
  sql_from_and_where_control := '
    FROM results.module_results m
    INNER JOIN aliases.string_id a
    ON m.alias_id = a.alias_id
    ' || join_blocklist || '
	' || join_arpu || '
    ' || join_tenure || '
    WHERE mod_job_id = '||in_mod_job_id||'
    ' || where_blocklist || '
    AND churn_postpaid_propensity_score IS NOT NULL';


  query_control :='
    INSERT INTO work.target_control_groups
    SELECT '
      ||in_mod_job_id||' AS mod_job_id,
      '||in_target_id||' AS target_id, 
      a.alias_id, 
      CASE 
      WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
      ELSE ''cg'' END as target,
      ''churn_postpaid'' AS use_case'
    ||sql_from_and_where_control|| ';';


  EXECUTE query_control;

  PERFORM core.analyze('work.target_control_groups', in_mod_job_id);
   
  sql_from_and_where := '
  FROM results.module_results m
  INNER JOIN aliases.string_id a
  ON m.alias_id = a.alias_id
  INNER JOIN work.target_control_groups c
  ON m.mod_job_id = c.mod_job_id AND m.alias_id = c.alias_id
  ' || join_blocklist || '
  WHERE m.mod_job_id = '||in_mod_job_id||'
    AND c.target_id = ' ||in_target_id||' '
   || where_blocklist || '
  AND churn_postpaid_propensity_score IS NOT NULL';


  query := '
     INSERT INTO results.module_export_churn_postpaid 
     SELECT '
       ||in_mod_job_id||' AS mod_job_id, '
       ||in_target_id||' AS target_id, 
       a.string_id AS msisdn, 
       m.churn_postpaid_propensity_score, 
       c.target, 
      NULL AS group_id,
      CURRENT_DATE AS delivery_date, 
      row_number() over(order by churn_postpaid_propensity_score DESC) AS order_id,
      m.churn_postpaid_expected_revenue_loss
    ' || sql_from_and_where || '
    ORDER BY churn_postpaid_propensity_score DESC
    ' || (select work.calculateLimit(target_limit, sql_from_and_where));



  EXECUTE query;

  PERFORM core.analyze('results.module_export_churn_postpaid', in_mod_job_id);

  return_query := 
    'SELECT 
       msisdn, 
       churn_postpaid_propensity_score, 
       churn_postpaid_expected_revenue_loss,
       target, 
       delivery_date
     FROM results.module_export_churn_postpaid
     WHERE mod_job_id ='||in_mod_job_id||'
     AND target_id = '||in_target_id||'
     ORDER BY order_id';

  IF (SELECT value FROM work.module_job_parameters AS mjp WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 'calculate_vargroup_output' AND mjp.value = 'true' LIMIT 1) IS NOT NULL THEN
    return_query = (SELECT * FROM work.append_variable_group_output_to_target_query(in_mod_job_id, return_query, 'churn_postpaid'));
  END IF;

     
  RETURN return_query;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.insert_campaign_target_list_churn_postpaid(integer, integer, double precision, text, text,text[],text[])
  OWNER TO xsl;

CREATE OR REPLACE FUNCTION work.insert_campaign_target_list_product(in_mod_job_id integer, in_target_id integer, target_limit text, products text, ctrl_grp_size double precision, blocklist_id text)
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function generates the target query that is used to fetch target list in the
 * SL target tab for the Best next product usecase. 
 *
 * INPUT
 * in_mod_job_id: Identifier of module job
 * in_target_id:     Identifier of the targetting job in the SL UI
 * target_limit:  Number of scores written to the target list
 * products:      A comma-separated list of product names whose propensity scores are written to the target list
 * ctrl_grp_size: (%) The size of control group included in the target list
 *
 * OUTPUT
 * The query that will be used to fetch target list data
 *
 * VERSION
 * 23.05.2013 KL  - Removing msisdn:s that are on blocklist(s)
 * 28.02.2013 HMa - If target calculated from the source period, leave out subscribers who already have the product
 * 27.02.2013 HMa - Matching product ID with product name from table data.product_information
 * 06.02.2013 HMa 
 */

DECLARE  
 
  case_block text;
  round_block text;
  greatest_block text;
  score_block text;
  scorename_block text;
  where_block text;
  column_list text;

  target_calculated_from text;

  product_array   text[];
  productname    text;
  productid      text;

  all_products boolean;
  
  i integer;

  join_blocklist  text;
  where_blocklist text;

  query           text;
  query_ret       text;
  sql_from_and_where text;

BEGIN

  IF blocklist_id != '' THEN
    join_blocklist := 'LEFT JOIN (SELECT msisdn FROM data.blocklist WHERE id IN ('''||replace(blocklist_id, ',', ''',''')||''') ) b ON a.string_id = b.msisdn';
    where_blocklist := 'AND b.msisdn IS NULL';
  ELSE
    join_blocklist := '';
    where_blocklist := '';
  END IF;

  product_array := string_to_array(products,',');

  target_calculated_from := value FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id AND key = 'uc_product_target_calculated_from';
 
  -- Or should we include all the products in the output if no products have been chosen?
  IF array_upper(product_array,1) IS NULL THEN

    RAISE NOTICE 'No products given for target list creation. Including all the products.';
    product_array := string_to_array(value,',') FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id AND key = 'uc_product_products';
    IF product_array IS NULL THEN
      RAISE EXCEPTION 'No products are included in the run related to this score list!';
    END IF;
    all_products := TRUE;
  ELSE
    all_products := FALSE;
  END IF;

  IF array_upper(product_array,1) = 1 THEN

    IF all_products THEN
      productid := trim(products);
      productname := product_name FROM data.product_information WHERE product_id = productid;
    ELSE
      productname := trim(products);
      productid := product_id FROM data.product_information WHERE replace(product_name,' ','_') = productname;
    END IF;

    IF productid IS NULL THEN 
      RAISE EXCEPTION 'Given product name does not match with products in the table data.product_information!';
    END IF;

    IF target_calculated_from = 'target' THEN

      sql_from_and_where := '
      FROM results.module_results m
      INNER JOIN aliases.string_id a
      ON m.alias_id = a.alias_id
      '||join_blocklist||'
      WHERE mod_job_id = '||in_mod_job_id||'
      AND product_'||productid||'_propensity_score IS NOT NULL
      '||where_blocklist;

      query :=
     'INSERT INTO results.module_export_product (
       mod_job_id,
       target_id,
       msisdn,
       '||replace(lower(trim(productname)),' ','_')||'_propensity, 
       target,
       delivery_date,
       order_id
     )
     SELECT 
        '||in_mod_job_id||' AS mod_job_id, 
        '||in_target_id||' AS target_id, 
        a.string_id AS msisdn, 
        round(m.product_'||productid||'_propensity_score::numeric, 4) as '||productid||'_propensity,
        CASE 
          WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
          ELSE ''cg''
        END AS target, 
        CURRENT_DATE AS delivery_date, 
        row_number() over(order by product_'||productid||'_propensity_score DESC) AS order_id
      ' || sql_from_and_where || '
      ORDER BY product_'||productid||'_propensity_score DESC
      ' || (select work.calculateLimit(target_limit, sql_from_and_where));

    ELSE -- target calculated from source: remove subscribers who already have the product from the target list

      sql_from_and_where := '
      FROM results.module_results m
      INNER JOIN aliases.string_id a
      ON m.alias_id = a.alias_id
      INNER JOIN work.module_targets mt
      ON m.alias_id = mt.alias_id
      '||join_blocklist||'
      WHERE m.mod_job_id = '||in_mod_job_id||'
      AND mt.mod_job_id = '||in_mod_job_id||'
      AND mt.target_product_'||productid||' != 1 -- only subs who do not have the product already
      AND m.product_'||productid||'_propensity_score IS NOT NULL
      '||where_blocklist;

      query :=
     'INSERT INTO results.module_export_product (
       mod_job_id,
       target_id,
       msisdn,
       '||replace(lower(trim(productname)),' ','_')||'_propensity, 
       target,
       delivery_date,
       order_id
     )
     SELECT 
        '||in_mod_job_id||' AS mod_job_id, 
        '||in_target_id||' AS target_id, 
        a.string_id AS msisdn, 
        round(m.product_'||productid||'_propensity_score::numeric, 4) as '||productid||'_propensity,
        CASE 
          WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
          ELSE ''cg''
        END AS target, 
        CURRENT_DATE AS delivery_date, 
        row_number() over(order by product_'||productid||'_propensity_score DESC) AS order_id
      ' || sql_from_and_where || '
      ORDER BY m.product_'||productid||'_propensity_score DESC
      ' || (select work.calculateLimit(target_limit, sql_from_and_where));

    END IF;

  END IF;

  IF array_upper(product_array,1) > 1 THEN -- several products

    -- Initialize strings: 
    case_block := '';
    round_block := '';
    scorename_block := '';
    score_block := '';
    where_block := '';
	column_list = '(mod_job_id, target_id, msisdn, highest_propensity, highest_propensity_product, ';

    -- Loop over products:
    FOR i IN 1..array_upper(product_array,1) LOOP
      
      productname := trim(product_array[i]);
    
      RAISE NOTICE 'Product: %', productname;

      IF all_products THEN
        productid := productname;
        productname := product_name FROM data.product_information WHERE product_id = productname;
      ELSE
        productid := product_id FROM data.product_information WHERE replace(product_name,' ','_') = productname;
      END IF;

      IF productid IS NULL THEN 
        RAISE EXCEPTION 'Given product name does not match with products in the target list!';
      END IF;

      IF target_calculated_from = 'target' THEN

        case_block := case_block || 'WHEN product_'||productid||'_propensity_score = highest_propensity THEN '''||productname||''' ';
        round_block := round_block || 'round(product_'||productid||'_propensity_score::numeric, 4) AS '||replace(lower(trim(productname)),' ','_')||'_propensity, ';
        scorename_block := scorename_block || 'm.product_'||productid||'_propensity_score, ';
        where_block := where_block || 'm.product_'||productid||'_propensity_score IS NOT NULL OR ';
		column_list := column_list || productid || '_propensity, ';

      ELSE -- target_calculated_from = 'source': remove propensity scores from subscribers who already have the product 

        case_block      := case_block || 'WHEN product_'||productid||'_propensity_score = highest_propensity THEN '''||productname||''' ';
        round_block     := round_block || 'round(product_'||productid||'_propensity_score::numeric, 4) AS '||replace(lower(trim(productname)),' ','_')||'_propensity, ';
        scorename_block := scorename_block || 'product_'||productid||'_propensity_score, ';
        score_block     := score_block || 'CASE WHEN mt.target_product_'||productid||' != 1 THEN m.product_'||productid||'_propensity_score ELSE NULL END AS product_'||productid||'_propensity_score, ';
        where_block     := where_block || 'm.product_'||productid||'_propensity_score IS NOT NULL OR ';
		column_list     := column_list || productid || '_propensity, ';

      END IF;

    END LOOP;

    greatest_block := trim(TRAILING ', ' FROM scorename_block);
    where_block := trim(TRAILING 'OR ' FROM where_block);
	column_list := column_list || 'target, group_id, delivery_date, order_id) ';

    IF target_calculated_from = 'target' THEN

      sql_from_and_where := '
         FROM results.module_results m
         INNER JOIN aliases.string_id a
         ON m.alias_id = a.alias_id
         '||join_blocklist||'
         WHERE m.mod_job_id = '||in_mod_job_id||'
         AND ('||where_block||')
         '||where_blocklist;

      -- Build the query:
      query :=
      'INSERT INTO results.module_export_product ' || column_list || '
      SELECT
        '||in_mod_job_id||' AS mod_job_id, 
        '||in_target_id||' AS target_id, 
         msisdn,
         round(highest_propensity::numeric, 4) AS highest_propensity,
         CASE ' || case_block ||'
           ELSE NULL
         END AS highest_propensity_product, '
         ||round_block||'
         CASE 
          WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
          ELSE ''cg''
         END AS target, 
         NULL AS group_id, 
         delivery_date,
         row_number() OVER(ORDER BY highest_propensity DESC) AS order_id
       FROM (
         SELECT a.string_id AS msisdn,
           greatest('||greatest_block||') AS highest_propensity, '
           ||scorename_block||'
           CURRENT_DATE AS delivery_date
         '|| sql_from_and_where ||'
         '|| (select work.calculateLimit(target_limit, sql_from_and_where)) ||'
       ) b
       ORDER BY highest_propensity DESC';

    ELSE -- target_calculated_from = 'source': remove propensity scores from subscribers who already have the product 

      scorename_block := trim(TRAILING ', ' FROM scorename_block);
      score_block     := trim(TRAILING ', ' FROM score_block);

      sql_from_and_where := '
           FROM results.module_results m
           INNER JOIN aliases.string_id a
           ON m.alias_id = a.alias_id
           INNER JOIN work.module_targets mt
           ON m.alias_id = mt.alias_id
           '||join_blocklist||'
           WHERE m.mod_job_id = '||in_mod_job_id||'
           '||where_blocklist||'
           AND mt.mod_job_id = '||in_mod_job_id||'
           AND ('||where_block||') ';

      -- Build the query:
      query :=
      'INSERT INTO results.module_export_product ' || column_list || '
      SELECT
        '||in_mod_job_id||' AS mod_job_id, 
        '||in_target_id||' AS target_id, 
         msisdn,
         round(highest_propensity::numeric, 4) AS highest_propensity,
         CASE ' || case_block ||'
           ELSE NULL
         END AS highest_propensity_product, '
         ||round_block||'
         CASE 
          WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
          ELSE ''cg''
         END AS target, 
         NULL AS group_id, 
         delivery_date,
         row_number() OVER(ORDER BY highest_propensity DESC) AS order_id
       FROM (
         SELECT 
           msisdn, 
           greatest('||greatest_block||') AS highest_propensity, 
           '||scorename_block||',
           delivery_date
         FROM (
           SELECT a.string_id AS msisdn,
             '||score_block||', 
             CURRENT_DATE AS delivery_date
           '|| sql_from_and_where ||'
           '|| (select work.calculateLimit(target_limit, sql_from_and_where)) ||'
         ) c
         WHERE greatest('||scorename_block||') IS NOT NULL  -- Leave out subscribers who have all the products
       ) b
       ORDER BY highest_propensity DESC';

    END IF;

  END IF; 

  EXECUTE query;

query_ret= '
  SELECT 
    msisdn, 
    highest_propensity,
    highest_propensity_product,
    product_x_propensity, 
    product_y_propensity, 
    target, 
    delivery_date
  FROM results.module_export_product
  WHERE mod_job_id = '||in_mod_job_id||' 
  AND target_id = '||in_target_id||' 
  ORDER BY order_id';

  RETURN query_ret;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.insert_campaign_target_list_product(in_mod_job_id integer, target_id integer, target_limit text, products text, ctrl_grp_size double precision, blocklist_id text) OWNER TO xsl;


CREATE OR REPLACE FUNCTION work.insert_campaign_target_list_zero_day_prediction (
  in_mod_job_id integer, 
  in_target_id integer, 
  target_limit text, 
  ctrl_grp_size double precision,
  order_name text, 
  blocklist_id text
  
)
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function inserts the zero-day prediction use case target list required in the
 * SL targeting page to results.module_export_zero_day_prediction
 *
 * INPUT
 * in_mod_job_id:   Identifier of module job
 * in_target_id:    Identifier of the targeting job in the SL UI
 * target_limit:    Number of scores written to the target list (in form 'LIMIT 1000')
 * ctrl_grp_size:   (%) The size of control group included in the target list
 * order_name:      Propensity score that is used to order the target list: 'Highest propensity', 'High value segment propensity', 'Medium value segment propensity' or 'Low value segment propensity' 
 *
 * OUTPUT
 * The query that will be used to fetch target list data
 *
 * VERSION
 * 23.05.2013 KL : Added removing msisd:s in blocklist from targetlist
 * 08.03.2013 HMa: Order the target list according to the given order_name
 * 06.03.2013 JVi: Created 
 */

DECLARE  

  query           text;
  order_score     text;
  query_ret       text;

  join_blocklist  text;
  where_blocklist text;
  sql_from_and_where text;

BEGIN

  IF blocklist_id != '' THEN
    join_blocklist := 'LEFT JOIN (SELECT msisdn FROM data.blocklist WHERE id IN ('''||replace(blocklist_id, ',', ''',''')||''') ) b ON si.string_id = b.msisdn';
    where_blocklist := ' WHERE b.msisdn IS NULL';
  ELSE
    join_blocklist := '';
    where_blocklist := '';
  END IF;
   
  
  IF    order_name = 'Highest propensity'              THEN order_score := 'highest_propensity';
  ELSIF order_name = 'High value segment propensity'   THEN order_score := 'high_value_segment_propensity';
  ELSIF order_name = 'Medium value segment propensity' THEN order_score := 'medium_value_segment_propensity';
  ELSIF order_name = 'Low value segment propensity'    THEN order_score := 'low_value_segment_propensity';
  ELSE 
    RAISE EXCEPTION 'Zero-day prediction target list should be ordered according to ''Highest propensity'', ''High value segment propensity'', ''Medium value segment propensity'' or ''Low value segment propensity''.';
  END IF;

  sql_from_and_where = '
     FROM (
       SELECT
         alias_id,
         CASE WHEN   zero_day_prediction_low_value_propensity_score    >= zero_day_prediction_medium_value_propensity_score
                AND  zero_day_prediction_low_value_propensity_score    >= zero_day_prediction_high_value_propensity_score
              THEN   round(zero_day_prediction_low_value_propensity_score::numeric, 4)
              WHEN   zero_day_prediction_medium_value_propensity_score >  zero_day_prediction_low_value_propensity_score
                AND  zero_day_prediction_medium_value_propensity_score >= zero_day_prediction_high_value_propensity_score
              THEN   round(zero_day_prediction_medium_value_propensity_score::numeric, 4)
              ELSE   round(zero_day_prediction_high_value_propensity_score::numeric, 4) END
              AS highest_propensity,
         round(zero_day_prediction_low_value_propensity_score::numeric,4)             AS low_value_segment_propensity,
         round(zero_day_prediction_medium_value_propensity_score::numeric,4)          AS medium_value_segment_propensity,
         round(zero_day_prediction_high_value_propensity_score::numeric,4)            AS high_value_segment_propensity
       FROM results.module_results mr0
       WHERE mr0.mod_job_id = '||in_mod_job_id||'
       AND mr0.zero_day_prediction_predicted_segment IS NOT NULL
     ) AS mr
     INNER JOIN aliases.string_id AS si
     ON mr.alias_id = si.alias_id
     '|| join_blocklist||'
     '|| where_blocklist;

  query :=
    'INSERT INTO results.module_export_zero_day_prediction (
       mod_job_id, 
       target_id, 
       msisdn, 
       highest_propensity, 
       highest_propensity_segment, 
       low_value_segment_propensity, 
       medium_value_segment_propensity, 
       high_value_segment_propensity, 
       target, 
       delivery_date, 
       order_id
     )
     SELECT 
       '||in_mod_job_id||'                 AS mod_job_id,
       '||in_target_id||'                  AS target_id,
       si.string_id                        AS msisdn,
       mr.highest_propensity               AS highest_propensity,
       CASE WHEN mr.highest_propensity = mr.low_value_segment_propensity    THEN ''low_value''
            WHEN mr.highest_propensity = mr.medium_value_segment_propensity THEN ''medium_value''
            WHEN mr.highest_propensity = mr.high_value_segment_propensity   THEN ''high_value'' END 
                                           AS highest_propensity_segment,
       mr.low_value_segment_propensity     AS low_value_segment_propensity,   
       mr.medium_value_segment_propensity  AS medium_value_segment_propensity,
       mr.high_value_segment_propensity    AS high_value_segment_propensity,
       CASE WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
            ELSE ''cg'' END   AS target,
       CURRENT_DATE AS delivery_date,
       row_number() OVER(ORDER BY '||order_score||' DESC) AS order_id
      '|| sql_from_and_where ||'
      ORDER BY '||order_score||' DESC
      '|| (select work.calculateLimit(target_limit, sql_from_and_where));

  EXECUTE query;

  query_ret := '
    SELECT 
      msisdn, 
      highest_propensity, 
      highest_propensity_segment, 
      low_value_segment_propensity, 
      medium_value_segment_propensity, 
      high_value_segment_propensity, 
      target, 
      delivery_date, 
      order_id
    FROM results.module_export_zero_day_prediction
    WHERE mod_job_id = '||in_mod_job_id||' 
    AND target_id = ' ||in_target_id||'
    ORDER BY order_id';


  RETURN query_ret;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.insert_campaign_target_list_zero_day_prediction (
  in_mod_job_id integer, 
  in_target_id integer, 
  target_limit text, 
  ctrl_grp_size double precision,
  order_name text,
  blocklist_id text
) OWNER TO xsl;



-------------------------------------------------------------------------



CREATE OR REPLACE FUNCTION work.insert_campaign_target_list_sni(
  in_mod_job_id integer, 
  in_target_id integer, 
  target_limit text, 
  ctrl_grp_size double precision, 
  descvar text, 
  order_name text, 
  min_val double precision, 
  max_val double precision,
  blocklist_id text,
  arpu_seg text[], 
  tenure_seg text[]
)
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function generates the target query that is used to fetch target list in the
 * SL target tab for the Social network insight usecase. 
 *
 * INPUT
 * in_mod_job_id: Identifier of module job
 * in_target_id:  Identifier of the targetting job in the SL UI
 * target_limit:  Target limit statement (number of scores written to the target list), e.g., 'LIMIT 1000'
 * ctrl_grp_size: (%) The size of control group included in the target list
 * descvar:       An optional name of a descriptive variable that is used to order and/or filter the list
 * order_name:    'Descending' or 'Ascending' to order the output list accordingly. Other values result in non-ordered list
 * min_val:       Minimum value of 'descvar' included in the target list
 * max_val:       Maximum value of 'descvar' included in the target list
 * blocklist_id:  Blocklist id(s)
 *
 * OUTPUT
 * The query that will be used to fetch target list data
 *
 * VERSION
 * 08.07.2014 QYu Add arpu and tenure_days segmentations to target list
 * 21.08.2013 QYu: Added data usage related SNI.
 * 23.05.2013 KL : Added blocklist functionality
 * 13.05.2013 HMa: Changed the order of output columns
 * 07.03.2013 HMa
 */

DECLARE  

  variables       text;
  query           text;
  query_order     text;
  query_order_id  text;
  query_min       text;
  query_max       text;
  descvar_colname text;
  query_where     text;
  query_return    text;
  query_ret_order text;
  join_blocklist  text;
  join_arpu       text;
  join_tenure     text;
  where_blocklist text;
  sql_from_and_where text;
  arpu_count      integer;
  tenure_count    integer;
  arpu_string     text;
  tenure_string     text;
  arpu_allseg    boolean;
  tenure_allseg  boolean; 

BEGIN

  IF blocklist_id != '' THEN
    join_blocklist := 'LEFT JOIN (SELECT msisdn FROM data.blocklist WHERE id IN ('''||replace(blocklist_id, ',', ''',''')||''') ) b ON a.string_id = b.msisdn';
    where_blocklist := 'AND b.msisdn IS NULL';
  ELSE
    join_blocklist := '';
    where_blocklist := '';
  END IF;
   

  IF descvar IS NOT NULL THEN

    descvar_colname := var_name FROM work.descriptive_variables_list WHERE long_name = descvar;

    IF descvar_colname IS NOT NULL THEN
      -- Ordering:
      IF order_name = 'Ascending' THEN
        query_order_id := 'row_number() over(order by '||descvar_colname||') AS order_id';
        query_order    := 'ORDER BY '||descvar_colname;
        query_ret_order := 'ORDER BY order_id';
        query_where := 'AND '||descvar_colname||' IS NOT NULL';
      ELSIF order_name = 'Descending' THEN
        query_order_id := 'row_number() over(order by '||descvar_colname||' desc) AS order_id';
        query_order    := 'ORDER BY '||descvar_colname||' desc';
        query_ret_order := 'ORDER BY order_id';
        query_where := 'AND '||descvar_colname||' IS NOT NULL';
      ELSE
        query_order_id := 'NULL AS order_id';
        query_order := '';
        query_ret_order := '';
        query_where := '';
      END IF;

      -- Min value: 
      IF min_val IS NOT NULL THEN
        query_min := 'AND '||descvar_colname||' >= '||min_val;
      ELSE
        query_min := '';
      END IF;    

      -- Max value: 
      IF max_val IS NOT NULL THEN
        query_max := 'AND '||descvar_colname||' <= '||max_val;
      ELSE
        query_max := '';
      END IF;  

    ELSE

      RAISE EXCEPTION 'The given descriptive variable name is not valid.';

    END IF;

  ELSE

    query_order_id := 'NULL AS order_id';
    query_order := '';
    query_ret_order := '';
    query_min := '';
    query_max := '';
    query_where := '';

  END IF;
  
  
   arpu_count:= count(*) from unnest(arpu_seg);
  tenure_count:= count(*) from unnest(tenure_seg);
  
 -- check arpu and tenure_days segmentations required

  arpu_string := array_to_string(arpu_seg, ', ');
  tenure_string := array_to_string(tenure_seg, ', ');
    
  arpu_allseg := arpu_string ~ 'All';
  tenure_allseg := tenure_string ~ 'All';
  
  IF  arpu_allseg THEN
  
     join_arpu := ''; 
   
  ELSEIF NOT arpu_allseg AND arpu_count=3 THEN
  
     join_arpu := '';	 
	  
  ELSEIF NOT arpu_allseg  AND arpu_count<=2 AND arpu_count>=1  THEN
	 
 	 join_arpu := 'INNER JOIN (SELECT alias_id, segmentation AS arpu_segmentation FROM work.monthly_arpu WHERE mod_job_id = ' ||in_mod_job_id|| ' AND segmentation IN ('''||array_to_string(arpu_seg, ''',''')||''')  ) ar ON  mr.alias_id = ar.alias_id' ;
	 
  ELSEIF arpu_count<1 THEN
     RETURN 
     'ERROR: Please choose at least one Arpu segmentation';     
  
  END IF;
  
  IF  tenure_allseg THEN
  
     join_tenure := ''; 
   
  ELSEIF NOT tenure_allseg AND tenure_count=4 THEN
  
     join_tenure := '';	 
	  
  ELSEIF NOT tenure_allseg AND tenure_count<=3 AND tenure_count>=1 THEN
	 
 	 join_tenure := 'INNER JOIN (SELECT alias_id, segmentation AS tenure_days_segmentation FROM work.tenure_days WHERE mod_job_id = ' ||in_mod_job_id|| ' AND segmentation IN ('''||array_to_string(tenure_seg, ''',''')||''') ) td ON  mr.alias_id = td.alias_id' ;
	 
  ELSEIF tenure_count<1 THEN
     RETURN 
     'ERROR: Please choose at least one Tenure days segmentation';     
  
  END IF;
  
  

  sql_from_and_where := '
  FROM results.descriptive_variables_subscriber_matrix d
  INNER JOIN aliases.string_id a
  ON d.alias_id = a.alias_id
  '||join_blocklist||'
  '|| join_arpu ||'
  '|| join_tenure ||'
  WHERE mod_job_id = '||in_mod_job_id||'
  '||query_where||'
  '||where_blocklist||'
  '||query_max||'
  '||query_min;

  query := '
  INSERT INTO results.module_export_sni
  SELECT 
    '||in_mod_job_id||' AS mod_job_id,
    '||in_target_id||' AS target_id,
    a.string_id AS msisdn,
    d.connectedness_of_connections, -- the order of output is not determined here but in the output query
    d.connections,
    d.connections_of_onnet_connections,
    d.voice_made_duration,
    d.voice_rec_duration,
    d.voice_made_nmb_day,
    d.voice_made_nmb_eve,
    d.voice_made_nmb_night,
    d.voice_made_nmb_weekday,
    d.voice_made_nmb_weekend,
    d.voice_made_duration_day,
    d.voice_made_duration_eve,
    d.voice_made_duration_night,
    d.voice_made_duration_weekday,
    d.voice_made_duration_weekend,
    d.new_subs_within_connections,
    d.voice_made_nmb,
    d.voice_rec_nmb,
    d.sms_made_nmb,
    d.sms_rec_nmb,
    d.offnet_connections,
    d.offnet_connections_of_onnet_connections,
    d.sms_made_day,
    d.sms_made_eve,
    d.sms_made_night,
    d.sms_made_weekday,
    d.sms_made_weekend,
    d.share_of_offnet_connections,
    d.share_of_voice,
    d.social_connectivity_score,
    d.social_revenue,
    d.social_role,
    d.no_connections,
    d.neigh_count_made,
    d.neigh_count_rec,
    d.neigh_count_made_weekly,
    d.neigh_count_rec_weekly,
    d.share_of_onnet_made,
    d.share_of_onnet_rec,
    d.topup_count,
    d.topup_amount,
    d.topup_typical,
    d.topup_shareof_typical,
    d.topup_count_firsthalf_lastmonth,
    d.topup_count_lasthalf_lastmonth,
    d.topup_count_day,
    d.topup_count_eve,
    d.topup_count_night,
    d.topup_count_weekday,
    d.topup_count_weekend,
    d.topup_days_from_last,
	d.data_usage_day,
    d.data_usage_weekday,
    d.data_usage_eve,
    d.data_usage_night,
    d.data_usage_weekend,
    d.weekly_data_usage,
    d.weekly_data_usage_cost,
    --d.weekly_cost,
    --d.share_of_data_usage, 
    CASE 
      WHEN random() > '||ctrl_grp_size||'::double precision / 100 THEN ''tg''
      ELSE ''cg''
    END AS target, 
    NULL AS group_id,
    CURRENT_DATE AS delivery_date, 
    '||query_order_id||'
  '||sql_from_and_where||'
  '||query_order||'
  '|| (select work.calculateLimit(target_limit, sql_from_and_where));

  EXECUTE query;

  query_return := '
  SELECT
    d.msisdn,
    d.social_connectivity_score, -- the order and headers of the output file are determined here:
    d.neigh_count_made AS total_number_of_ids_called,
    d.neigh_count_rec AS total_number_of_ids_who_have_called,
    d.neigh_count_made_weekly AS weekly_number_of_ids_called,
    d.neigh_count_rec_weekly AS weekly_number_of_ids_who_have_called,
    d.no_connections AS subscriber_with_no_connections,
    d.connections,
    d.connections_of_onnet_connections,
    d.offnet_connections_of_onnet_connections,
    d.connectedness_of_connections,
    d.new_subs_within_connections AS new_subscribers_within_connections,
    d.social_revenue,
    d.offnet_connections,
    d.share_of_offnet_connections,
    d.social_role,
    d.share_of_onnet_made AS share_of_onnet_calls_among_calls_made,
    d.share_of_onnet_rec share_of_onnet_calls_among_calls_received,
    d.voice_made_nmb AS number_of_calls_made,
    d.voice_made_duration AS duration_of_calls_made,
    d.voice_rec_nmb AS number_of_calls_received,
    d.voice_rec_duration AS duration_of_calls_received,
    d.sms_made_nmb AS number_of_smses_sent,
    d.sms_rec_nmb AS number_of_smses_received,
    d.share_of_voice AS share_of_voice_activity,
    d.voice_made_nmb_day AS voice_calls_made_nmb_daytime,
    d.voice_made_nmb_eve AS voice_calls_made_nmb_evening,
    d.voice_made_nmb_night AS voice_calls_made_nmb_nighttime,
    d.voice_made_nmb_weekday AS voice_calls_made_nmb_weekday,
    d.voice_made_nmb_weekend AS voice_calls_made_nmb_weekend,
    d.voice_made_duration_day AS voice_calls_made_vol_daytime,
    d.voice_made_duration_eve AS voice_calls_made_vol_evening,
    d.voice_made_duration_night AS voice_calls_made_vol_nighttime,
    d.voice_made_duration_weekday AS voice_calls_made_vol_weekday,
    d.voice_made_duration_weekend AS voice_calls_made_vol_weekend,
    d.sms_made_day AS smses_sent_daytime,
    d.sms_made_eve AS smses_sent_evening,
    d.sms_made_night AS smses_sent_nighttime,
    d.sms_made_weekday AS smses_sent_weekday,
    d.sms_made_weekend AS smses_sent_weekend,
    d.topup_count AS number_of_topups,
    d.topup_amount AS average_topup_amount,
    d.topup_typical AS typical_topup_amount,
    d.topup_shareof_typical AS share_of_typical_topup_amount,
    d.topup_count_firsthalf_lastmonth AS number_of_topups_in_the_first_half_of_last_month,
    d.topup_count_lasthalf_lastmonth AS number_of_topups_in_the_last_half_of_last_month,
    d.topup_days_from_last AS number_of_days_from_last_topup,
    d.topup_count_day AS weekly_number_of_topups_daytime,
    d.topup_count_eve AS weekly_number_of_topups_evening,
    d.topup_count_night AS weekly_number_of_topups_nighttime,
    d.topup_count_weekday AS weekly_number_of_topups_weekday,
    d.topup_count_weekend AS weekly_number_of_topups_weekend,
	d.data_usage_day AS weekly_data_usage_daytime,
    d.data_usage_weekday AS weekly_data_usage_weekday,
    d.data_usage_eve AS weekly_data_usage_evening,
    d.data_usage_night AS weekly_data_usage_nighttime,
    d.data_usage_weekend AS weekly_data_usage_weekend,
    d.weekly_data_usage,
    d.weekly_data_usage_cost,
    --d.weekly_cost,
    --d.share_of_data_usage, 
    d.target,
    d.delivery_date
  FROM results.module_export_sni AS d
  WHERE mod_job_id = '||in_mod_job_id||'
  AND target_id = '||in_target_id||'
  '||query_ret_order;

  RETURN query_return;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.insert_campaign_target_list_sni(
  in_mod_job_id integer, 
  in_target_id integer, 
  target_limit text, 
  ctrl_grp_size double precision, 
  descvar text, 
  order_name text, 
  min_val double precision, 
  max_val double precision,
  blocklist_id text,
  arpu_seg text[], 
  tenure_seg text[]
) OWNER TO xsl;

----------------------------------------------

CREATE OR REPLACE FUNCTION work.social_network_statistics(netjob_id integer, in_mod_job_id integer)
RETURNS VOID AS 
$BODY$
/*
 * SUMMARY
 * 
 * Fills in the information about social network to network statistics chart.
 * 
 * VERSION
 * 27.02.2013 HMa - Only run in the apply phase. data_date = t2
 * 04.02.2013 KL
 */
	
DECLARE

  job_type text;
  t2 date;

BEGIN

  SELECT value INTO job_type
  FROM work.module_job_parameters
  WHERE key='run_type'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO t2
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;

  -- Churn statistics will only be calculated if the in_mod_job_id corresponds to apply period. 
  IF job_type='Predictors + Apply' THEN 


    INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
    SELECT in_mod_job_id, 'NETWORK_STATS', 'ALL', t2, 'Total number of subscribers in network', count(*), 11 
    FROM work.out_scores 
    WHERE job_id=netjob_id;
    
    
    INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
    SELECT in_mod_job_id, 'NETWORK_STATS', 'ALL', t2, 'Total number of on-net subscribers', count(*), 12 
    FROM work.out_scores  network 
    JOIN (
      SELECT alias_id 
      FROM (
        SELECT alias_id 
        FROM data.in_crm 
        UNION 
        SELECT charged_id 
        FROM data.topup 
        WHERE is_credit_transfer is false 
      ) a
      GROUP BY alias_id
    ) onnet 
    ON network.alias_id=onnet.alias_id 
    WHERE job_id=netjob_id;
    
    
    INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
    SELECT in_mod_job_id, 'NETWORK_STATS', 'ALL', t2, 'Total number of on-net active subscribers', count(*) AS value, 14 
    FROM work.out_scores a 
    JOIN work.modelling_data_matrix b 
    ON a.alias_id=b.alias_id 
    WHERE job_id=netjob_id 
    AND mod_job_id=in_mod_job_id;
    
    INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
    SELECT in_mod_job_id, 'NETWORK_STATS', 'ALL', t2, 'Percentage of on-net subscribers', round((act_subs/all_subs*100)::numeric,2), 13 
    FROM (
      SELECT var_value AS all_subs 
      FROM charts.chart_data 
      WHERE mod_job_id=in_mod_job_id
      AND var_name='Total number of subscribers in network'
    ) b 
    CROSS JOIN (
      SELECT var_value AS act_subs 
      FROM charts.chart_data 
      WHERE mod_job_id=in_mod_job_id
      AND var_name='Total number of on-net subscribers'
    ) c;
    
    
    INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
    SELECT in_mod_job_id, 'NETWORK_STATS', 'ALL', t2, 'Percentage of on-net active subscribers', round((act_subs/all_subs*100)::numeric,2), 15 
    FROM (
      SELECT var_value AS all_subs 
      FROM charts.chart_data 
      WHERE mod_job_id=in_mod_job_id
      AND var_name='Total number of subscribers in network'
    ) b 
    CROSS JOIN (
      SELECT var_value AS act_subs 
      FROM charts.chart_data 
      WHERE mod_job_id=in_mod_job_id
      AND var_name='Total number of on-net active subscribers'
    ) c;

  END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.social_network_statistics(integer, integer) OWNER TO xsl;


-----------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.verify_partition(in_schema_name text, in_table_name text, in_partition_name text)
  RETURNS integer AS
$BODY$ 
/*
 * A helper function to check whether table partition exists and has rows inserted. Returns
 * number of rows in partition if partition exists, otherwise NULL.
 *
 * Example: SELECT * FROM work.verify_partition('work', 'module_targets', '1');
 * 
 * VERSION  
 * 22.07.2014 JVi: Created 
 */ 
DECLARE

  partition_child_table_name text;
  return_value integer;
  query text;

BEGIN

  -- Verify that the target partition still exists...
  SELECT child.relname INTO partition_child_table_name
  FROM pg_inherits 
  JOIN pg_class       parent         ON pg_inherits.inhparent = parent.oid
  JOIN pg_class       child          ON pg_inherits.inhrelid  = child.oid
  JOIN pg_namespace   nmsp_parent    ON nmsp_parent.oid       = parent.relnamespace
  JOIN pg_namespace   nmsp_child     ON nmsp_child.oid        = child.relnamespace
  WHERE nmsp_parent.nspname = in_schema_name AND parent.relname = in_table_name
  AND   child.relname ~ ('_prt_' || in_partition_name || '$'); 
  
  -- ...and contains data
  IF partition_child_table_name IS NOT NULL THEN
    query := '
    SELECT COUNT(*) 
    FROM ' || in_schema_name || '.' || partition_child_table_name || ';';

    EXECUTE query INTO return_value;

  END IF;

  RETURN return_value;

END;
$BODY$
LANGUAGE plpgsql;
ALTER FUNCTION work.verify_partition(text, text, text) OWNER TO xsl;


----------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.churn_rate_score_list(in_mod_job_id integer, in_churn_type text)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Adds information of churn statistics to the charts when the churners are known (i.e. we have data till t7)
 * ToDo: Should the churn statistics be updated every week even if descriptive variables were calculated more seldom?
 *       Now, the statistic 'Time from last topup' is obtained from the descriptive variables, so the statistics are
 *       updated every time the true churners become available for an old descriptive variables run. 
 *
 * INPUT
 * in_mod_job_id: Identifier of a module job. If it is an apply job, churn statistics are calculated. 
 * in_churn_type: Identifier of the churn use case. Possible values 'churn_inactivity' or 'churn_postpaid'
 * 
 * VERSION
 * 22.07.2014 JVi - Fixed ICIF-216
 * 16.10.2013 HMa - Renamed work.churn_monthly_rate to work.churn_rate_score_list to reflect changes in ICIF-160. 
 * 21.08.2013 MOj - Fixed a bug with the churn rate for new and mature subscribers
 * 20.08.2013 HMa - ICIF-160: Removed the scaling from the churn rate (we won't report this as monthly churn rate anymore but as score list churn rate)
 * 19.06.2013 AV - Function renamed to churn_monthly_rate, can be used to calculate churn inactivity and churn postpaid monthly rates
 * 25.02.2013 HMa - Changed the definition of montly churn rate to count(target_churn_inactivity = 1 OR target_churn_inactivity IS NULL)/count(audience_churn_inactivity = 1) * (t5-t2)/30
 * 22.02.2013 HMa - Separated churn statistics and churn factors to different functions
 * 21.02.2013 HMa - Corrected the calculation of monthly churn rate
 * 12.02.2013 KL
 */

DECLARE
  churners_known_lag integer; -- days from t2 until we know the true churners
  current_t2 date;
  current_t4 date;
  current_t5 date;
  current_t6 date;
  current_t7 date;
  include_postpaid smallint;
  t2_ch date;
  t4_ch date;
  t5_ch date;
  t6_ch date;
  t7_ch date;
  tcrm_ch date;
  job_type text;
  target_list_mod_job_id integer;
  tmp integer;
  sql_query text;

BEGIN
  SELECT value INTO job_type
  FROM work.module_job_parameters
  WHERE key='run_type'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO current_t2
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;

  IF (in_churn_type = 'churn_postpaid') THEN 
    SELECT value INTO current_t5
    FROM work.module_job_parameters
    WHERE key='uc_churn_postpaid_t5'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO current_t4 
    FROM work.module_job_parameters
    WHERE key='uc_churn_postpaid_t4'
    AND mod_job_id=in_mod_job_id;  
  ELSE 
    SELECT value INTO current_t5
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t5'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO current_t4 
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t4'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO current_t6
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t6'
    AND mod_job_id=in_mod_job_id;
  
    SELECT value INTO current_t7
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t7'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO include_postpaid
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_include_postpaid'
    AND mod_job_id=in_mod_job_id;
  END IF;
  
  -- Churn statistics will only be calculated if the in_mod_job_id corresponds to apply period. 
  IF job_type='Predictors + Apply' THEN 

    ------ * Score list churn rate * ------
  
    -- churn rate for all, mature and new
    -- Calculated for each week once the true churners are known. 
    -- Defitinion: Churn rate according to churn inactivity / churn postpaid definition 
    --  (see functions work.create_target_list_churn_inactivity and work.create_target_list_churn_postpaid)

    -- How many days are needed after t2 so that true churners are known: 
    IF (in_churn_type = 'churn_postpaid') THEN
      churners_known_lag := current_t5 - current_t2;
    ELSE
      churners_known_lag := current_t7 - current_t2;
    END IF;

    -- See which is the latest t2 for which we know the true churners: 
    t2_ch := date_trunc('week', current_t2 - churners_known_lag)::date; 

    -- Check if there is a target list already calculated for the required time period in a fit job / churn rate calculation (this function): 
    SELECT max(m2.mod_job_id) INTO target_list_mod_job_id
    FROM work.module_job_parameters m1
    JOIN work.module_job_parameters m2
    ON m2.mod_job_id=m1.mod_job_id
    INNER JOIN work.module_job_parameters m3
    ON m3.mod_job_id=m1.mod_job_id
    WHERE (
         (m1.key = 'run_type' AND m1.value = 'Predictors + Fit + Apply') 
      OR (m1.key = 'job_type' AND m1.value = 'churn_inactivity_target' AND in_churn_type = 'churn_inactivity')
      OR (m1.key = 'job_type' AND m1.value = 'churn_postpaid_target' AND in_churn_type = 'churn_postpaid')
      OR (m1.key = 'job_type' AND m1.value = 'descriptive_variables_initialization')
      OR (m1.key = 'job_type' AND m1.value = 'desc_var_targets')
    )
    AND m2.key='t2'
    AND m2.value=t2_ch
    AND m3.key='job_use_cases'
    AND m3.value ~ in_churn_type;

    -- We need to verify that the target list partition still exists and contains subscribers
    SELECT * INTO tmp 
    FROM work.verify_partition('work', 'module_targets', target_list_mod_job_id::text);

    -- If target list partition no longer exists or contains no subscribers, forget the mod job id
    IF tmp IS NULL OR tmp = 0 THEN
      target_list_mod_job_id := NULL;
    END IF;
  
    -- if there is no fitting for that time, we evaluate the targets again 
    IF target_list_mod_job_id IS NULL THEN 

      --Create a new mod_job_id for calculating the monthly churn rate: 
      target_list_mod_job_id := core.nextval_with_partitions('work.module_sequence');
      tcrm_ch := max(m.date_inserted) 
                 FROM data.data_quality dq
                 JOIN data.crm_snapshot_date_inserted_mapping m
                 ON dq.data_date = m.snapshot_date
                 WHERE dq.data_source = 'crm' AND dq.status = 2
                 AND m.date_inserted <= t2_ch;
      IF (in_churn_type = 'churn_inactivity') THEN
        t4_ch = t2_ch + (current_t4 - current_t2); 
        t5_ch = t4_ch + (current_t5 - current_t4);
        t6_ch = t5_ch + (current_t6 - current_t5);
        t7_ch = t6_ch + (current_t7 - current_t6);
              
        INSERT INTO work.module_job_parameters VALUES
        (target_list_mod_job_id, 't2', t2_ch),
        (target_list_mod_job_id, 'uc_churn_inactivity_t4', t4_ch),
        (target_list_mod_job_id, 'uc_churn_inactivity_t5', t5_ch),
        (target_list_mod_job_id, 'uc_churn_inactivity_t6', t6_ch),
        (target_list_mod_job_id, 'uc_churn_inactivity_t7', t7_ch),
        (target_list_mod_job_id, 'uc_churn_inactivity_include_postpaid', include_postpaid),
        (target_list_mod_job_id, 'tCRM', tcrm_ch),
        (target_list_mod_job_id, 'job_type', 'churn_inactivity_target'),
        (target_list_mod_job_id, 'job_use_cases', 'churn_inactivity');
  
        tmp := work.create_target_list_churn_inactivity(target_list_mod_job_id);
        tmp := work.combine_target_lists(target_list_mod_job_id);
      END IF;             
      
      IF (in_churn_type = 'churn_postpaid') THEN
        t4_ch = t2_ch + (current_t4 - current_t2); 
        t5_ch = t4_ch + (current_t5 - current_t4);
      
        INSERT INTO work.module_job_parameters VALUES
        (target_list_mod_job_id, 't2', t2_ch),
        (target_list_mod_job_id, 'uc_churn_postpaid_t4', t4_ch),
        (target_list_mod_job_id, 'uc_churn_postpaid_t5', t5_ch),
        (target_list_mod_job_id, 'tCRM', tcrm_ch),
        (target_list_mod_job_id, 'job_type', 'churn_postpaid_target'),
        (target_list_mod_job_id, 'job_use_cases', 'churn_postpaid');
  
        tmp := work.create_target_list_churn_postpaid(target_list_mod_job_id);
        tmp := work.combine_target_lists(target_list_mod_job_id);
      END IF;
 
    ELSE 
      tcrm_ch := 
        value 
      FROM work.module_job_parameters
      WHERE mod_job_id = target_list_mod_job_id 
      AND lower(key) = 'tcrm';
    END IF;      

    -- Insert churn rate to chart data table: 
    sql_query := '
    INSERT INTO charts.chart_data(
      mod_job_id, 
      stat_name, 
      group_name, 
      data_date, 
      var_name, 
      var_value, 
      order_id
    )
    SELECT 
      '||target_list_mod_job_id||', 
      CASE WHEN ''churn_postpaid'' = '''|| in_churn_type ||''' THEN ''CHURN_RATE_SCORE_LIST_P'' ELSE ''CHURN_RATE_SCORE_LIST_I'' END AS stat_name,
      gr AS group_name, 
      '''|| t2_ch::text ||'''::date AS data_date, 
      to_char('''|| t2_ch::text ||'''::date, ''DD Mon'') AS var_name,  
      CASE 
        WHEN gr=''ALL'' THEN all_churn_rate
        WHEN gr=''MATURE'' THEN mature_churn_rate
        WHEN gr=''NEW'' THEN new_churn_rate
        ELSE NULL
      END as var_value, 
      NULL
    FROM (
      SELECT 
        round((count(churner)/NULLIF(count(*),0)::double precision*100.0)::numeric,2) AS all_churn_rate,
        round((count(churner*new_1)/NULLIF(count(new_1),0)::double precision*100.0)::numeric,2) AS new_churn_rate,
        round((count(churner*mature)/NULLIF(count(mature),0)::double precision*100.0)::numeric,2) AS mature_churn_rate,
        u.gr 
      FROM (
        SELECT uu.alias_id, 
          CASE WHEN uu.target_'||in_churn_type||' = 1 OR uu.target_'||in_churn_type||' IS NULL THEN 1 
            ELSE NULL END as churner,
          CASE WHEN b.switch_on_date>'''|| t2_ch::text ||'''::date-30 THEN 1 
            ELSE NULL END AS new_1, 
          CASE WHEN b.switch_on_date<'''|| t2_ch::text ||'''::date-90 THEN 1 
            ELSE NULL END AS mature 
        FROM work.module_targets uu
        INNER JOIN data.in_crm b 
        ON uu.alias_id=b.alias_id 
        WHERE uu.mod_job_id = '||target_list_mod_job_id||'
        AND b.date_inserted = '''|| tcrm_ch::text ||'''::date
        AND uu.audience_'||in_churn_type||' = 1
        AND b.switch_on_date IS NOT NULL
      ) bb
      JOIN (
        SELECT ''ALL'' as gr UNION
        SELECT ''NEW'' as gr UNION
        SELECT ''MATURE'' 
      ) as u 
      ON u.gr IS NOT NULL
      GROUP BY u.gr
    ) oi;';

    EXECUTE sql_query;  
  END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.churn_rate_score_list(integer, text) OWNER TO xsl;


-----------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.churn_statistics(in_mod_job_id integer, in_churn_type text)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Adds information of churn statistics to the charts when the churners are known (i.e. we have data till t7)
 * ToDo: Should the churn statistics be updated every week even if descriptive variables were calculated more seldom?
 *       Now, the statistic 'Time from last topup' is obtained from the descriptive variables, so the statistics are
 *       updated every time the true churners become available for an old descriptive variables run. 
 *
 * INPUT
 * in_mod_job_id: Identifier of a module job. If it is an apply job, churn statistics are calculated. 
 * in_churn_type: Identifier of the churn use case. Possible values 'churn_inactivity' or 'churn_postpaid'
 * 
 * VERSION
 * 22.07.2014 JVi - Fixed ICIF-216
 * 29.10.2013 JVi - Added calculating age class and time from last topup for non-churners as well
 * 19.06.2013 AV - Separated from churn_monthly rate and joined with churn_factors function. Support both churn inactivity and chunr postpadi statistics cacluations. 
 * 25.02.2013 HMa - Changed the definition of montly churn rate to count(target_churn_inactivity = 1 OR target_churn_inactivity IS NULL)/count(audience_churn_inactivity = 1) * (t5-t2)/30
 * 22.02.2013 HMa - Separated churn statistics and churn factors to different functions
 * 21.02.2013 HMa - Corrected the calculation of monthly churn rate
 * 12.02.2013 KL
 */

DECLARE
  churn_mod_job_id integer;
  churners_known_lag integer; -- days from t2 until we know the true churners
  churn_t2 date;
  churn_t7 date;
  churn_tcrm date;
  current_t2 date;
  current_t4 date;
  current_t5 date;
  current_t6 date;
  current_t7 date;
  include_postpaid smallint;
  t2_ch date;
  t4_ch date;
  t5_ch date;
  t6_ch date;
  t7_ch date;
  tcrm_ch date;
  job_type text;
  target_list_mod_job_id integer;
  tmp integer;
  sql_query text;
  i integer;
  this_long_name text;
  this_var_name text;

BEGIN
  SELECT value INTO job_type
  FROM work.module_job_parameters
  WHERE key='run_type'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO current_t2
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;

  IF (in_churn_type = 'churn_postpaid') THEN 
    SELECT value INTO current_t5
    FROM work.module_job_parameters
    WHERE key='uc_churn_postpaid_t5'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO current_t4 
    FROM work.module_job_parameters
    WHERE key='uc_churn_postpaid_t4'
    AND mod_job_id=in_mod_job_id;  
  ELSE 
    SELECT value INTO current_t5
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t5'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO current_t4 
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t4'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO current_t6
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t6'
    AND mod_job_id=in_mod_job_id;
  
    SELECT value INTO current_t7
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_t7'
    AND mod_job_id=in_mod_job_id;

    SELECT value INTO include_postpaid
    FROM work.module_job_parameters
    WHERE key='uc_churn_inactivity_include_postpaid'
    AND mod_job_id=in_mod_job_id;
  END IF;
  
  -- Churn statistics will only be calculated if the in_mod_job_id corresponds to apply period. 
  IF job_type='Predictors + Apply' THEN 

    ------ The rest of the churn statistics are calculated once we know the true churners for a descriptive variables run from the past -------

    -- Check if there is a descriptive variables run for which we know the churners. 
    SELECT max(m1.mod_job_id) INTO churn_mod_job_id  
    FROM work.module_job_parameters m1
    JOIN work.module_job_parameters m2
    ON m2.mod_job_id=m1.mod_job_id
    WHERE m1.key='job_type'
    AND m1.value ~ 'descriptive_variables'
    AND ((m2.key='uc_churn_inactivity_t7' AND m2.value<=current_t2 AND 'churn_inactivity' = in_churn_type) 
    OR (m2.key='uc_churn_postpaid_t5' AND m2.value<=current_t2 AND 'churn_postpaid' = in_churn_type));

    -- If such a mod_job_id is found, we will calculate the churn statistics, if not already calculated
    IF churn_mod_job_id IS NOT NULL THEN 

      -- Get the t2 of the descriptive variables run:
      SELECT value INTO churn_t2
      FROM work.module_job_parameters
      WHERE key='t2'
      AND mod_job_id=churn_mod_job_id;

      -- Get the tcrm of the descriptive variables run:
      SELECT value INTO churn_tcrm
      FROM work.module_job_parameters
      WHERE lower(key)='tcrm'
      AND mod_job_id=churn_mod_job_id;
      
      -- Check if the churn statistics have already been calculated for the required time period: 
      SELECT mod_job_id INTO tmp FROM charts.chart_data WHERE data_date = churn_t2 AND stat_name IN ('AGE_CLASS','LAST_TOPUP') AND ((group_name = 'CHURNER_I' AND in_churn_type = 'churn_inactivity') OR (group_name = 'CHURNER_P' AND in_churn_type = 'churn_postpaid')) LIMIT 1;
      
      -- If churn statistics have already been calculated, do not calculate them again
      IF tmp IS NULL THEN 

        -- Check if there is a target list already calculated for the required time period in a fit job: 
        SELECT max(m2.mod_job_id) INTO target_list_mod_job_id
        FROM work.module_job_parameters m1
        INNER JOIN work.module_job_parameters m2
        ON m2.mod_job_id=m1.mod_job_id
        INNER JOIN work.module_job_parameters m3
        ON m3.mod_job_id=m1.mod_job_id
        WHERE ( 
            (m1.key = 'run_type' AND m1.value = 'Predictors + Fit + Apply') 
            OR (m1.key = 'job_type' AND m1.value = 'churn_inactivity_target' AND in_churn_type = 'churn_inactivity')
            OR (m1.key = 'job_type' AND m1.value = 'churn_postpaid_target' AND in_churn_type = 'churn_postpaid')
            OR (m1.key = 'job_type' AND m1.value = 'descriptive_variables_initialization')
            OR (m1.key = 'job_type' AND m1.value = 'desc_var_targets')
        )
        AND m2.key='t2'
        AND m2.value=churn_t2
        AND m3.key='job_use_cases'
        AND m3.value ~ in_churn_type;

        -- We need to verify that the target list partition still exists and contains subscribers
        SELECT * INTO tmp 
        FROM work.verify_partition('work', 'module_targets', target_list_mod_job_id::text);

        -- If target list partition no longer exists or contains no subscribers, forget the mod job id
        IF tmp IS NULL OR tmp = 0 THEN
          target_list_mod_job_id := NULL;
        END IF;
        
        -- if there is no fitting for that time, we evaluate the targets again 
        IF target_list_mod_job_id IS NULL THEN 
  
          target_list_mod_job_id := core.nextval_with_partitions('work.module_sequence');

          IF (in_churn_type = 'churn_inactivity') THEN
            t4_ch = churn_t2 + (current_t4 - current_t2); 
            t5_ch = t4_ch + (current_t5 - current_t4);
            t6_ch = t5_ch + (current_t6 - current_t5);
            t7_ch = t6_ch + (current_t7 - current_t6);
              
            INSERT INTO work.module_job_parameters VALUES
            (target_list_mod_job_id, 't2', churn_t2::text),
            (target_list_mod_job_id, 'uc_churn_inactivity_t4', t4_ch::text),
            (target_list_mod_job_id, 'uc_churn_inactivity_t5', t5_ch::text),
            (target_list_mod_job_id, 'uc_churn_inactivity_t6', t6_ch::text),
            (target_list_mod_job_id, 'uc_churn_inactivity_t7', t7_ch::text),
            (target_list_mod_job_id, 'uc_churn_inactivity_include_postpaid', include_postpaid),
            (target_list_mod_job_id, 'tCRM', churn_tcrm::text),
            (target_list_mod_job_id, 'job_type', 'churn_inactivity_target'),
            (target_list_mod_job_id, 'job_use_cases', 'churn_inactivity');
      
            tmp := work.create_target_list_churn_inactivity(target_list_mod_job_id);
            tmp := work.combine_target_lists(target_list_mod_job_id);
          END IF;             
      
          IF (in_churn_type = 'churn_postpaid') THEN
            t4_ch = churn_t2 + (current_t4 - current_t2); 
            t5_ch = t4_ch + (current_t5 - current_t4);
          
            INSERT INTO work.module_job_parameters VALUES
            (target_list_mod_job_id, 't2', churn_t2),
            (target_list_mod_job_id, 'uc_churn_postpaid_t4', t4_ch),
            (target_list_mod_job_id, 'uc_churn_postpaid_t5', t5_ch),
            (target_list_mod_job_id, 'tCRM', churn_tcrm),
            (target_list_mod_job_id, 'job_type', 'churn_postpaid_target'),
            (target_list_mod_job_id, 'job_use_cases', 'churn_postpaid');
      
            tmp := work.create_target_list_churn_postpaid(target_list_mod_job_id);
            tmp := work.combine_target_lists(target_list_mod_job_id);
          END IF;
        END IF;
  
        ------ * Age class * ------
        sql_query := '
        INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
        SELECT '||churn_mod_job_id||', 
        ''AGE_CLASS'', 
        CASE WHEN hu.churn_flag = 0 THEN ''ALL''
             ELSE 
             CASE WHEN ''churn_postpaid'' = '''|| in_churn_type ||''' THEN ''CHURNER_P'' ELSE ''CHURNER_I'' END
        END,
        '''||churn_t2::text||'''::date, 
        hu.age_class,
        CASE WHEN hu.churn_flag = 1 THEN COALESCE(val,0)
             ELSE SUM(COALESCE(val,0)) OVER (PARTITION BY hu.age_class) END,
        dense_rank() over(order by hu.age_class)
        FROM ( 
          SELECT age_class, churn_flag, count(*) val
          FROM (
            SELECT 
              (CASE 
                WHEN ag<=interval ''20 years''  THEN ''0-20''
                WHEN ag<=interval ''25 years''  THEN ''20-25''
                WHEN ag<=interval ''30 years''  THEN ''25-30''
                WHEN ag<=interval ''35 years''  THEN ''30-35''
                WHEN ag<=interval ''40 years''  THEN ''35-40''
                WHEN ag<=interval ''45 years''  THEN ''40-45''
                WHEN ag<=interval ''50 years''  THEN ''45-50''
                WHEN ag<=interval ''55 years''  THEN ''50-55''
                WHEN ag<=interval ''60 years''  THEN ''55-60''
                WHEN ag<=interval ''65 years''  THEN ''60-65''
                WHEN ag>interval ''65 years''   THEN ''over 65''
                END) as age_class,
                churn_flag
            FROM (
              SELECT 
                age('''||churn_t2::text||'''::date, b.birth_date) ag,
                CASE WHEN (a.target_'||in_churn_type||' IS NULL OR a.target_'||in_churn_type||'=1) 
                     THEN 1 ELSE 0 END AS churn_flag -- 1 for churners, 0 for non-churners
              FROM work.module_targets a
              JOIN data.in_crm b
              ON a.alias_id=b.alias_id 
              WHERE a.audience_'||in_churn_type||'=1
              AND a.mod_job_id='||target_list_mod_job_id||'
              AND b.date_inserted = '''||churn_tcrm::text||'''::date
              AND b.birth_date IS NOT NULL
            ) c  
          )d
          GROUP BY age_class, churn_flag) u
          --join with age classes to ensure that all classes are included even though they are empty
          RIGHT OUTER JOIN (
            SELECT * FROM (
              SELECT  ''0-20'' as age_class
              UNION
              SELECT  ''20-25'' as age_class
              UNION
              SELECT  ''25-30'' as age_class
              UNION
              SELECT  ''30-35'' as age_class
              UNION
              SELECT  ''35-40'' as age_class
              UNION
              SELECT  ''40-45'' as age_class
              UNION
              SELECT  ''45-50'' as age_class
              UNION
              SELECT  ''50-55'' as age_class
              UNION
              SELECT  ''55-60'' as age_class
              UNION
              SELECT  ''60-65'' as age_class
              UNION
              SELECT  ''over 65'' as age_class
            ) a
            CROSS JOIN (
              VALUES (0),(1)
            ) b (churn_flag)
          ) hu
          ON  hu.age_class  = u.age_class 
          AND hu.churn_flag = u.churn_flag;';
        
        EXECUTE sql_query;      
  
        ------ * Time from last top-up * ------
        IF (in_churn_type = 'churn_inactivity') THEN    -- This chart ia avaialbe only for churn inactivity use case
          --last top up for churners, check the classes, now these work for data generator data but for real data they may not be good ones
          INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
          SELECT 
            churn_mod_job_id, 
            'LAST_TOPUP', 
            CASE WHEN hu.churn_flag = 1 THEN 'CHURNER' ELSE 'ALL' END, 
            churn_t2, 
            hu.time_from_topup, 
            CASE WHEN hu.churn_flag = 1 THEN COALESCE(val,0)
                 ELSE SUM(COALESCE(val,0)) OVER (PARTITION BY hu.time_from_topup) END,
            dense_rank() over(order by ord)
            FROM (
              SELECT   time_from_topup, churn_flag, count(*) val
              FROM (
                SELECT (
                  CASE 
                    WHEN time_from_topup<=7 THEN 'Less than 1 week'
                    WHEN time_from_topup<=14  THEN '1-2 weeks'
                    WHEN time_from_topup<=21  THEN '2-3 weeks'
                    WHEN time_from_topup<=28  THEN '3-4 weeks'
                    WHEN time_from_topup<=35  THEN '4-5 weeks'
                    WHEN time_from_topup<=42  THEN '5-6 weeks'
                    WHEN time_from_topup>42 THEN 'More than 6 weeks'
                    END) AS time_from_topup,
                  churn_flag
                FROM  (
                  SELECT 
                    a.alias_id, 
                    topup_days_from_last AS time_from_topup,
                    CASE WHEN a.target_churn_inactivity IS NULL OR a.target_churn_inactivity=1 THEN 1
                         ELSE 0 END AS churn_flag
                  FROM work.module_targets a
                  JOIN results.descriptive_variables_subscriber_matrix b
                  ON a.alias_id=b.alias_id 
                  WHERE a.audience_churn_inactivity=1 
                    AND a.mod_job_id=target_list_mod_job_id
                    AND b.mod_job_id=churn_mod_job_id
                ) c  
              )d
              GROUP BY time_from_topup, churn_flag
            ) u
            --join with the classes to ensure all classes to be included (even though frequency would be 0, additionally, add right order of classes
            RIGHT OUTER JOIN (
              SELECT * FROM (
                SELECT  'Less than 1 week' as time_from_topup, 1 as ord
                UNION
                SELECT  '1-2 weeks' as time_from_topup, 2 as ord
                UNION
                SELECT  '2-3 weeks' as time_from_topup, 3  as ord 
                UNION
                SELECT  '3-4 weeks' as time_from_topup,  4 as ord 
                UNION
                SELECT  '4-5 weeks' as time_from_topup,  5 as ord 
                UNION
                SELECT  '5-6 weeks' as time_from_topup,  6 as ord 
                UNION
                SELECT  'More than 6 weeks' as time_from_topup,  7 as ord
              ) a
              CROSS JOIN (
                VALUES (0),(1)
              ) b (churn_flag)
            ) hu 
            ON hu.time_from_topup=u.time_from_topup
            AND hu.churn_flag = u.churn_flag;
        END IF;

        ------ * Descriptive variables for churners * ------
        sql_query := '';

        FOR i IN
          SELECT var_id FROM work.differentiating_factors
        LOOP

          this_long_name := long_name FROM work.differentiating_factors WHERE var_id = i;
          this_var_name  := var_name  FROM work.differentiating_factors WHERE var_id = i;


          IF this_long_name = 'Typical top-up amount' THEN
            sql_query := sql_query || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||') '; -- no coalesce for typical top-up amount (because no top-up is not amount 0)
          ELSE
            sql_query := sql_query || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||')) ';
          END IF;

        END LOOP;

        sql_query := '
        INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
        SELECT
          '||churn_mod_job_id||' AS mod_job_id,
          df.stat_name,
          CASE WHEN ''churn_postpaid'' = '''|| in_churn_type ||''' THEN ''CHURNER_P'' ELSE ''CHURNER_I'' END AS group_name,
          '''||churn_t2::text||'''::date AS data_date,
          bb.var_name,
          bb.var_value,  
          df.order_id
        FROM (
          SELECT
            CASE 
              '||sql_query||'
            ELSE NULL END AS var_value, 
            long_name AS var_name
          FROM (
            SELECT 
              af.*,
              keys.long_name,
              keys.var_name
            FROM results.descriptive_variables_subscriber_matrix AS af
            INNER JOIN work.module_targets c 
            ON  af.alias_id=c.alias_id
            CROSS JOIN
            (SELECT long_name, var_name FROM work.differentiating_factors) AS keys
            WHERE af.mod_job_id = '||churn_mod_job_id||'
            AND c.mod_job_id = '||target_list_mod_job_id||'
            AND (
              target_'||in_churn_type||' = 1 
              OR (target_'||in_churn_type||' IS NULL AND audience_'||in_churn_type||' = 1)
            )
          ) aa
          GROUP BY long_name
        ) bb
        INNER JOIN work.differentiating_factors df
        ON bb.var_name = df.long_name;';

        EXECUTE sql_query;
      END IF;
    END IF;
  END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.churn_statistics(integer, text) OWNER TO xsl;



----------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.define_model_preprocessing(in_mod_job_id integer, use_case text)
RETURNS text AS
$BODY$

/* SUMMARY
 * This function reads a template model corresponding to the given template model id from the database 
 * (work.module_template_models), calls function work.tmpmodel_2_premodel to determine the preprocessing 
 * parameters and to save the preprocessing model to the database (work.module_models). If the given 
 * template model id is a submodel list, preprocessing parameters are determined for all the submodels. 
 * This function returns the id of the new preprocessing model. 
 *
 * INPUTS
 * in_mod_job_id        : integer : Indentifier of module job.
 * use_case             : text    : Name of the use case.
 *
 * OUTPUT
 * model_id             : integer : The id of the new premodel
 * 
 * VERSION
 * 
 * 19.04.2013 KL Changed the function to handle the fitter parameters and child model as model preprocessing is done in java
 * 13.02.2013 HMa 
 */

DECLARE

  model_id_key     text; 
  out_model_id     integer;
  tmp              integer;
  
BEGIN

  -- Create a child model as a copy of the newly created parent premodel: 
  out_model_id = work.model_parameters_for_fitter(
    in_mod_job_id, 
    ARRAY['use_case'        , 'is_parent'], 
    ARRAY[use_case          , 'true'     ], 
    ARRAY[NULL],
    ARRAY[NULL]);

  -- Write the new child model id to the module job parameters:
  model_id_key := 'uc_' || use_case || '_model_id';

  tmp := count(*) FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id AND "key" = model_id_key;
  IF tmp IS NULL OR tmp < 1 THEN 
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
      VALUES (in_mod_job_id, model_id_key, out_model_id::text);
  ELSE
    UPDATE work.module_job_parameters SET "value" = out_model_id::text WHERE mod_job_id = in_mod_job_id AND "key" = model_id_key;
  END IF;

 RETURN out_model_id::text;
 

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.define_model_preprocessing(in_mod_job_id integer, use_case text) OWNER TO xsl;


--------------Functions related to targeting charts:-----------------------


CREATE OR REPLACE FUNCTION work.insert_targeting_chart_data_score_distribution(in_target_id integer, use_case text)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates the churn propensity distribution and inserts it
 * into the table charts.chart_targeting_data. 
 *
 * INPUT
 * in_target_id:     Identifier of the targetting job in the SL UI
 * use_case:         Name of the use case, e.g., 'churn_inactivity', 'product'...
 *
 * VERSION
 * 08.02.2013 HMa 
 */
DECLARE

  query text;

BEGIN

  query := '
  INSERT INTO charts.chart_targeting_data (target_id, group_id, group_name, var_name, var_value, order_id) 
  SELECT
    '||in_target_id||' AS target_id,
    1 AS group_id, 
    '''||initcap(replace(use_case,'_',' '))||' score'' AS group_name,
    score AS var_name,
    var_value,
    row_number() over(order by score) AS order_id
  FROM (
    SELECT 
      round('||use_case||'_propensity_score::numeric, 2) AS score,
      count(*) AS var_value
    FROM results.module_export_'||use_case||'
    WHERE target_id = '||in_target_id||'
    AND target = ''tg''
    GROUP BY score
  ) s;';

  EXECUTE query;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.insert_targeting_chart_data_score_distribution(in_target_id integer, churn_type text) OWNER TO xsl;




CREATE OR REPLACE FUNCTION work.insert_targeting_chart_data_product(in_mod_job_id integer, in_target_id integer, products text)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates the product propensity distributions as well as the 
 * distribution of most likely products and inserts them
 * into the table charts.chart_targeting_data. 
 *
 * INPUT
 * in_mod_job_id:    Identifier of module job
 * in_target_id:     Identifier of the targetting job in the SL UI
 * products:         A comma-separated list of product names included in the targeting run
 *
 * VERSION
 * 08.02.2013 HMa 
 */
DECLARE

  product_array   text[];
  productname     text;
  productid       text;
  all_products    boolean;
  
  i integer;

  query text;

BEGIN

  product_array := string_to_array(products,',');

  IF array_upper(product_array,1) IS NULL THEN

    RAISE NOTICE 'No products given for targeting chart creation. Including all the products.';
    product_array := string_to_array(value,',') FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id AND key = 'uc_product_products';
    IF product_array IS NULL THEN
      RAISE EXCEPTION 'No products are included in the run related to this score list!';
    END IF;
    all_products := TRUE;
  ELSE
    all_products := FALSE;
  END IF;

    -- Loop over products:
  FOR i IN 1..array_upper(product_array,1) LOOP
      
    productname := trim(product_array[i]);

    IF all_products THEN 
      productid := productname;
      productname := product_name FROM data.product_information WHERE product_id = productid;
    ELSE
      -- Read product ID from the database: 
      productname := replace(productname,'_',' ');
      productid := product_id FROM data.product_information WHERE product_name = productname;
    END IF;

    IF productid IS NULL THEN 
      RAISE EXCEPTION 'Given product name does not match with products in the product information table!';
    END IF;

    query := '
    INSERT INTO charts.chart_targeting_data (target_id, group_id, group_name, var_name, var_value, order_id) 
    SELECT
      '||in_target_id||' AS target_id,
      '||i||' AS group_id, 
      '''||productname||''' AS group_name, 
      score AS var_name,
      var_value,
      score * 100 AS order_id
    FROM (
      SELECT 
        round('||productid||'_propensity::numeric, 2) AS score,
        count(*) AS var_value
      FROM results.module_export_product
      WHERE target_id = '||in_target_id||'
      AND target = ''tg''
      GROUP BY score
    ) s;';

    EXECUTE query;

  END LOOP;

  IF array_upper(product_array,1) > 1 THEN -- Several products chosen -> make chart for the most likely product distribution

    INSERT INTO charts.chart_targeting_data (target_id, group_id, group_name, var_name, var_value, order_id) 
    SELECT
      in_target_id AS target_id,
      0 AS group_id, 
      'highest_propensity_distribution' AS group_name, 
      replace(highest_propensity_product,'_',' ') AS var_name,
      count(*) AS var_value,
      row_number() over(order by highest_propensity_product) AS order_id
    FROM results.module_export_product
    WHERE target_id = in_target_id
    AND target = 'tg'
    GROUP BY highest_propensity_product;

  END IF;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.insert_targeting_chart_data_product(in_mod_job_id integer, in_target_id integer, products text) OWNER TO xsl;


CREATE OR REPLACE FUNCTION work.insert_targeting_chart_data_zero_day_prediction(in_target_id integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates the propensity distributions for zero day prediction as well as the distribution of 
 * most likely value segments and inserts them into the table charts.chart_targeting_data. 
 *
 * INPUT
 * in_target_id:     Identifier of the targeting job in the SL UI
 * objective:        String of form 'Zero day prediction: Low value', used to identify target segment
 *
 * VERSION
 * 08.03.2013 HMa - Calculate score distributions for all value segments and create data for predicted values chart
 * 06.03.2013 JVi - Created
 */


BEGIN

  
  -- Score distribution chart:
  INSERT INTO charts.chart_targeting_data (target_id, group_id, group_name, var_name, var_value, order_id) 
  SELECT
    in_target_id AS target_id,
    group_id, 
    CASE 
      WHEN group_id = 1 THEN 'Low' 
      WHEN group_id = 2 THEN 'Medium'  
      WHEN group_id = 3 THEN 'High' 
    END AS group_name, 
    score AS var_name,
    var_value,
    score * 100 AS order_id
  FROM (
    SELECT 
      round(low_value_segment_propensity::numeric, 2) AS score,
      1 AS group_id, 
      count(*) AS var_value
    FROM results.module_export_zero_day_prediction
    WHERE target_id = in_target_id
    AND target = 'tg'
    GROUP BY score
    UNION 
    SELECT 
      round(medium_value_segment_propensity::numeric, 2) AS score,
      2 AS group_id, 
      count(*) AS var_value
    FROM results.module_export_zero_day_prediction
    WHERE target_id = in_target_id
    AND target = 'tg'
    GROUP BY score
    UNION 
    SELECT 
      round(high_value_segment_propensity::numeric, 2) AS score,
      3 AS group_id, 
      count(*) AS var_value
    FROM results.module_export_zero_day_prediction
    WHERE target_id = in_target_id
    AND target = 'tg'
    GROUP BY score
  ) s;

  -- Predicted values chart:
  INSERT INTO charts.chart_targeting_data (target_id, group_id, group_name, var_name, var_value, order_id) 
  SELECT
    in_target_id AS target_id,
    0 AS group_id, 
    'highest_propensity_distribution' AS group_name, 
    CASE 
      WHEN highest_propensity_segment = 'low_value' THEN 'Low'
      WHEN highest_propensity_segment = 'medium_value' THEN 'Medium'
      WHEN highest_propensity_segment = 'high_value' THEN 'High'
    END AS var_name,
    count(*) AS var_value,
    row_number() over(order by highest_propensity_segment) AS order_id
  FROM results.module_export_zero_day_prediction
  WHERE target_id = in_target_id
  GROUP BY highest_propensity_segment;


END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION work.insert_targeting_chart_data_zero_day_prediction(integer) OWNER TO xsl;





CREATE OR REPLACE FUNCTION work.product_penetration(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * This function calculates the weekly penetration rate of products included in the product recommendation use case. 
 * Assumes that the product take-up data are delivered weekly with timestamp = sunday of the week.
 * Note that, if historical product penetration rates are shown, one should run this function for past weeks as well. 
 * As default, the product penetration rate is only calculated for the last week of the known data 
 * (last week of source period in the apply phase). 
 * 
 * VERSION
 * 18.04.2013 HMa - ICIF-126
 * 25.02.2013 HMa 
 */
DECLARE

  t2 date;
  products text;
  product_arr text[];
  p_id integer; 
  this_product_id text;
  query text;

BEGIN

  SELECT value INTO t2
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO products
  FROM work.module_job_parameters
  WHERE key='uc_product_products'
  AND mod_job_id=in_mod_job_id;

  IF products IS NOT NULL THEN 

    product_arr := string_to_array(products,',');

    -- Loop over products
    FOR p_id IN 1..array_upper(product_arr,1) LOOP

      this_product_id := product_arr[p_id]; 

      -- Insert the proporion of product users to chart data table: 
      query := '
      INSERT INTO charts.chart_data (
        mod_job_id, 
        stat_name, 
        group_name, 
        data_date, 
        var_name, 
        var_value, 
        order_id    
      )
      SELECT
        '||in_mod_job_id||', 
        ''PRODUCT_PENETRATION'' AS stat_name, 
        pi.product_name AS group_name, 
        '''||t2||'''::date - 1 AS data_date, -- Monday of the week
        to_char('''||t2||'''::date - 1, ''DD Mon'') AS var_name,
        sum(has_product)::double precision / count(*)::double precision AS var_value,
        NULL AS order_id
      FROM (
        SELECT 
          mt.alias_id, 
          CASE 
            WHEN p.product_taken_date IS NOT NULL AND (p.product_churn_date IS NULL OR p.product_churn_date >= '''||t2||'''::date-7) THEN 1 -- Has the product at some point during the last week of the source period (latest data if in_mod_job_id corresponds to apply period)
            ELSE NULL
          END AS has_product
        FROM work.module_targets AS mt 
        LEFT JOIN (SELECT * FROM data.product WHERE date_inserted = '''||t2||'''::date - 1 and product_id = '''|| this_product_id ||''') AS p -- Sunday of the week
        ON mt.alias_id = p.alias_id
        WHERE mt.mod_job_id = '||in_mod_job_id||'
      ) aa
      INNER JOIN data.product_information pi
      ON pi.product_id = '''|| this_product_id ||'''
      GROUP BY pi.product_name;';

      EXECUTE query;


    END LOOP;

  END IF;


END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.product_penetration(integer) OWNER TO xsl;


-------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.zero_day_prediction_statistics(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Adds zero-day prediction statistics to the charts data table. 
 *
 * INPUT
 * in_mod_job_id: Identifier of a module job. If it is an apply job, zero-day prediction statistics are calculated. 
 * 
 * VERSION
 * 09.09.2013 JVi Changed to use median instead of mean for ARPU calculation
 * 07.03.2013 JVi
 * 07.03.2013 HMa 
 */

DECLARE
  
  current_t2 date;
  uc_zero_day_prediction_t5 date;
  job_type text;
  earliest_data_month date; -- Fourth-to-last month, e.g. if current_t2 is in Jun13 (and after 7th day), this is Feb13)
  
  temp_table_sql text;

BEGIN

  SELECT value INTO job_type
  FROM work.module_job_parameters
  WHERE key='run_type'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO uc_zero_day_prediction_t5
  FROM work.module_job_parameters
  WHERE key='uc_zero_day_prediction_t5'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO current_t2
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;
  
  IF EXTRACT('day' FROM current_t2) > 7 THEN -- Include current month only if at least one full calendar week of data is available
    earliest_data_month := date_trunc('month', current_t2::timestamp) + '-4 months';
  ELSE
    earliest_data_month := date_trunc('month', current_t2::timestamp) + '-5 months';
  END IF;
  
  -- Zero-day prediction statistics will only be calculated if the in_mod_job_id corresponds to apply period. 
  IF job_type='Predictors + Apply' THEN 

    -- Predicted zero-day value segment distribution: 
    INSERT INTO charts.chart_data (
      mod_job_id, 
      stat_name,
      group_name, 
      data_date, 
      var_name, 
      var_value,
      order_id
    )
    SELECT 
      in_mod_job_id AS mod_job_id,
      'ZERO_DAY_SEGMENT_DISTR' AS stat_name,
      CASE 
        WHEN zero_day_prediction_predicted_segment = 1 THEN 'Low value'
        WHEN zero_day_prediction_predicted_segment = 2 THEN 'Medium value'
        WHEN zero_day_prediction_predicted_segment = 3 THEN 'High value'
      END AS group_name,
      uc_zero_day_prediction_t5 AS data_date,
      to_char(current_t2, 'DD Mon') AS var_name, 
      round((count(*)::numeric / n_all::numeric) * 100.0, 1)::double precision AS var_value,
      zero_day_prediction_predicted_segment::integer AS order_id
    FROM (
      SELECT
        *,
        count(*) over() AS n_all
      FROM results.module_results
      WHERE mod_job_id = in_mod_job_id
    AND zero_day_prediction_predicted_segment IS NOT NULL
    ) aa
    GROUP BY zero_day_prediction_predicted_segment, n_all;

    -- The next chart stores average weekly ARPU for each month and value segment, aggregated over users per joining month.
    -- Only users with a value segment predicted are included (if several are available, latest prediction is used).
    -- Only topups after original value segment scoring are included, and users who haven't made any topups during the month 
    -- in question are excluded. Current month is updated only if at least one full calendar week is available. Order_id is stored as YYYYMM.
        
    -- First, a temporary table is created to make code more readable
    DROP TABLE IF EXISTS temp_subscriber_table;
    temp_table_sql := '
      CREATE TEMPORARY TABLE temp_subscriber_table AS
        SELECT -- Find subscribers who have been scored for zero day prediction and their last predicted segment
          alias_id, 
          switch_on_date,
          predicted_segment, 
          original_prediction_date,
          topup_month
        FROM (
          SELECT -- Prepare list of subscribers and scoring dates for removing all but last scoring date
            alias_id, 
            switch_on_date,
            predicted_segment,
            original_prediction_date,
            row_number() OVER (PARTITION BY alias_id ORDER BY original_prediction_date DESC, mod_job_id DESC) AS rn
          FROM (
            SELECT -- Find subscribers who have been scored for zero day prediction, their switch on dates and predicted segments from all mod jobs
              mr_crm.alias_id,
              mr_crm.predicted_segment,
              mr_crm.switch_on_date,
              mjp.original_prediction_date,
              mjp.mod_job_id
            FROM ( -- Find predicted segment per mod job for each scored subscriber, discard those who don''t have segment predicted in any mod job
              SELECT
                mr.mod_job_id,
                mr.alias_id,
                crm.switch_on_date,
                CASE WHEN mr.predicted_segment = 1 THEN ''Low value''
                     WHEN mr.predicted_segment = 2 THEN ''Medium value''
                     WHEN mr.predicted_segment = 3 THEN ''High value'' END
                  AS predicted_segment
              FROM (
                SELECT mod_job_id, alias_id, zero_day_prediction_predicted_segment AS predicted_segment
                FROM results.module_results 
                WHERE zero_day_prediction_predicted_segment IS NOT NULL
              ) mr
              INNER JOIN ( -- Add switch on dates to each subscriber''s data
                SELECT DISTINCT alias_id, MAX(switch_on_date)::date AS switch_on_date
                FROM data.in_crm
                WHERE switch_on_date BETWEEN ''' || earliest_data_month || '''::date AND ''' || uc_zero_day_prediction_t5 || '''::date - 1
                GROUP BY alias_id
              ) crm
              ON crm.alias_id = mr.alias_id
            ) mr_crm
            INNER JOIN ( -- Add prediction dates (t5) for subscribers
              SELECT mod_job_id, value::date AS original_prediction_date
              FROM work.module_job_parameters
              WHERE key = ''uc_zero_day_prediction_t5''
            ) mjp 
            ON mr_crm.mod_job_id = mjp.mod_job_id
          ) mr_crm_mjp0
        ) mr_crm_mjp1
        CROSS JOIN ( VALUES (to_char(''' || earliest_data_month || '''::date, ''YYYYMM'')::int     ),
	                    (to_char(''' || earliest_data_month || '''::date, ''YYYYMM'')::int + 1 ),
	                    (to_char(''' || earliest_data_month || '''::date, ''YYYYMM'')::int + 2 ),
	                    (to_char(''' || earliest_data_month || '''::date, ''YYYYMM'')::int + 3 ),
	                    (to_char(''' || earliest_data_month || '''::date, ''YYYYMM'')::int + 4 )
	) months (topup_month)
	WHERE mr_crm_mjp1.rn = 1;
    ';
    
    EXECUTE temp_table_sql;

    INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
    SELECT
      in_mod_job_id                                            AS mod_job_id,
      'ZERO_DAY_WARPU'                                         AS stat_name,
      predicted_segment || ', Joined ' || switch_on_month      AS group_name,
      current_t2                                                             AS data_date,
      to_char(to_date(topup_month::text, 'YYYYMM'), 'MonYY')                 AS var_name,
      MAX(CASE WHEN two_quantile = 1 THEN weekly_weighted_topups ELSE 0 END) AS var_value,  -- To get the median value
      topup_month                                                            AS order_id
    FROM (
      SELECT DISTINCT
        switch_on_month,
        predicted_segment,
        topup_month,
        ntile(2) OVER (PARTITION BY switch_on_month, predicted_segment, topup_month, valid_activity_days_in_month
                       ORDER     BY monthly_topups * (          valid_activity_days_in_month 
                                                       / NULLIF(valid_activity_days_for_subscriber, 0)::double precision)
                      ) AS two_quantile,
        monthly_topups * ( 
          valid_activity_days_in_month / NULLIF(valid_activity_days_for_subscriber, 0)::double precision) 
          / (valid_activity_days_in_month) * 7.0::double precision AS weekly_weighted_topups
      FROM (
        SELECT DISTINCT -- Find monthly RPU, convert switch_on_date to switch_on_month and count valid days for each month and sub to allow weighting of partial months
          alias_id,
          to_char(switch_on_date, 'MonYY') AS switch_on_month,
          predicted_segment,
          original_prediction_date,
          topup_month,
          monthly_topups,

          NULLIF(   LEAST(  to_date((topup_month + 1)::text, 'YYYYMM'), uc_zero_day_prediction_t5 )
                  - to_date(topup_month::text, 'YYYYMM'), 
            0) -- Close NULLIF
            AS valid_activity_days_in_month, -- Number of days in topup_month, unless it is current month in which case days before t5.
                                             -- There shouldn't arise a case where the value would actually be zero, but the NULLIF
                                             -- is there just in case to prevent division by zero in the next query.
          NULLIF(GREATEST( 0, 
                           LEAST(to_date((topup_month + 1)::text, 'YYYYMM'), uc_zero_day_prediction_t5) 
                             - GREATEST( to_date(topup_month::text, 'YYYYMM'),  original_prediction_date,  switch_on_date)) 
                 , 0)
            AS valid_activity_days_for_subscriber -- Number of days in topup month after customer has joined but not counting days after current t5.
                                                  -- Null if zero.
        FROM (
          SELECT -- Combine subscription data with topup data
            tst.alias_id, 
            tst.switch_on_date,
            tst.predicted_segment, 
            tst.original_prediction_date,
            tst.topup_month AS topup_month,
            SUM(COALESCE(topup.topup_cost, 0)) AS monthly_topups -- Total topups for each sub in each month
          FROM temp_subscriber_table tst
          LEFT JOIN ( -- Add topup data for each sub
            SELECT charged_id, topup_cost, "timestamp"
            FROM data.topup
            WHERE "timestamp" >= earliest_data_month 
            AND   "timestamp" < uc_zero_day_prediction_t5
          ) topup
          ON topup.charged_id = tst.alias_id
          AND to_char(topup."timestamp", 'YYYYMM')::int = tst.topup_month
          AND topup."timestamp" >= tst.original_prediction_date
          GROUP BY tst.alias_id, tst.switch_on_date, tst.predicted_segment, tst.original_prediction_date, topup_month
        ) tst_topup1
      ) tst_topup2
      WHERE valid_activity_days_for_subscriber >= 14 -- Must be at least two weeks old 
      --GROUP BY switch_on_month, predicted_segment, topup_month, valid_activity_days_in_month
    ) chart_data
    GROUP BY switch_on_month, predicted_segment, topup_month;
    
        
  END IF;

  DROP TABLE IF EXISTS temp_subscriber_table;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.zero_day_prediction_statistics(integer) OWNER TO xsl;


-- DROP FUNCTION work.zero_day_prediction_factors(in_mod_job_id integer)
CREATE OR REPLACE FUNCTION "work".zero_day_prediction_factors(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Adds value segment differentiating factors to the charts baesd on the predicted segment.
 * 
 * VERSION
 * 21.03.2013 HMa: ICIF-122
 * 07.03.2013 AV
 */

DECLARE
  descvar_mod_job_id integer;
  job_type text;
  query_all text;
  query_segment_low text;
  query_segment_medium text;
  query_segment_high text;
  this_long_name text;
  this_var_name text;
  query text;
  t2 date;
  i integer;


BEGIN
  --tables that are called inside dynamic sql should be explicitely locked (not exactly sure, but idea - MOj)
  LOCK TABLE work.module_targets;
  LOCK TABLE charts.chart_data;
  LOCK TABLE results.descriptive_variables_subscriber_matrix;
  LOCK TABLE results.module_results;
  LOCK TABLE work.module_targets;
  LOCK TABLE work.differentiating_factors;

  SELECT value INTO job_type
  FROM work.module_job_parameters
  WHERE key='run_type'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO t2
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;

  IF job_type='Predictors + Apply' THEN 
  
      -- Get the latest descriptive variables run <= t2 with zero-day prediction included
    SELECT bb.mod_job_id INTO descvar_mod_job_id  
    FROM work.module_job_parameters AS aa
    INNER JOIN work.module_job_parameters AS bb
    ON aa.mod_job_id = bb.mod_job_id
    INNER JOIN work.module_job_parameters AS cc
    ON aa.mod_job_id = cc.mod_job_id
    WHERE aa.key = 'job_type' AND aa.value = 'descriptive_variables'
    AND bb.key = 't2'
    AND bb.value::date <= t2 -- Descvar mod_job_id should not be larger than current t2
    AND cc.key = 'job_use_cases'
    AND cc.value ~ 'zero_day_prediction'
    ORDER BY bb.value DESC, bb.mod_job_id DESC -- Take the latest t2 with the largest mod_job_id 
    LIMIT 1;

    -- If such a mod_job_id is found, we will calculate the value segment factors
    IF descvar_mod_job_id IS NOT NULL THEN 
  
      query_all = '';
      query_segment_low = '';
      query_segment_medium = '';
      query_segment_high = '';
    
      FOR i IN
        SELECT var_id FROM work.differentiating_factors
      LOOP
 
        this_long_name := long_name FROM work.differentiating_factors WHERE var_id = i;
        this_var_name  := var_name  FROM work.differentiating_factors WHERE var_id = i;


        IF this_long_name = 'Typical top-up amount' THEN
          query_all           := query_all            || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||' ) '; -- no coalesce for typical top-up amount (because no top-up is not amount 0)
          query_segment_low   := query_segment_low    || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||' * segment_low) '; 
          query_segment_medium:= query_segment_medium || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||' * segment_medium) '; 
          query_segment_high  := query_segment_high   || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||' * segment_high) ';
        ELSE
          query_all           := query_all            || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0) ) ';
          query_segment_low   := query_segment_low    || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0) * segment_low) ';
          query_segment_medium:= query_segment_medium || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0) * segment_medium) ';
          query_segment_high  := query_segment_high   || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0) * segment_high) ';
        END IF;

      END LOOP;
    
      query := '
        INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
        SELECT
          '||in_mod_job_id||' AS mod_job_id,
          df.stat_name,
          u.gr AS group_name, 
          '''||t2||'''::date AS data_date,
          bb.var_name,
          CASE 
      			WHEN u.gr = ''ALL_ZERO_DAY'' THEN bb.average_all
            WHEN u.gr = ''Low value''    THEN bb.average_segment_low
            WHEN u.gr = ''Medium value'' THEN bb.average_segment_medium
            WHEN u.gr = ''High value''   THEN bb.average_segment_high
            ELSE NULL
            END AS var_value,
          df.order_id
        FROM (
          SELECT
            CASE 
              '||query_all||'
            ELSE NULL END AS average_all, 
            CASE 
              '||query_segment_low||'
            ELSE NULL END AS average_segment_low,
            CASE 
              '||query_segment_medium||'
            ELSE NULL END AS average_segment_medium,
            CASE 
              '||query_segment_high||'
            ELSE NULL END AS average_segment_high,
            long_name AS var_name
          FROM (
            SELECT 
              af.*,
              keys.long_name,
              keys.var_name,
              CASE WHEN c.zero_day_prediction_predicted_segment = 1 THEN 1 ELSE NULL END AS segment_low,
              CASE WHEN c.zero_day_prediction_predicted_segment = 2 THEN 1 ELSE NULL END AS segment_medium,
              CASE WHEN c.zero_day_prediction_predicted_segment = 3 THEN 1 ELSE NULL END AS segment_high
            FROM results.descriptive_variables_subscriber_matrix AS af
            INNER JOIN results.module_results c 
            ON  af.alias_id=c.alias_id
            INNER JOIN work.module_targets t 
            ON  af.alias_id=t.alias_id
            CROSS JOIN
            (SELECT long_name, var_name FROM work.differentiating_factors) AS keys
            WHERE af.mod_job_id = '||descvar_mod_job_id||'
            AND c.mod_job_id = '||in_mod_job_id||'
            AND t.mod_job_id = '||in_mod_job_id||'
            AND t.audience_zero_day_prediction = 1
          ) aa
          GROUP BY long_name
        ) bb
        INNER JOIN work.differentiating_factors df
        ON bb.var_name = df.long_name
        INNER JOIN (
    				SELECT ''ALL_ZERO_DAY'' AS gr UNION 
    				SELECT ''Low value'' AS gr UNION 
    				SELECT ''Medium value'' AS gr UNION
    				SELECT ''High value'' AS gr
  			) AS u
  			ON (u.gr = ''ALL_ZERO_DAY'' AND bb.average_all IS NOT NULL)
  			OR (u.gr = ''Low value'' AND bb.average_segment_low IS NOT NULL)
  			OR (u.gr = ''Medium value'' AND bb.average_segment_medium IS NOT NULL)
  			OR (u.gr = ''High value'' AND bb.average_segment_high IS NOT NULL);';
    
        EXECUTE query; 
      
    END IF;
  END IF;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION "work".zero_day_prediction_factors(integer) OWNER TO xsl;

-- Function: work.churn_model_quality_verification(integer)

-- DROP FUNCTION work.churn_model_quality_verification(integer);



CREATE OR REPLACE FUNCTION work.churn_model_quality_verification(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Adds information for quality charts for applied models, when churners are known.
 * 
 * VERSION
 * 23.07.2014 JVi: Fixed ICIF-216
 * 29.05.2013 KL
 * 
 */

DECLARE
  mod_job_id_apply_churn_inactivity integer;
  mod_job_id_apply_churn_postpaid integer;
  model_id_churn_postpaid integer;
  model_id_churn_inactivity integer;
  model_id integer;
  mod_job_id_apply integer;
  job_use_cases text;
  t2 date;
  use_cases text[];
  use_case text;
  mod_job_id_fit integer;
  query text;
  apply_t2 date;
  apply_t4 date;
  apply_t5 date;
  apply_t6 date;  
  apply_t7 date;
  apply_tcrm date;
  target_list_mod_job_id integer;
  target_list_mod_job_id_churn_inactivity integer;  
  target_list_mod_job_id_churn_postpaid integer;  
  target_list_mod_job_id_new integer;  
  new_target_list boolean;
  insert_use_cases text;
  apply_include_postpaid text;
  new_postpaid boolean;
  new_inactivity boolean;
  tmp integer;

BEGIN

  new_target_list = FALSE;
  new_postpaid = FALSE;
  new_inactivity = FALSE;

  SELECT value INTO t2
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO job_use_cases
  FROM work.module_job_parameters 
  WHERE mod_job_id = in_mod_job_id 
  AND key = 'job_use_cases';

  -- Read the use case names from job_use_cases:
  use_cases := string_to_array(trim(job_use_cases),',');

  FOR uc_ind IN 1..array_upper(use_cases, 1) LOOP
    
    -- Use case name:
    use_case := use_cases[uc_ind];

    -- charts are done only for churn
--    IF use_case in ('churn_inactivity', 'churn_postpaid') THEN 

   IF use_case in ('churn_inactivity', 'churn_postpaid') THEN 


      IF use_case = 'churn_inactivity' THEN
      
      query='SELECT max(a.mod_job_id)
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      JOIN results.module_export_'||use_case||' d
      ON d.mod_job_id=a.mod_job_id
      WHERE a.key = ''uc_'||use_case||'_t7''
      AND a.value <= '''||t2||'''::date
      AND b.key = ''run_type''
      AND b.value = ''Predictors + Apply'';';

      ELSE

      query='SELECT max(a.mod_job_id)
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      JOIN results.module_export_'||use_case||' d
      ON d.mod_job_id=a.mod_job_id
      WHERE a.key = ''uc_'||use_case||'_t5''
      AND a.value <= '''||t2||'''::date
      AND b.key = ''run_type''
      AND b.value = ''Predictors + Apply'';';

      END IF;

      EXECUTE query INTO mod_job_id_apply;


      --if such a job is found
      IF  mod_job_id_apply IS NOT NULL THEN
      
        SELECT value::date INTO apply_t2
        FROM work.module_job_parameters a
        WHERE a.key = 't2'
        AND a.mod_job_id = mod_job_id_apply;

        -- find corresponding model_id
        SELECT value::integer INTO model_id
        FROM work.module_job_parameters 
        WHERE mod_job_id = mod_job_id_apply 
        AND key = 'uc_'||use_case||'_model_id';

        -- find corresponding 
        SELECT value::date INTO apply_t4
        FROM work.module_job_parameters 
        WHERE mod_job_id = mod_job_id_apply 
        AND key = 'uc_'||use_case||'_t4';

        SELECT value::date INTO apply_t5
        FROM work.module_job_parameters 
        WHERE mod_job_id = mod_job_id_apply 
        AND key = 'uc_'||use_case||'_t5';

        SELECT value::date INTO apply_tcrm
        FROM work.module_job_parameters 
        WHERE mod_job_id = mod_job_id_apply 
        AND lower(key) = 'tcrm';

        IF use_case= 'churn_inactivity' THEN

        SELECT value::date INTO apply_t6
        FROM work.module_job_parameters 
        WHERE mod_job_id = mod_job_id_apply 
        AND key = 'uc_'||use_case||'_t6';

        SELECT value::date INTO apply_t7
        FROM work.module_job_parameters 
        WHERE mod_job_id = mod_job_id_apply 
        AND key = 'uc_'||use_case||'_t7';

        END IF;

        -- Check if there is a target list already calculated for the required time period in a fit job  /monthly churn_rate in the same use case
        query='SELECT max(m2.mod_job_id) 
        FROM work.module_job_parameters m1
        JOIN work.module_job_parameters m2
        ON m2.mod_job_id=m1.mod_job_id
        INNER JOIN work.module_job_parameters m3
        ON m3.mod_job_id=m1.mod_job_id
        WHERE (
           (m1.key = ''run_type'' AND m1.value = ''Predictors + Fit + Apply'') 
        OR (m1.key = ''job_type'' AND m1.value in ('''||use_case||'_target'',''apply_quality'', ''desc_var_targets'', ''descriptive_variables_initialization'' ))
        )
        AND m2.key~''t2''
        AND m2.value= '''||apply_t2||'''
        AND m3.key=''job_use_cases''
        AND m3.value ~ '''||use_case||''';';



        EXECUTE query INTO target_list_mod_job_id;

        -- We need to verify that the target list partition still exists and contains subscribers
        SELECT * INTO tmp 
        FROM work.verify_partition('work', 'module_targets', target_list_mod_job_id::text);

        -- If target list partition no longer exists or contains no subscribers, forget the mod job id
        IF tmp IS NULL OR tmp = 0 THEN
          target_list_mod_job_id := NULL;
        END IF;

        IF use_case = 'churn_inactivity' THEN 
          target_list_mod_job_id_churn_inactivity = target_list_mod_job_id;
          mod_job_id_apply_churn_inactivity = mod_job_id_apply;
          model_id_churn_inactivity = model_id;
        ELSE
          target_list_mod_job_id_churn_postpaid = target_list_mod_job_id;
          mod_job_id_apply_churn_postpaid = mod_job_id_apply;
          model_id_churn_postpaid = model_id;
        END IF;

        -- if there is no fitting for that time, we evaluate the targets again 
        IF target_list_mod_job_id IS NULL THEN 

          IF NOT new_target_list THEN
            --Create a new mod_job_id for target_list
            target_list_mod_job_id_new := core.nextval_with_partitions('work.module_sequence');
            new_target_list = TRUE;
   

            INSERT INTO work.module_job_parameters VALUES
              (target_list_mod_job_id_new, 'job_type', 'apply_quality');

            insert_use_cases = use_case;    
          ELSE
             insert_use_cases = insert_use_cases || ','|| use_case;
          
          END IF;

       
          IF use_case = 'churn_inactivity' THEN 
            target_list_mod_job_id_churn_inactivity = target_list_mod_job_id_new;
  
            SELECT value INTO apply_include_postpaid
            FROM work.module_job_parameters 
            WHERE mod_job_id = mod_job_id_apply 
            AND key = 'uc_churn_inactivity_include_postpaid';

            INSERT INTO work.module_job_parameters VALUES
              (target_list_mod_job_id_new, 'uc_churn_inactivity_include_postpaid', apply_include_postpaid),
              (target_list_mod_job_id_new, 'uc_'||use_case||'_t6', apply_t6),
              (target_list_mod_job_id_new, 'uc_'||use_case||'_t7', apply_t7);

            new_inactivity = TRUE;
              
           ELSE 
             target_list_mod_job_id_churn_postpaid = target_list_mod_job_id_new;
             new_postpaid = TRUE;
           END IF;
        
           INSERT INTO work.module_job_parameters VALUES
             (target_list_mod_job_id_new, 'uc_'||use_case||'_tcrm', apply_tcrm),
             (target_list_mod_job_id_new, 'uc_'||use_case||'_t2', apply_t2),
             (target_list_mod_job_id_new, 'uc_'||use_case||'_t4', apply_t4),
             (target_list_mod_job_id_new, 'uc_'||use_case||'_t5', apply_t5);
            
         
            
         END IF; --target_list_mod_job_id_is  is  null)

      END IF; --mod_job_id_apply_is not null
    END IF; --if in churn_inactivity/postpaid
    
  END LOOP; 	





  IF new_inactivity OR new_postpaid THEN 
    IF new_inactivity THEN           
      tmp := work.create_target_list_churn_inactivity(target_list_mod_job_id_new);
    END IF;
    IF new_postpaid THEN           
      tmp := work.create_target_list_churn_postpaid(target_list_mod_job_id_new);

    END IF;

    tmp := work.combine_target_lists(target_list_mod_job_id_new);

    IF new_target_list THEN 
      INSERT INTO work.module_job_parameters VALUES
        (target_list_mod_job_id_new, 'job_use_cases', insert_use_cases);
    END IF;

  END IF;
  

  FOR uc_ind IN 1..array_upper(use_cases, 1) LOOP
    
    -- Use case name:
   use_case := use_cases[uc_ind];


    -- charts are done only for churn
    IF use_case = 'churn_inactivity' THEN  

        IF target_list_mod_job_id_churn_inactivity IS NOT NULL THEN

          query='SELECT count(*) --check if there are results for this mod_job_id
            FROM results.module_results_verification 
            WHERE mod_job_id = '||mod_job_id_apply_churn_inactivity||'
            AND model_id =  '||model_id_churn_inactivity||';';

          EXECUTE query INTO tmp;

          IF tmp = 0 THEN           

            --check if there are already results for this mod_job_id

            PERFORM * FROM results.insert_result_verification_charts(
	      target_list_mod_job_id_churn_inactivity, 
	      mod_job_id_apply_churn_inactivity,
	      model_id_churn_inactivity, 
	      'churn_inactivity',
	      'target_churn_inactivity',
	      1,
	      ARRAY['churn_inactivity_propensity_score'],
	      ARRAY['roc','lift','cumulative_gains'],
	      '_apply'
	    );


          END IF;
        END IF;
     ELSEIF use_case = 'churn_postpaid' THEN 

            IF  target_list_mod_job_id_churn_postpaid IS NOT NULL THEN
    
              query='SELECT count(*)
                FROM results.module_results_verification 
                WHERE mod_job_id = '||mod_job_id_apply_churn_postpaid||'
                AND model_id =  '||model_id_churn_postpaid||';';


              EXECUTE query INTO tmp;

              IF tmp = 0 THEN    

              PERFORM * FROM results.insert_result_verification_charts(
	      	target_list_mod_job_id_churn_postpaid, 
	      	mod_job_id_apply_churn_postpaid,
	      	model_id_churn_postpaid, 
	      	'churn_postpaid',
	      	'target_churn_postpaid',
	      	1,
	      	ARRAY['churn_postpaid_propensity_score'],
	      	ARRAY['roc','lift','cumulative_gains'],
	      	'_apply'
              );
              
            END IF;
          END IF;
     END IF;
    
  END LOOP;

  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.churn_model_quality_verification(integer)
  OWNER TO xsl;


----------------------------------------------------------------
CREATE OR REPLACE FUNCTION work.check_fitted_model_availability(model_fitting boolean)
  RETURNS boolean AS
$BODY$

/* SUMMARY
 * This function checks, if there already is a fitted model, returns true if not.
 *
 * VERSION
 * 14.06.2013 KL  
 */

DECLARE

  mod_job_id_fit integer;
  fitting  boolean;
  
BEGIN

  IF NOT model_fitting THEN

      mod_job_id_fit := a.mod_job_id 
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id=b.mod_job_id
      JOIN work.module_models c
      ON b.value=c.model_id
      WHERE a.key = 'run_type'
      AND a.value ~ 'Fit'
      AND b.key ~ 'model_id'
      AND c.key ~ 'intercept'
      LIMIT 1; 

    IF mod_job_id_fit IS NULL THEN
      fitting = true;
    ELSE 
      fitting = false;
    END IF;

  ELSE
    fitting = model_fitting;
  END IF;
  
  RETURN fitting;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.check_fitted_model_availability(boolean)
  OWNER TO xsl;


-- Function: work.calculate_monthly_arpu(integer)

-- DROP FUNCTION work.calculate_monthly_arpu(integer);

CREATE OR REPLACE FUNCTION work.calculate_monthly_arpu(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/* SUMMARY:
 * This function calculates normalized (30 days) monthly arpu for specified source period for prepaid and postpaid subscribers, as well as the arpu segmentations
 * Function also takes into account the length of the subscription and for new customer.
 * Inputs:
 *
 * in_mod_job_id             mod_job_id of the job
 *
 * 2014-06-19 QY
 * 2013-06-28 KL 
 */

DECLARE
  --Variables and parameters
  t1                          date; --source period start
  t2                          date; --source period end
  tcrm                        date;
  

BEGIN

    SELECT value::date INTO t2
      FROM work.module_job_parameters a
      WHERE a.key = 't2'
      AND a.mod_job_id = in_mod_job_id;

    SELECT value::date INTO t1
      FROM work.module_job_parameters a
      WHERE a.key = 't1'
      AND a.mod_job_id = in_mod_job_id;

    SELECT value::date INTO tcrm
      FROM work.module_job_parameters a
      WHERE lower(a.key) = 'tcrm'
      AND a.mod_job_id = in_mod_job_id;

  INSERT INTO work.monthly_arpu
    (mod_job_id, alias_id, monthly_arpu,segmentation) 
    SELECT 
     in_mod_job_id AS mod_job_id,
     alias_id,
     monthly_arpu, 
	 CASE WHEN s.intile=1   THEN 'Low value'
           WHEN s.intile=2  THEN 'Medium value'
           WHEN s.intile=3   THEN 'High value' END
           AS segmentation  
	FROM (	   
     SELECT
	 alias_id,
     max(arpu) AS monthly_arpu,
	 --rank() OVER (ORDER BY max(arpu) ASC)  
	 NTILE(3) OVER (ORDER BY max(arpu) ASC) AS intile
	 FROM (
        SELECT charged_id AS alias_id, 
          COALESCE(sum(credit_amount),0)*30/
            (CASE WHEN (t2-switch_on_date)<(t2-t1)
             THEN t2-switch_on_date
             ELSE t2-t1
             END)   AS arpu
        FROM data.topup a
        JOIN (
          SELECT alias_id, max (switch_on_date) as switch_on_date
          FROM data.in_crm
          WHERE switch_on_date< t2
          GROUP BY alias_id
        ) b
        ON a.charged_id=b.alias_id
        WHERE
          is_credit_transfer = false
        AND a.timestamp < t2
        AND a.timestamp >= t1
        GROUP BY a.charged_id, b.switch_on_date
        UNION ALL
        SELECT alias_id,
        CASE  WHEN subscriber_value >0 THEN subscriber_value
        ELSE 0 END  AS arpu  
        FROM data.in_crm
        WHERE payment_type = 'postpaid'
        AND date_inserted = tcrm
      ) a
      GROUP BY alias_id
	  ) s;

            
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.calculate_monthly_arpu(integer)
  OWNER TO xsl;

  
CREATE OR REPLACE FUNCTION work.calculateLimit(sql_limit character varying , sql_from_and_where character varying)
  RETURNS character varying AS
$BODY$
DECLARE
  percentage_limit integer;
BEGIN
  IF position('%' in sql_limit) = 0 THEN
    return sql_limit;
  ELSE
    EXECUTE 'SELECT count(*)*' || substring(sql_limit from 7 for (char_length(sql_limit)-7)) || '/100 ' || sql_from_and_where INTO percentage_limit;
    return 'LIMIT ' || percentage_limit;
  END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.calculateLimit(character varying , character varying)
  OWNER TO xsl;


---------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.variable_group_balance_factors(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Calculates balance factors for variable groups for adjusting logit propensity score calculation later. Consider:
 *
 *   aps = 1 / (1 + bf*exp(-als))
 *
 * where aps = adjusted propensity score, bf = balance factor stored in work.variable_group_balance_factors table
 * and als = adjusted linear score stored in work.module_model_output_variable_group
 *
 * Idea is that AVG(aps) =~ churn rate in population.
 * 
 * VERSION
 * 15.04.2014 JVi - Created
 */

DECLARE

  mjp record;
  
BEGIN

  SELECT DISTINCT
    in_mod_job_id AS mod_job_id,
    max(CASE WHEN d.key        IN ('t2') THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS t2
  INTO mjp
  FROM work.module_job_parameters AS d
  WHERE d.mod_job_id = in_mod_job_id;

  INSERT INTO work.variable_group_balance_factors
  (mod_job_id, model_id, output_id, vargroup, balance_factor)
  SELECT -- Calculate balance factor
    in_mod_job_id AS mod_job_id,
    model_id,
    output_id,
    mmovg.vargroup,
    ((1 - avg_propensity_score) / (NULLIF(avg_propensity_score,0))::numeric) * ((avg_vargroup_score) / (NULLIF(1 - avg_vargroup_score,0))::numeric) AS balance_factor
  FROM ( -- Calculate average logit score for whole population
    SELECT
      model_id,
      output_id,
      AVG(value) AS avg_propensity_score
    FROM work.module_model_output 
    WHERE mod_job_id = in_mod_job_id
    GROUP BY model_id, output_id
  ) mmo
  INNER JOIN ( -- Calculate average logit score for each vargroup
    SELECT 
      mmovg.model_id,
      mmovg.output_id,
      mmovg.vargroup,
      AVG(1 / NULLIF(1 + exp(-mmovg.value),0)::numeric) AS avg_vargroup_score
    FROM work.module_model_output_variable_group mmovg
    INNER JOIN ( -- Get only logistic models
      SELECT DISTINCT model_id FROM work.module_models 
      WHERE aid = 0 AND key = 'model_type' AND value = 'logistic' 
    ) mm
    USING (model_id)
    WHERE mmovg.mod_job_id = in_mod_job_id
    GROUP BY model_id, output_id, vargroup
  ) mmovg
  USING (model_id, output_id);

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.variable_group_balance_factors(integer) OWNER TO xsl;


---------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.copy_vargroup_output_to_results(module_job_id integer)
RETURNS integer AS
$BODY$

/* SUMMARY
 * This function copies variable group -specific output from work.module_model_output_variable_group table to
 * tmp.module_results_vargroup_tmp table for the included use cases. Afterwards, 
 * results.combine_variable_group_results function is called to combine the results in the 
 * results.module_results_variable_group table. 
 *
 * INPUTS
 * module_job_id : integer : Indentifier of module job.
 *
 * OUTPUT
 * count         : integer : The number of copied rows
 * 
 * VERSION
 * 03.05.2014 JVi - Created
 */

DECLARE

  job_use_cases text;
  
  use_cases     text[];
  use_case      text;
  uc_model_id   integer;
  
  records_found integer;
  n_rows        integer;

  product_run   boolean;
  
BEGIN

  -- Check if the results.module_results table already contains data for the given module_job_id:
  records_found := count(mrvg.*) FROM results.module_results_variable_group AS mrvg WHERE mrvg.mod_job_id = module_job_id;
  IF records_found IS NOT NULL AND records_found > 0 THEN
    
    RAISE EXCEPTION 'Table results.module_results_variable_group already contains data for mob_job_id %', module_job_id;
    
  END IF;

  job_use_cases := (
    SELECT mjp.value
    FROM work.module_job_parameters AS mjp 
    WHERE mjp.mod_job_id = module_job_id AND mjp.key = 'job_use_cases'
  );

  -- Read the use case names from job_use_cases:
  use_cases := string_to_array(trim(job_use_cases),',');
  
  FOR uc_ind IN 1..array_upper(use_cases, 1) LOOP
    
    -- Use case name:
    use_case := use_cases[uc_ind];

    -- Use case model id: 
    uc_model_id :=  mjp.value FROM work.module_job_parameters AS mjp WHERE mjp.mod_job_id = module_job_id AND mjp.key = 'uc_' || use_case || '_model_id';
    
    IF uc_model_id IS NULL THEN    
      RAISE EXCEPTION '%_model_id not found!', use_case;
    ELSIF use_case = 'churn_inactivity' THEN
      PERFORM results.copy_vargroup_output_to_results_churn(module_job_id, uc_model_id, 'inactivity');
    ELSIF use_case = 'churn_postpaid' THEN
      PERFORM results.copy_vargroup_output_to_results_churn(module_job_id, uc_model_id, 'postpaid');
      -- Do not raise notice
    ELSE
      RAISE NOTICE 'Copy results for use case ''%'' not supported', use_case;
    END IF;
  
  END LOOP;

  SELECT * FROM results.combine_variable_group_results(module_job_id) INTO n_rows;

  RETURN n_rows;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.copy_vargroup_output_to_results(integer) OWNER TO xsl;


---------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.copy_vargroup_output_to_results_churn(in_mod_job_id integer, in_model_id integer, churn_type text)
RETURNS void AS
$BODY$

/* SUMMARY
 * This function copies vargroup specific churn use case model output from work.module_model_output_variable_group table to
 * tmp.module_results_vargroup_tmp table.
 *
 * INPUTS
 * in_mod_job_id : integer : Indentifier of module job.
 * in_model_id   : integer : Identifier of model id.
 * churn_type    : text    : churn type, 'inactivity' or 'postpaid'
 * 
 * VERSION
 *
 * 15.04.2014 JVi: Created
 */

DECLARE
  
  records_found     integer;
  is_submodel_list  text;
  model_ids         integer[];
  uc_model_id       integer;
  m_ind             integer;
    
BEGIN

  -- Check if the model id is a submodel list: 
  SELECT "value" = 'submodel_list' FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_type' INTO is_submodel_list;

  IF is_submodel_list THEN -- Insert submodel ids to the model id array
    model_ids := array(SELECT "value"::integer FROM work.module_models WHERE model_id = in_model_id AND "key" = 'model_id');
  ELSE
    model_ids := ARRAY[in_model_id]; -- Insert model id to the model id array
  END IF;

  -- Loop over submodel ids (or only the main model id if there are no submodels): 
  FOR m_ind IN 1..array_upper(model_ids,1) LOOP

    uc_model_id := model_ids[m_ind];

    -- Check if the results.module_results table already contains data for the given in_mod_job_id:
    records_found := count(mr.*) FROM tmp.module_results_vargroup_tmp AS mr WHERE mr.mod_job_id = in_mod_job_id AND mr.model_id = uc_model_id; 
    IF records_found IS NOT NULL AND records_found > 0 THEN
      
      RAISE EXCEPTION 'Table tmp.module_results_vargroup_tmp already contains data for mob_job_id %, model_id %,', in_mod_job_id, uc_model_id;
      
    END IF;

    INSERT INTO tmp.module_results_vargroup_tmp
    (mod_job_id, model_id, alias_id, score_base_name, vargroup, score_value, vargroup_rank)
    SELECT
      in_mod_job_id AS mod_job_id,
      uc_model_id AS model_id,
      a.alias_id,
      'churn_' || churn_type  AS score_base_name,
      a.vargroup,
      a.adjusted_score AS score_value,
      rn AS vargroup_rank
    FROM (
      SELECT 
        mmovg.alias_id,
        mmovg.vargroup,
        1 / NULLIF(1 + vgbf.balance_factor * exp(-mmovg.value), 0)::numeric AS adjusted_score,
        row_number() OVER (PARTITION BY mmovg.alias_id ORDER BY mmovg.value - LN(vgbf.balance_factor) ASC) AS rn
      FROM work.module_model_output_variable_group mmovg
      INNER JOIN work.variable_group_balance_factors vgbf
      USING (mod_job_id, model_id, output_id, vargroup)
      WHERE mmovg.mod_job_id = in_mod_job_id
      AND mmovg.model_id = uc_model_id
      AND mmovg.output_id = 1 -- Currently no support for e.g. multinomial models
    ) a
--    WHERE rn = 1 -- If this condition is NOT commented out, only the adjusted score for that vargroup which most increases churn probability is stored for each sub
    ;

  END LOOP;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.copy_vargroup_output_to_results_churn(integer, integer, text) OWNER TO xsl;


---------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION results.combine_variable_group_results(in_mod_job_id integer)
RETURNS integer AS
$BODY$

/* SUMMARY
 * This function combines model output from tmp.module_results_vargroup_tmp table 
 * to results.module_results_variable_group table.
 *
 * INPUTS
 * in_mod_job_id : integer : Indentifier of module job.
 *
 * OUTPUT
 * count         : integer : The number of copied rows
 * 
 * VERSION
 * 15.04.2014 JVi
 */

DECLARE
  
  records_found   integer;
  score_base_name text;
  
  colname_string text;
  max_string     text;
  case_string    text;
  sql_string     text;

BEGIN

  -- Check if the results.module_results table already contains data for the given in_mod_job_id:
  records_found := count(mrvg.*) FROM results.module_results_variable_group AS mrvg WHERE mrvg.mod_job_id = in_mod_job_id;
  IF records_found IS NOT NULL AND records_found > 0 THEN
    
    RAISE EXCEPTION 'Table results.module_results_variable_group already contains data for mod_job_id %', in_mod_job_id;
    
  END IF;

  -- initialize the strings: 
  colname_string := '';
  max_string     := '';
  case_string    := '';
  
  FOR score_base_name IN (
    SELECT 
      mrvt.score_base_name
    FROM tmp.module_results_vargroup_tmp mrvt
    WHERE mod_job_id = in_mod_job_id
    GROUP BY mrvt.score_base_name
  )
  LOOP
    
    colname_string = colname_string || 
         score_base_name || '_driver, 
    ' || score_base_name || '_adjusted_score, ';
    
    max_string = max_string || '
    vargroup AS ' || score_base_name || '_driver, 
    max('||score_base_name||'_adjusted_score) AS '||score_base_name||'_adjusted_score, ';

    case_string = case_string || 'CASE WHEN mrvt.score_base_name = '''||score_base_name||''' THEN score_value ELSE NULL END AS '||score_base_name||'_adjusted_score, ';
  
  END LOOP;
  
  -- Trim strings:
  colname_string := trim(TRAILING ', ' FROM colname_string);
  max_string     := trim(TRAILING ', ' FROM max_string);
  case_string    := trim(TRAILING ', ' FROM case_string);

  IF colname_string = '' THEN
    
    RAISE NOTICE 'No scores found for mod_job_id %', in_mod_job_id;
    RETURN 0;
    
  END IF;

    
  -- Combine results from all the use cases and write them to results.module_results_variable_group:
  sql_string :=
 'INSERT INTO results.module_results_variable_group (
    mod_job_id, 
    alias_id,
    ' || colname_string || '
  ) 
  SELECT 
   '|| in_mod_job_id || ' AS mod_job_id, 
    alias_id, ' ||
    max_string || '
  FROM
  (
    SELECT
      mrvt.alias_id, 
      vargroup,
      ' || case_string || '
    FROM tmp.module_results_vargroup_tmp AS mrvt
    WHERE mrvt.mod_job_id = ' || in_mod_job_id ||'
    AND mrvt.vargroup_rank = 1
  ) a
  GROUP BY alias_id,vargroup;';

  EXECUTE sql_string;

  PERFORM core.analyze('results.module_results_variable_group', in_mod_job_id);

  RETURN (
    SELECT count(mrvg.*)
    FROM results.module_results_variable_group AS mrvg
    WHERE mrvg.mod_job_id = in_mod_job_id
  );

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION results.combine_variable_group_results(integer) OWNER TO xsl;


---------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION work.variable_group_statistics(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Calculates statistics for variable groups.
 * 
 * VERSION
 * 01.04.2014 JVi - Created
 */

DECLARE

  mjp record;
  
BEGIN

  SELECT DISTINCT
    in_mod_job_id AS mod_job_id,
    max(CASE WHEN d.key        IN ('t2') THEN to_date(d.value, 'YYYY-MM-DD') ELSE NULL END) AS t2
  INTO mjp
  FROM work.module_job_parameters AS d
  WHERE d.mod_job_id = in_mod_job_id;


 /*
  INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
  SELECT
    in_mod_job_id AS mod_job_id,
    'VARIABLE_GROUP_SCORES_AVG_SCORE' AS stat_name,
    'model_id ' || model_id::text AS group_name,
    mjp.t2::date AS data_date,
    vargroup AS var_name,
    ROUND(avg_score_adjusted::numeric, 6) AS var_value, 
    row_number() OVER (ORDER BY vargroup) + 1 AS order_id
  FROM (
    SELECT 
      mmovg.model_id,
      mmovg.output_id,
      mmovg.vargroup,
      AVG(1 / NULLIF(1 + balance_factor * exp(-mmovg.value),0)) AS avg_score_adjusted
    FROM work.module_model_output_variable_group mmovg
    INNER JOIN ( -- Get only logistic models
      SELECT DISTINCT model_id FROM work.module_models 
      WHERE aid = 0 AND key = 'model_type' AND value = 'logistic' 
    ) mm
    USING (model_id)
    INNER JOIN work.variable_group_balance_factors vgbf
    USING (mod_job_id, model_id, output_id, vargroup)
    WHERE mmovg.mod_job_id = in_mod_job_id
    GROUP BY model_id, output_id, vargroup
  ) a;
*/

  INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
  SELECT
    in_mod_job_id AS mod_job_id,
    'VARIABLE_GROUP_SCORES_AVG_SCORE' AS stat_name,
    'model_id ' || model_id::text AS group_name,
    mjp.t2::date AS data_date,
    'All' AS var_name,
    ROUND(avg_score::numeric, 6) AS var_value, 
    1 AS order_id
  FROM (
    SELECT 
      mmo.model_id,
      mmo.output_id,
      AVG(mmo.value) AS avg_score
    FROM work.module_model_output mmo
    INNER JOIN ( -- Get only logistic models
      SELECT DISTINCT model_id FROM work.module_models 
      WHERE aid = 0 AND key = 'model_type' AND value = 'logistic' 
    ) mm
    USING (model_id)
    WHERE mmo.mod_job_id = in_mod_job_id
    GROUP BY model_id, output_id
  ) a;

  -- Idea: vargroup (=driver for e.g. churn) is quantified by average prediction score in the group where that vargroup is most 
  -- significant driver to average score in whole population, in addition count of alias_ids is stored

  INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
  SELECT
    in_mod_job_id AS mod_job_id,
    CASE WHEN vars.varname = 'vargroup_gain' THEN 'VARIABLE_GROUP_SCORE_DRIVERS_GAIN' 
         WHEN vars.varname = 'alias_count'   THEN 'VARIABLE_GROUP_SCORE_DRIVERS_COUNT' END AS stat_name,
    'model_id ' || vals.model_id AS group_name,
    mjp.t2::date AS data_date,
    vals.vargroup AS var_name,
    CASE WHEN vars.varname = 'vargroup_gain' THEN round(vargroup_gain::numeric,3)
         WHEN vars.varname = 'alias_count' THEN count_aliases END AS var_value,
    row_number() OVER (ORDER BY vargroup) + 1 AS order_id
  FROM (VALUES ('alias_count'), ('vargroup_gain')
  ) vars (varname)
  CROSS JOIN (
    SELECT DISTINCT
      model_id,
      output_id,
      a.vargroup,
      COUNT(alias_id) OVER (PARTITION BY model_id, output_id, a.vargroup) AS count_aliases,
      AVG(mmo.value) OVER (PARTITION BY model_id, output_id, a.vargroup) / NULLIF(AVG(mmo.value) OVER (PARTITION BY model_id, output_id),0) AS vargroup_gain
    FROM (
      SELECT 
        mmovg.alias_id,
        mmovg.model_id,
        mmovg.output_id,
        mmovg.vargroup,
        ROW_NUMBER() OVER (PARTITION BY mmovg.alias_id, mmovg.model_id, mmovg.output_id ORDER BY mmovg.value - LN(vgbf.balance_factor) ASC) AS rn
      FROM work.module_model_output_variable_group mmovg
      INNER JOIN ( -- Get only logistic models
        SELECT DISTINCT model_id FROM work.module_models 
        WHERE aid = 0 AND key = 'model_type' AND value = 'logistic' 
      ) mm
      USING (model_id)
      INNER JOIN work.variable_group_balance_factors vgbf
      USING (mod_job_id, model_id, output_id, vargroup)
      WHERE mmovg.mod_job_id = in_mod_job_id
    ) a
    INNER JOIN work.module_model_output mmo
    USING (alias_id, model_id, output_id)
    WHERE a.rn = 1
    AND mmo.mod_job_id = in_mod_job_id
  ) vals;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.variable_group_statistics(integer) OWNER TO xsl;



CREATE OR REPLACE FUNCTION work.append_variable_group_output_to_target_query(in_mod_job_id integer, in_target_query text, in_use_case text)
  RETURNS text AS
$BODY$

/* SUMMARY
 * This function joins data in the variable group result table to an existing target list query before the 
 * target list is written to a csv file. Currently, only churn_inactivity and churn_postpaid are supported,
 * for other use cases the target query is unchanged
 *
 * INPUT
 * in_mod_job_id:   Identifier of module job
 * in_target_id:    Identifier of the target list
 * in_use_case:     Use case name
 *
 * OUTPUT
 * The query that will be used to write target list data to csv file
 *
 * VERSION
 * 30.04.2014 JVi Created
 */

DECLARE  

  field_names_part           text;
  out_target_query           text;

BEGIN
  
  
  IF in_use_case NOT IN ('churn_inactivity', 'churn_postpaid') THEN
    return in_target_query;
  END IF;
  
  field_names_part := substring(in_target_query, E'(?i)^.*SELECT\\s*(.*)??\\s*FROM\\s*results.module_export_');
  
  out_target_query := regexp_replace(in_target_query, 'ORDER BY order_id', '');
  out_target_query := regexp_replace(out_target_query, field_names_part, field_names_part || E',\norder_id');

  out_target_query := '
  SELECT
   ' || field_names_part || ',
    b.' || in_use_case || '_driver,
    b.' || in_use_case || '_adjusted_score
  FROM (
    ' || out_target_query || '
  ) a
  INNER JOIN aliases.string_id si
  ON a.msisdn = si.string_id
  LEFT JOIN results.module_results_variable_group b
  ON b.mod_job_id = ' || in_mod_job_id || '
  AND si.alias_id = b.alias_id
  ORDER BY a.order_id';
     
  RETURN out_target_query;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.append_variable_group_output_to_target_query(integer, text, text)
  OWNER TO xsl;

 
  CREATE OR REPLACE FUNCTION work.cellid_lda_input_data(in_mod_job_id integer)
  RETURNS SETOF integer AS
$BODY$ 
/* 
 * A function to calculate the input data for LDA that calculates cell id topics from the location profiles
 * 
 * Parameters: in_mod_job_id: module job id defining the input data period (t1...t2)
 * 
 * Reads cell_id data from data.cdr and aggregates form data.in_split_weekly. 
 * Generates a new LDA input ID
 * Writes to work.lda_input and work.lda_input_parameters
 * 
 * VERSION
 * 20.09.2014 QYu 
 * 30.11.2012 HMa
 * 
 */


DECLARE
  lda_inp_id INTEGER;  
  insert_query text;
  t1_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1'
  );  	
  t2_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  tcrm_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'tcrm'
  );
--  t_start text := greatest(to_date(t1_text,'YYYY-MM-DD'),(to_date(t2_text,'YYYY-MM-DD')-'3 month'::interval))::text; -- Calculate from the last month of the source period (or shorter if shorter source period)
 
  
BEGIN
  
  SELECT core.nextval_with_partitions('work.lda_input_sequence') INTO lda_inp_id;
  
  INSERT INTO work.lda_input_parameters 
  (lda_input_id, "key", "value")
  VALUES 
  (lda_inp_id, 'LDA type', 'cellid_topic_from_events'),
  (lda_inp_id, 'doc', 'events_type'),
  (lda_inp_id, 'term', 'cell_id'),
  (lda_inp_id, 't_start', t1_text),
  (lda_inp_id, 't_end', t2_text);

  INSERT INTO work.lda_input_parameters
  (lda_input_id, "key", "value")
  SELECT lda_inp_id AS lda_job_id, 'input_data_inserted' AS "key", to_char(CURRENT_TIMESTAMP(0), 'YYYY-MM-DD HH24:MI:SS') AS "value";
  
 	FOR i IN 1..(24*7) LOOP
	  insert_query := 
	 'INSERT INTO work.lda_input (lda_id, doc, term, n) 
	  SELECT ' 
	  ||lda_inp_id ||','
	  ||'''voice_c_'||i ||'''as doc, 
	  cell_id::text as term, 
	  sum(cc.n_sub) AS n	  
	  FROM (
		  SELECT 
		  alias_a AS alias_id, 
		  a_cell_id AS cell_id,
          count(*) AS n_sub		  
		  FROM data.cdr aa 
		  WHERE aa.a_cell_id IS NOT NULL 
		  AND aa.call_time >= to_timestamp('''||t1_text||''',''YYYY-MM-DD'') 
		  AND aa.call_time < to_timestamp('''||t2_text||''',''YYYY-MM-DD'')
          AND aa.call_time >= date_trunc(''week'', aa.call_time + '''|| i-1 || ' hours''::interval)::timestamp without time zone 
          AND aa.call_time < date_trunc(''week'', aa.call_time + '''|| i || ' hours''::interval)::timestamp without time zone 
		  GROUP BY a_cell_id, alias_a 
		  UNION ALL 
		  SELECT
		  alias_b AS alias_id, 
		  b_cell_id AS cell_id,
          count(*) AS n_sub	 
		  FROM data.cdr bb 
		  --INNER JOIN aliases.aliases_updated au
		  --ON bb.alias_b = au.alias_id
		  --WHERE au.net_id =0
		  WHERE bb.b_cell_id IS NOT NULL
		  AND bb.call_time >= to_timestamp('''||t1_text||''',''YYYY-MM-DD'') 
		  AND bb.call_time < to_timestamp('''||t2_text||''',''YYYY-MM-DD'')
          AND bb.call_time >= date_trunc(''week'', bb.call_time + '''|| i-1 || ' hours''::interval)::timestamp without time zone 
          AND bb.call_time < date_trunc(''week'', bb.call_time + '''|| i || ' hours''::interval)::timestamp without time zone 		  
		  GROUP BY b_cell_id, alias_b
	  ) cc
	  GROUP BY cell_id';
	  execute insert_query;
  
    
	  
  END LOOP;
  
  --TRUNCATE work.lda_input_tmp;
	
  RETURN NEXT lda_inp_id;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.cellid_lda_input_data(integer)
  OWNER TO xsl;
  
 
    CREATE OR REPLACE FUNCTION work.create_modelling_data_cell_events_model(in_mod_job_id integer, in_lda_output_id integer)
  RETURNS void AS
$BODY$
/*
 * A function to add cell topics obtained from LDA runner to predictors.
 * Reads from work.lda_output
 * Inserts into work.modelling_data_matrix_cell_events_topic.
 * 
 * Parameters: 
 * in_mod_job_id: module job id
 * in_lda_output_id: LDA output id 
 * 
 * VERSION 
 * 20.09.2014 QYu 
 * 05.06.2014 HMa - ICIF-181 Stack removal
 * 30.11.2012 HMa
 */


DECLARE
   t1_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1'
  );  
  t2_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  i integer;
  n_topics integer;
  var_names text := '';
  aggr_block text := '';
  query text;

BEGIN

  IF in_lda_output_id = -1 THEN
    RAISE NOTICE 'in_lda_output_id % is not valid, quitting.',in_lda_output_id;
    RETURN;
  END IF;

  SELECT value::integer FROM work.lda_output_parameters WHERE lda_output_id = in_lda_output_id AND key = 'n_topics' INTO n_topics;

  
  FOR i IN 1..(n_topics-1) LOOP -- leave one topic out because of collinearity
    var_names := var_names || 'cell_events_topic_' ||i||'_1, cell_events_topic_' ||i||'_2, ';
    aggr_block := aggr_block || 'MAX(CASE WHEN topic = ''cell_events_topic_' || i || ''' AND vals.cell_rank = 1 THEN value END) AS cell_events_topic_' 
	|| i || '_1 , MAX(CASE WHEN topic = ''cell_events_topic_' || i || ''' AND vals.cell_rank = 2 THEN value END) AS cell_events_topic_' 
	|| i || '_2,'; 
  END LOOP;
  
  var_names  := trim(TRAILING ', ' FROM var_names);
  aggr_block := trim(TRAILING ', ' FROM aggr_block);

  -- Values of new variables added to work.modelling_data (stack)
  query := 
  'INSERT INTO work.modelling_data_matrix_cell_events_topic
  ( mod_job_id, alias_id, '||var_names||' )
      SELECT
        '||in_mod_job_id||' AS mod_job_id,
        vals.alias_id,    
        '||aggr_block||'
      FROM (
        SELECT 
          mt.alias_id, 
          hmp.topic,
          hmp.value,
		  b.cell_rank
        FROM work.module_targets AS mt 
        LEFT JOIN 
          (
		  SELECT
		    alias_id,
			cell_id,
			rank() OVER (PARTITION BY alias_id ORDER BY sum(n_sub) DESC) AS cell_rank
		  FROM	  
		     (SELECT 
			  alias_a AS alias_id, 
			  a_cell_id AS cell_id,
			  count(*) AS n_sub		  
			  FROM data.cdr aa 
			  WHERE aa.a_cell_id IS NOT NULL 
			  AND aa.call_time >= to_timestamp('''||t1_text||''',''YYYY-MM-DD'') 
			  AND aa.call_time < to_timestamp('''||t2_text||''',''YYYY-MM-DD'')
			  GROUP BY a_cell_id, alias_a 
			  UNION ALL 
			  SELECT
			  alias_b AS alias_id, 
			  b_cell_id AS cell_id,
			  count(*) AS n_sub	 
			  FROM data.cdr bb 
			  INNER JOIN aliases.aliases_updated au
			  ON bb.alias_b = au.alias_id
			  WHERE au.alias_id =0
			  AND bb.b_cell_id IS NOT NULL
			  AND bb.call_time >= to_timestamp('''||t1_text||''',''YYYY-MM-DD'') 
			  AND bb.call_time < to_timestamp('''||t2_text||''',''YYYY-MM-DD'')
			  GROUP BY b_cell_id, alias_b ) kk
		    GROUP BY alias_id, cell_id  
		  )AS b
        ON mt.alias_id = b.alias_id
		LEFT JOIN 
          work.lda_output AS hmp
        ON b.cell_id = hmp.term -- because of possible formatting differences, remove spaces and convert to lowercase
        WHERE mt.mod_job_id = '||in_mod_job_id ||'
		AND b.cell_rank IN (1,2)
        AND hmp.lda_id = '||in_lda_output_id||'
      ) AS vals
      GROUP BY alias_id;';

  EXECUTE query;

  PERFORM core.analyze('work.modelling_data_matrix_cell_events_topic', in_mod_job_id);
END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.create_modelling_data_cell_events_model(integer, integer)
  OWNER TO xsl;
  
  
 

CREATE OR REPLACE FUNCTION work.create_modelling_data_geolocation(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * A function to add cell topics obtained from LDA runner to predictors.
 * Reads from work.lda_output
 * Inserts into work.modelling_data_matrix_cell_events_topic.
 * 
 * Parameters: 
 * in_mod_job_id: module job id
 * in_lda_output_id: LDA output id 
 * 
 * VERSION
 
 * 20.09.2014 QYu 
 * 05.06.2014 HMa - ICIF-181 Stack removal
 * 30.11.2012 HMa
 */


DECLARE
   t1_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1'
  );  
  t2_text text := (
    SELECT DISTINCT mjp.value
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2'
  );
  i integer;
  
  var_names text := '';
  aggr_block text := '';
  query text;
  
  monday_time timestamp without time zone;
  max_time timestamp without time zone;
  min_time timestamp without time zone;
  long_interval interval := '28 days'::interval;
  short_interval interval := '7 days'::interval;
  
  monday_list timestamp without time zone []:= ARRAY(SELECT date_trunc('week',call_time) FROM tmp.cdr_full_weeks GROUP BY 1
    EXCEPT 
    SELECT a.monday::timestamp without time zone 
    FROM (SELECT monday FROM tmp.cdr_simpson_index GROUP BY 1) AS a );
  

BEGIN

    
   Truncate tmp.cdr_simpson_index;

      /*  FOR monday_time IN (
    SELECT date_trunc('week',call_time) FROM tmp.cdr_full_weeks GROUP BY 1
    EXCEPT 
    SELECT a.monday::timestamp without time zone 
    FROM (SELECT monday FROM tmp.cdr_simpson_index GROUP BY 1) AS a
  ) LOOP */
    FOR monday_time IN (select unnest(monday_list))
    LOOP
    
    max_time := monday_time + '7 days'::interval;
    min_time := max_time - long_interval;
    
	
    INSERT INTO tmp.cdr_simpson_index (
      alias_id,
      monday,      
      alltime_long_diversity,
      leisure_long_diversity,
      business_long_diversity,    
      alltime_short_diversity,
      leisure_short_diversity,
      business_short_diversity)
    SELECT
      dddd.alias_id,
      monday_time::date AS monday,
      sum(dddd.alltime_long_count * (dddd.alltime_long_count - 1.0))::double precision / greatest(1e-10, sum(dddd.alltime_long_count) * (sum(dddd.alltime_long_count) - 1.0)) AS alltime_long_diversity, ----------- Simpson diversity index
      sum(dddd.leisure_long_count * (dddd.leisure_long_count - 1.0))::double precision / greatest(1e-10, sum(dddd.leisure_long_count) * (sum(dddd.leisure_long_count) - 1.0)) AS leisure_long_diversity, ----------- Simpson diversity index
      sum(dddd.business_long_count * (dddd.business_long_count - 1.0))::double precision / greatest(1e-10, sum(dddd.business_long_count) * (sum(dddd.business_long_count) - 1.0)) AS business_long_diversity, ------ Simpson diversity index
      sum(dddd.alltime_short_count * (dddd.alltime_short_count - 1.0))::double precision / greatest(1e-10, sum(dddd.alltime_short_count) * (sum(dddd.alltime_short_count) - 1.0)) AS alltime_short_diversity, ------ Simpson diversity index
      sum(dddd.leisure_short_count * (dddd.leisure_short_count - 1.0))::double precision / greatest(1e-10, sum(dddd.leisure_short_count) * (sum(dddd.leisure_short_count) - 1.0)) AS leisure_short_diversity, ------ Simpson diversity index
      sum(dddd.business_short_count * (dddd.business_short_count - 1.0))::double precision / greatest(1e-10, sum(dddd.business_short_count) * (sum(dddd.business_short_count) - 1.0)) AS business_short_diversity -- Simpson diversity index
    FROM (
      SELECT
        ddd.alias_id,
        ddd.location_id,
        ddd.alltime_long_count,
        ddd.alltime_short_count,
        ddd.leisure_long_count,
        ddd.leisure_short_count,
        ddd.business_long_count,
        ddd.business_short_count
      FROM (
        SELECT
          dd.alias_id,
          dd.location_id,
          sum(dd.long_count) AS alltime_long_count,
          sum(dd.short_count) AS alltime_short_count,
          sum(CASE WHEN dd.is_business = 0 THEN dd.long_count ELSE 0 END) AS leisure_long_count,
          sum(CASE WHEN dd.is_business = 0 THEN dd.short_count ELSE 0 END) AS leisure_short_count,
          sum(CASE WHEN dd.is_business = 1 THEN dd.long_count ELSE 0 END) AS business_long_count,
          sum(CASE WHEN dd.is_business = 1 THEN dd.short_count ELSE 0 END) AS business_short_count
        FROM (
          SELECT
            d.alias_a AS alias_id,
            d.a_cell_id AS location_id,
            CASE 
              WHEN extract(DOW FROM d.call_time) BETWEEN 1 AND 5  -- From Sunday to Thursday (weekday in Finland)
              AND extract(HOUR FROM d.call_time) BETWEEN 8 AND 16 -- Business hour
              THEN 1 ELSE 0 
            END AS is_business,
            count(*) AS long_count,
            sum(CASE WHEN max_time - d.call_time < short_interval THEN 1 ELSE 0 END) AS short_count
          FROM data.cdr AS d
          WHERE d.call_time < max_time 
          AND d.call_time >= min_time
          GROUP BY alias_id, location_id, is_business
        ) AS dd
        GROUP BY dd.alias_id, dd.location_id
      ) AS ddd
    ) AS dddd
    GROUP BY dddd.alias_id;

  END LOOP;

  ANALYZE tmp.cdr_simpson_index;

  INSERT INTO work.modelling_data_matrix_geolocation
  ( mod_job_id, alias_id, mobility,alltime_long_diversity,leisure_long_diversity, business_long_diversity, alltime_short_diversity, leisure_short_diversity, business_short_diversity)
      SELECT
        in_mod_job_id AS mod_job_id,
        mt.alias_id, 
        b.mobility,
		c.alltime_long_diversity,
		c.leisure_long_diversity, 
		c.business_long_diversity, 
		c.alltime_short_diversity, 
		c.leisure_short_diversity, 
		c.business_short_diversity
        FROM work.module_targets AS mt 
        LEFT JOIN 
          (
		  SELECT
		    alias_id,
			SUM(-LOG((n_sub+0.001)/(tot_sub+0.001))*n_sub/tot_sub) AS mobility 
		  FROM	  
		     (SELECT 
			  alias_a AS alias_id, 
			  a_cell_id AS cell_id,
			  COUNT(*) AS n_sub,
              SUM(COUNT(*)) OVER (PARTITION BY alias_a) AS tot_sub 			  
			  FROM data.cdr aa 
			  WHERE aa.a_cell_id IS NOT NULL 
			  AND aa.call_time >= to_timestamp(t1_text,'YYYY-MM-DD') 
			  AND aa.call_time < to_timestamp(t2_text,'YYYY-MM-DD')
			  GROUP BY a_cell_id, alias_a 
			  ) kk
			  GROUP BY alias_id
		  )AS b
        ON mt.alias_id = b.alias_id
		LEFT JOIN
		(
		 SELECT
		  alias_id, 
		  AVG(alltime_long_diversity) AS alltime_long_diversity,
		  AVG(leisure_long_diversity) AS leisure_long_diversity,
		  AVG(business_long_diversity) AS business_long_diversity,
		  AVG(alltime_short_diversity) AS alltime_short_diversity,
		  AVG(leisure_short_diversity) AS leisure_short_diversity,
		  AVG(business_short_diversity) AS business_short_diversity
		  FROM tmp.cdr_simpson_index aa 
		  WHERE aa.monday >= to_date(t1_text,'YYYY-MM-DD') 
		  AND aa.monday < to_date(t2_text,'YYYY-MM-DD')
		  GROUP BY alias_id	
		) AS c
		ON mt.alias_id = c.alias_id
        WHERE mt.mod_job_id = in_mod_job_id	;

  PERFORM core.analyze('work.modelling_data_matrix_geolocation', in_mod_job_id);
   


END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.create_modelling_data_geolocation(integer)
  OWNER TO xsl;

  

CREATE OR REPLACE FUNCTION work.initialize_lda(in_mod_job_id integer, doc_value text, term_value text, in_lda_model_options text, in_lda_output_id_old integer, in_lda_input_options text, in_lda_input_id integer)
  RETURNS integer[] AS
$BODY$
/* SUMMARY:
 * This function initializes the handset-cellid LDA run for the given module job. 
 *
 * 2014-10-09 QYu: Set doc and term value as input parameters, so that the function is generalized for other cases
 * 2013-08-12 HMa: Moved out of function work.initialize_wf_muc_common
 */

DECLARE

  run_lda                     boolean;
  use_existing_lda            boolean;
  fit_new_lda                 boolean;
  update_lda                  boolean;
  calculate_new_lda_input     boolean;

  calculate_lda_input_data    boolean;
  calculate_lda_predictors    boolean;
  lda_input_id                int;
  lda_out_id                  int;
  lda_output_id_old           int;

  id_not_found                boolean;
  id_wrong_type               boolean;

  t2                          date; 

BEGIN

  -- Evaluate which parts of the LDA subflow are run
  use_existing_lda := in_lda_model_options ~ 'Use existing';
  fit_new_lda := in_lda_model_options ~ 'Fit new';
  update_lda := in_lda_model_options ~ 'Update';
  calculate_new_lda_input := in_lda_input_options ~ 'Calculate new';
  calculate_lda_predictors := use_existing_lda OR fit_new_lda OR update_lda;
  run_lda := fit_new_lda OR update_lda;

  t2 := value::date FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id AND key = 't2';

    --  LDA parameters  --
    IF calculate_lda_predictors THEN 
 
      -- If an old model is used/updated, check if an old LDA model is actually available
      IF use_existing_lda OR update_lda THEN 
      
        -- If no old LDA output id is given, use the latest one as a default
        IF in_lda_output_id_old < 0 THEN -- No LDA output id is given
        
          -- Check if there are LDA output data of type 'handset_model' from the current source period. If there are, use/update that one. 
          lda_out_id := 
            MAX(l2.lda_output_id)
            FROM work.lda_output_parameters l2
            INNER JOIN work.lda_output_parameters l3
            ON l2.lda_output_id = l3.lda_output_id
            INNER JOIN work.lda_output_parameters l4
            ON l2.lda_output_id = l4.lda_output_id
            INNER JOIN work.lda_output lda
            ON l2.lda_output_id = lda.lda_id
            AND l2.key = 't_end' AND l2.value = t2::text
            AND l3.key = 'doc' AND l3.value = doc_value
            AND l4.key = 'term' AND l4.value = term_value;
        
          -- If there are no LDA output data from the current source period, check if there are old LDA output data that can be updated. 
          IF lda_out_id IS NULL THEN
            lda_output_id_old :=   
              MAX(p.lda_output_id)
              FROM work.lda_output_parameters p
              INNER JOIN work.lda_output l
              ON p.lda_output_id = l.lda_id
              WHERE p.key = 'term' 
              AND p.value = term_value;
          
            IF lda_output_id_old IS NULL THEN -- No old LDA data found -> fit a new LDA model
              run_lda := TRUE;
              fit_new_lda = TRUE;
              use_existing_lda = FALSE;
              update_lda = FALSE;
              lda_out_id = -1;
              lda_output_id_old = -1;
            ELSE -- Old LDA output data found -> update the old LDA model
              run_lda := TRUE;
              update_lda = TRUE;
              use_existing_lda = FALSE;
              fit_new_lda = FALSE;
              lda_out_id = -1;
            END IF;
        
          ELSE -- LDA model output from the current source period found
            IF use_existing_lda THEN -- Use the output of the existing LDA model of the current source period
              lda_output_id_old = -1;
            ELSIF update_lda THEN -- Update the existing LDA model from the current source period (useful, for example, if more iterations are needed.)
              lda_output_id_old = lda_out_id;
              lda_out_id = -1;
            END IF;
        
          END IF;
        
        ELSE -- LDA output id is given
        
          -- Check if the given LDA model ID is valid:
          EXECUTE 'SELECT (SELECT '|| in_lda_output_id_old ||' INTERSECT SELECT lda_output_id FROM work.lda_output_parameters GROUP BY lda_output_id) IS NULL' INTO id_not_found;
          EXECUTE 'SELECT (SELECT lda_output_id FROM work.lda_output_parameters WHERE lda_output_id = '|| in_lda_output_id_old ||' AND "key" = ''term'' AND "value" = term_value) IS NULL' INTO id_wrong_type;
          IF id_not_found THEN 
            RAISE EXCEPTION 'The given LDA model id is not found';
            ELSIF id_wrong_type THEN 
            RAISE EXCEPTION 'The given LDA model id does not correspond to a handset LDA model';
          END IF;
          
          IF use_existing_lda THEN
            lda_out_id = in_lda_output_id_old;
            lda_output_id_old = -1;
            ELSIF update_old_lda THEN
            lda_output_id_old = in_lda_output_id_old;
            lda_out_id = -1;
          END IF;
          
        END IF;
        
      ELSE -- no old model is used or updated
        lda_output_id_old = -1;      
        lda_out_id = -1;
      END IF;
  
      -- If a new model is fitted or an old model is updated, check if input data are available or if new input data should be calculated. 
      IF fit_new_lda OR update_lda THEN 
      
        IF calculate_new_lda_input THEN -- If new input data are required
          calculate_lda_input_data := TRUE; -- new input data will be calculated
          lda_input_id = -1;
        
        ELSIF in_lda_input_id < 0 THEN -- No LDA input id is given
      
          --Check if handset LDA input data has been calculated for the current source period:
          lda_input_id :=
            MAX(l2.lda_input_id)
            FROM work.lda_input_parameters l2
            INNER JOIN work.lda_input_parameters l3
            ON l2.lda_input_id = l3.lda_input_id
            INNER JOIN work.lda_input_parameters l4
            ON l2.lda_input_id = l4.lda_input_id
            INNER JOIN work.lda_input lin
            ON l2.lda_input_id = lin.lda_id
            WHERE l2.key = 't_end' AND l2.value = t2::text
            AND l3.key = 'doc' AND l3.value = doc_value
            AND l4.key = 'term' AND l4.value = term_value;
  
          -- Determine if handset LDA input data will be calculated:
          IF lda_input_id IS NULL THEN -- if no LDA input data has been calculated for the current source period 
            calculate_lda_input_data := TRUE; -- new input data will be calculated
            lda_input_id = -1;
          ELSE -- Input data found for the current source period
            calculate_lda_input_data := FALSE; -- Input data is not calculated - old input data used
          END IF;  
          
        ELSE -- lda_input_id given
          
          -- Check if the given LDA input ID is valid:
          EXECUTE 'SELECT (SELECT '|| in_lda_input_id ||' INTERSECT SELECT lda_input_id FROM work.lda_input_parameters GROUP BY lda_input_id) IS NULL' INTO id_not_found;
          EXECUTE 'SELECT (SELECT lda_input_id FROM work.lda_input_parameters WHERE lda_input_id = ' || in_lda_input_id || ' AND "key" = ''term'' AND "value" = term_value) IS NULL' INTO id_wrong_type;
          IF id_not_found THEN 
            RAISE EXCEPTION 'The given LDA input id is not found!';
          ELSIF id_wrong_type THEN -- No handset LDA for the given LDA output id
            RAISE EXCEPTION 'The given LDA input id does not correspond to a handset LDA model';
          ELSE -- The given LDA input ID is valid
            calculate_lda_input_data := FALSE;
            lda_input_id = in_lda_input_id;
          END IF;
            
        END IF;
      
      ELSE -- No model fitted / updated -> no need to calculate input data
        calculate_lda_input_data := FALSE;
        lda_input_id = -1;
      END IF;
      
    ELSE -- LDA is not run and no LDA predictors are calculated
      calculate_lda_input_data = FALSE;
      lda_output_id_old = -1;  
      lda_out_id = -1;  
      lda_input_id = -1;  
    END IF;

    RETURN ARRAY[run_lda::integer, calculate_lda_input_data::integer, calculate_lda_predictors::integer, lda_input_id, lda_out_id, lda_output_id_old];

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.initialize_lda(integer,text,text, text, integer, text, integer)
  OWNER TO xsl;

DROP FUNCTION work.initialize_wf_muc_common(integer, integer, integer, integer, integer, integer, boolean, text[], boolean[], text, date, integer, text, integer, text, integer, text, integer, text[], text[], boolean);

CREATE OR REPLACE FUNCTION work.initialize_wf_muc_common(source_period_length integer, post_source_period_length integer, lag_count1 integer, max_data_age integer, crm_data_age integer, n_handset_topics integer, n_cell_events_topics integer, override_model_variables_check boolean, in_use_cases text[], in_included_use_cases boolean[], in_run_type text, in_t2 date, in_mod_job_id integer, in_run_descvar text, in_descvar_interval_weeks integer, in_lda_model_options text, in_lda_output_id_old integer, in_lda_input_options text, in_lda_input_id integer,in_lda_cell_events_model_options text, in_lda_cell_events_output_id_old integer, in_lda_cell_events_input_options text, in_lda_cell_events_input_id integer, in_keys text[], in_vals text[], in_check_data boolean)
  RETURNS SETOF text AS
$BODY$
/* SUMMARY:
 * This function initializes common parameters for the multiple use case (MUC) work flow.
 * Initially based on initialize_wf_prepaid_churn.
 *
 * Inputs:
 * source_period_length  
 * post_source_period_length ,  
 * lag_count1 ,
 * max_data_age , -- CDR and top-up data can not be older
 * crm_data_age, -- CRM data can not be older
 * n_handset_topics , -- Number of topics needs to be predefined, because the corresponding columns are needed in work.modelling_data_matrix
 * override_model_variables_check boolean,  -- Force all predictors to be calculated in work.create_modelling_data1/2/3
 * 
 * in_use_cases              All use cases included in the workflow, e.g. ARRAY['churn_inactivity', 'zero_day_prediction']
 * in_included_use_cases     Flags use cases to include in this mod_job, e.g. ARRAY[false, true]
 * in_run_type               'Predictors' or 'Predictors + Apply' or 'Predictors + Fit + Apply'
 * in_t2                     Source period end date
 * in_mod_job_id             Module job id; if <0, a new job is created
 * 
 * in_run_descvar            Run type of descriptive variables: 'Run', 'Run every given interval', or 'Do not run'
 * in_descvar_interval_weeks If in_run_descvar = 'Run every given interval', descriptive variables are calculated every 'descvar_interval_weeks' weeks
 * 
 * in_lda_model_options      LDA parameters - see LDA documentation or ask HMa
 * in_lda_output_id_old
 * in_lda_input_options
 * in_lda_input_id
 *
 * in_keys                   Optional keys and values to include in work.module_job_parameters table
 * in_vals
 *
 * 2014-06-06 QYu: Move the hard coded parameters to a separate table: work.scoring_muc_parameters
 * 2013-08-13 HMa: Moved LDA initialization to its own function
 * 2013-05-13 KL : Added secondary network initialization
 * 15.04.2013 KL Added arpu_query for network scorer as a parameter to be initialized
 * 2013-03-08 MOj: Fixed a bug in data quality check and usage of t9
 * 2013-02-21 HMa: Added the option to run only descriptive variables
 * 2013-01-10 JVi: Created from "work".initialize_wf_prepaid_churn
 */

DECLARE
  --Definitions
  lag_length                  int := ceil(source_period_length::double precision / lag_count1);
  lag_count                   int;
  
  --Variables and parameters
  t1                          date; --source period start
  t2                          date; --source period end
  t9                          date; --end of data period (i.e. t2 + post_source_period_length if model is fitted, otherwise same as t2)
  tcrm                        date;
  job_use_cases               text;

  this_mod_job_id             int;
  network_id                  int;
  network_2_id                int;
  network_weeks               text;
  
  calculate_predictors        boolean;
  fit_model                   boolean;
  apply_model                 boolean;
  run_type                    text := in_run_type;
  calculate_only_descvar      boolean;
  calculate_targets           boolean; --this means only target audience (no data needed after t2)

  calculate_targets_text      text;

  --Descriptive variables specific
  calculate_descvar           boolean;
  force_new_network           boolean;
  max_t2_descvar              date;

  --LDA-specific
  lda_params                  integer[];
  run_lda                     int;
  calculate_lda_input_data    int;
  calculate_lda_predictors    int;
  lda_input_id                int;
  lda_out_id                  int;
  lda_output_id_old           int;

  lda_cell_events_params                  integer[];
  run_lda_cell_events                     int;
  calculate_lda_cell_events_input_data    int;
  calculate_lda_cell_events_predictors    int;
  lda_cell_events_input_id                int;
  lda_cell_events_out_id                  int;
  lda_cell_events_output_id_old           int;
  
  
  data_check_end              date;
  data_status                 record;
  cdr_latest                  date;
  topup_latest                date;
  date_latest                 date;
  topup_data				  boolean;

  arpu_query                  text; --query for network scorer


  --Miscellaneous
  keys text[] := in_keys;
  vals text[] := in_vals;
  tmp int; --Used in for loops etc.

BEGIN
 
  
  calculate_predictors := run_type ~ 'Predictors' AND in_mod_job_id < 1;
  fit_model := run_type ~ 'Fit';
  apply_model := run_type ~ 'Apply';
  calculate_only_descvar := run_type ~ 'Descriptive variables only';
  calculate_targets := calculate_predictors OR (calculate_only_descvar AND in_mod_job_id < 1); --this means only target audience (no data needed after t2)

  --Remove 'Predictors' from run type if mod_job_id is given
  IF run_type ~ 'Predictors' AND in_mod_job_id > 0 THEN
    run_type := regexp_replace(run_type, E'Predictors \\+ ', '');
    run_type := regexp_replace(run_type, 'Predictors', '');
  END IF;
 
  --Parameters for calculate predictors
  IF calculate_predictors OR calculate_targets THEN 

    -- Evaluate which use cases are run
    tmp := count(*) from unnest(in_use_cases);
    job_use_cases := '';
    FOR i IN 1..tmp LOOP
      IF in_included_use_cases[i] THEN
        job_use_cases := job_use_cases || in_use_cases[i] || ',';
      END IF;
    END LOOP;
    job_use_cases := TRIM(TRAILING ',' FROM job_use_cases);
    
    --Create a new mod_job_id
    this_mod_job_id := core.nextval_with_partitions('work.module_sequence');

    --Evaluate source period end date
    IF in_t2 IS NOT NULL THEN
      t2 := date_trunc('week', in_t2);
      t9 := t2;
      IF fit_model OR (NOT fit_model AND NOT apply_model AND NOT calculate_only_descvar) THEN --Fit or only predictors -> assume fitting
        t9 := t9 + post_source_period_length;
      END IF;
    ELSE --Use maximum possible end date
      cdr_latest := coalesce(max(data_date),'1900-01-01'::date) FROM data.data_quality WHERE data_source='cdr' AND status = 2;
      topup_latest := coalesce(max(data_date),'1900-01-01'::date) FROM data.data_quality WHERE data_source = 'topup' AND status = 2;
      -- Use Monday of the last week for which all the data are available: 
	  topup_data := (job_use_cases ~ 'churn_inactivity' OR job_use_cases ~ 'zero_day_prediction');
	  
	  date_latest := cdr_latest;
	  IF (topup_data) THEN
	    date_latest := least(date_latest, topup_latest);
      END IF;
      IF extract(DOW FROM date_latest) = 0 THEN -- Sunday -> we have all data from that week
        t9 := date_latest + 1; -- Monday next week
      ELSE -- other than Sunday -> we do not have all data from that week
        t9 := date_trunc('week', date_latest);
      END IF;
      t2 := t9;
      IF fit_model OR (NOT fit_model AND NOT apply_model AND NOT calculate_only_descvar) THEN --Fit or only predictors -> assume fitting
        t2 := t2 - post_source_period_length;
      END IF;
    END IF;

    -- Evaluate source period start date and last customer record date
    t1 := t2 - source_period_length;  
    tcrm := max(m.date_inserted) 
            FROM data.data_quality dq
            JOIN data.crm_snapshot_date_inserted_mapping m
            ON dq.data_date = m.snapshot_date
            WHERE dq.data_source = 'crm' AND dq.status = 2
            AND m.date_inserted <= t2;

    -- Data availability check
    data_check_end := t9; 

    -- Availability check
    -- CDR
    SELECT min(coalesce(qual.status,0)) AS min_status, max(coalesce(qual.status,100)) AS max_status INTO data_status
    FROM (
      SELECT t1 + generate_series(0,(data_check_end - t1 - 1)) AS data_date
    ) dates
    LEFT JOIN (
      SELECT data_date, status 
      FROM data.data_quality 
      WHERE data_source = 'cdr'
    ) qual
    ON qual.data_date = dates.data_date;

    IF (data_status.min_status < 2) THEN
      RAISE EXCEPTION 'Part or all of the CDR data is not available for period % to %.', t1, data_check_end;
    END IF;

	IF (data_status.max_status > 2) THEN
      RAISE EXCEPTION 'Part or all of the CDR data is not valid for period % to %.', t1, data_check_end;
    END IF;

   -- TOPUP data is checked only in case of churn_inactivity and zero day prediction
	IF (topup_data) THEN
  	  SELECT min(coalesce(qual.status,0)) AS min_status, max(coalesce(qual.status,100)) AS max_status INTO data_status
        FROM (
          SELECT t1 + generate_series(0,(data_check_end - t1 - 1)) AS data_date
        ) dates
      LEFT JOIN (
        SELECT data_date, status
        FROM data.data_quality 
	    WHERE data_source = 'topup'
      ) qual
      ON qual.data_date = dates.data_date;

	  IF (data_status.min_status < 2) THEN
        RAISE EXCEPTION 'Part or all of the TOPUP data is not available for period % to %.', t1, data_check_end;
      END IF;
	  IF (data_status.max_status > 2) THEN
        RAISE EXCEPTION 'Part or all of the TOPUP data is not valid for period % to %.', t1, data_check_end;
      END IF;
	END IF;

    -- CRM
    IF tcrm IS NULL OR t2 - tcrm > crm_data_age THEN
      RAISE EXCEPTION 'CRM data is older than allowed (% days) or not available.', crm_data_age;
    END IF;

    -- Too old data 
    IF in_check_data THEN
      -- CDR & TOPUP
      IF in_t2 IS NULL AND (now()::date - data_check_end > max_data_age) THEN -- Do not check it if the t2 is given
        RAISE EXCEPTION 'Latest data (%) is older then allowed (% days).', data_check_end , max_data_age;
      END IF;
    END IF;  

    --Parse network_id
    IF keys && ARRAY['xsl_job_id'] THEN
      network_id := d.value::integer FROM (SELECT unnest(keys) AS key, unnest(vals) AS value) AS d WHERE d.key = 'xsl_job_id';
    ELSE
      network_id := core.nextval_with_partitions('work.network_sequence');
    END IF;    

    --Parse secondary network_id
    IF keys && ARRAY['xsl_job_id_2'] THEN
      network_2_id := d.value::integer FROM (SELECT unnest(keys) AS key, unnest(vals) AS value) AS d WHERE d.key = 'xsl_job_id_2';
    ELSE
      network_2_id := core.nextval_with_partitions('work.network_sequence');
    END IF; 

    --Store the values
    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES 
     (this_mod_job_id, 't1',            t1::text),
     (this_mod_job_id, 't2',            t2::text),
     (this_mod_job_id, 't9',            t9::text),
     (this_mod_job_id, 'tCRM',          tcrm::text),
     (this_mod_job_id, 'lag_count',     lag_count1::text),
     (this_mod_job_id, 'lag_length',    lag_length::text),
     (this_mod_job_id, 'xsl_job_id',    network_id::text),
     (this_mod_job_id, 'job_type',      'multiple_use_cases'),
     (this_mod_job_id, 'job_use_cases', job_use_cases),
     (this_mod_job_id, 'xsl_job_id_2',  network_2_id::text);


    INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
    VALUES (this_mod_job_id, 'time_inserted', now()::text);

    SELECT
      *
    FROM work.initialize_lda(this_mod_job_id, 'events', 'cell_id', in_lda_model_options, in_lda_output_id_old, in_lda_input_options, in_lda_input_id)
    INTO lda_params;

    run_lda := lda_params[1];
    calculate_lda_input_data := lda_params[2];
    calculate_lda_predictors := lda_params[3];
    lda_input_id := lda_params[4];
    lda_out_id := lda_params[5];
    lda_output_id_old := lda_params[6];
	
	 SELECT
      *
    FROM work.initialize_lda(this_mod_job_id, 'cell_id', 'handset_model', in_lda_cell_events_model_options, in_lda_cell_events_output_id_old, in_lda_cell_events_input_options, in_lda_cell_events_input_id)
    INTO lda_cell_events_params;

    run_lda_cell_events := lda_cell_events_params[1];
    calculate_lda_cell_events_input_data := lda_cell_events_params[2];
    calculate_lda_cell_events_predictors := lda_cell_events_params[3];
    lda_cell_events_input_id := lda_cell_events_params[4];
    lda_cell_events_out_id := lda_cell_events_params[5];
    lda_cell_events_output_id_old := lda_cell_events_params[6];

  ELSE 
    --If predictors are not calculated, mod_job_id is given.    
    this_mod_job_id := in_mod_job_id; 

    --The t2 date should not be given.
    IF in_t2 IS NOT NULL THEN 
      RAISE EXCEPTION 'The source period end date t2 should not be given because existing predictors and the corresponding t2 date are used.';
    END IF;    

    IF in_run_type != run_type THEN 
      RAISE WARNING 'Resetting the run_type and calculate_targets in work.module_job_parameters';
      UPDATE work.module_job_parameters mjp SET "value" = run_type WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'run_type';
      UPDATE work.module_job_parameters mjp SET "value" = 'false' WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key = 'calculate_targets';
    END IF;
    
    --Get the output parameters from db 
    t1            := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't1';
    t2            := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't2';
    t9            := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 't9';
    tcrm          := mjp.value::date FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND lower(mjp.key) = 'tcrm';
    lag_count     := mjp.value::int  FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'lag_count';
    lag_length    := mjp.value::int  FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'lag_length';    
    network_id    := mjp.value::int  FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'xsl_job_id';
    job_use_cases := mjp.value       FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id AND mjp.key        = 'job_use_cases';
  END IF;

  --Evaluate if descriptive variables are calculated
  IF calculate_only_descvar THEN
    calculate_descvar = TRUE;
  ELSIF in_run_descvar = 'Run' THEN
    calculate_descvar = TRUE;
  ELSIF in_run_descvar = 'Run every given interval' THEN

    -- Find the latest t2 for a descriptive variables run 
    SELECT max(to_date(b.value, 'YYYY-MM-DD'))
    FROM work.module_job_parameters AS a
    INNER JOIN work.module_job_parameters AS b
    ON a.mod_job_id = b.mod_job_id
    INNER JOIN results.descriptive_variables_subscriber_matrix AS dv
    ON a.mod_job_id = dv.mod_job_id
    WHERE a.key = 'job_type' 
    AND a.value = 'descriptive_variables' 
    AND b.key = 't2'
    INTO max_t2_descvar;

    IF max_t2_descvar IS NULL THEN
      calculate_descvar = TRUE;
    ELSIF t2 - max_t2_descvar >= 7 * in_descvar_interval_weeks THEN
      calculate_descvar = TRUE;
    ELSE 
      calculate_descvar = FALSE;
    END IF;

  ELSE
    calculate_descvar = FALSE;
  END IF;

  force_new_network = FALSE;

  --Evaluate source period weeks
  network_weeks := core.concatenate_yyyyww(t1, t2, ',');

  IF calculate_targets THEN
    calculate_targets_text := 'true';
  ELSE 
    calculate_targets_text := 'false';
  END IF;

  --Add parameters to keys and values
  keys := keys || ARRAY['run_type','calculate_targets','override_model_variables_check'];
  vals := vals || ARRAY[run_type,calculate_targets_text, (CASE WHEN override_model_variables_check THEN 'true' ELSE 'false' END)];
  
  --Check if some keys are already in the work.module_job_parameters and raise a warning
  tmp := count(*) FROM ( SELECT unnest(keys) INTERSECT SELECT mjp.key FROM work.module_job_parameters mjp WHERE mjp.mod_job_id = this_mod_job_id ) a;
  IF tmp > 0 THEN 
    RAISE WARNING 'Some of the keys are already in the work.module_job_parameters table and they are not stored even if they differ.';
  END IF;

  --Insert all key-value pairs to work.module_job_parameters that are not there yet
  INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
  SELECT this_mod_job_id AS mod_job_id, d.key, d.value
  FROM (SELECT unnest(keys) AS key, unnest(vals) AS value) AS d
  LEFT OUTER JOIN work.module_job_parameters mjp 
  ON mjp.mod_job_id = this_mod_job_id AND mjp.key = d.key
  WHERE d.key IS NOT NULL
  AND mjp.key IS NULL;

  arpu_query='SELECT alias_id, monthly_arpu AS arpu
              FROM work.monthly_arpu
              WHERE mod_job_id = '|| this_mod_job_id ||';'; 
       


  PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);

  RETURN NEXT this_mod_job_id::text; ------------------------------------------------ 0
  RETURN NEXT network_id::text;  ---------------------------------------------------- 1
  RETURN NEXT network_weeks; -------------------------------------------------------- 2
  RETURN NEXT t1::text; ------------------------------------------------------------- 3
  RETURN NEXT t2::text; ------------------------------------------------------------- 4
  RETURN NEXT lag_count1::text; ------------------------------------------------------ 5
  RETURN NEXT lag_length::text; ----------------------------------------------------- 6
  RETURN NEXT source_period_length::text; ------------------------------------------- 7
  RETURN NEXT CASE WHEN calculate_predictors THEN 'true' ELSE 'false' END; ---------- 8
  RETURN NEXT CASE WHEN fit_model THEN 'true' ELSE 'false' END; --------------------- 9
  RETURN NEXT CASE WHEN apply_model THEN 'true' ELSE 'false' END; ------------------- 10
  RETURN NEXT CASE WHEN run_lda = 1 THEN 'true' ELSE 'false' END;---------------------11
  RETURN NEXT CASE WHEN calculate_lda_input_data = 1 THEN 'true' ELSE 'false' END;----12
  RETURN NEXT CASE WHEN calculate_lda_predictors = 1 THEN 'true' ELSE 'false' END;----13
  RETURN NEXT lda_input_id::text; ----------------------------------------------------14
  RETURN NEXT lda_output_id_old::text; -----------------------------------------------15
  RETURN NEXT lda_out_id::text; ------------------------------------------------------16
  RETURN NEXT n_handset_topics::text; ------------------------------------------------17
  RETURN NEXT job_use_cases::text; ---------------------------------------------------18
  RETURN NEXT tcrm::text; ------------------------------------------------------------19
  RETURN NEXT CASE WHEN calculate_descvar THEN 'true' ELSE 'false' END;---------------20
  RETURN NEXT CASE WHEN force_new_network THEN 'true' ELSE 'false' END;---------------21
  RETURN NEXT CASE WHEN calculate_targets THEN 'true' ELSE 'false' END;---------------22
  RETURN NEXT arpu_query;-------------------------------------------------------------23
  RETURN NEXT network_2_id;-----------------------------------------------------------24
  RETURN NEXT lda_cell_events_input_id::text; ----------------------------------------25
  RETURN NEXT lda_cell_events_output_id_old::text; -----------------------------------26
  RETURN NEXT lda_cell_events_out_id::text; ------------------------------------------27
  RETURN NEXT n_cell_events_topics::text; --------------------------------------------28
  RETURN;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.initialize_wf_muc_common(integer, integer, integer, integer, integer, integer, integer, boolean, text[], boolean[], text, date, integer, text, integer, text, integer, text, integer, text, integer, text, integer, text[], text[], boolean)
  OWNER TO xsl;

