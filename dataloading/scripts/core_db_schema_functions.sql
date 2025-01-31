
-- Copyright (c) 2009 Xtract Oy. All rights reserved.  
-- This software is the proprietary information of Xtract Oy. 
-- Use is subject to license terms. --

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.create_cdr_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Creates partitions for CDR tables based on the contents of tmp.cdr_staging. 
 * The partitioned tables are: 
 * - data.cdr
 * - data.call_types_weekly
 * - data.in_split_weekly
 * - data.in_split_aggregates
 *
 * VERSION
 * 03.04.2013 HMa
 */
DECLARE

  datelist  date[];

  clevel    integer;
  clup      boolean;
  d         record;

BEGIN

  -- Find the dates that are found in tmp.cdr_staging:
  datelist := ARRAY(SELECT call_time::date AS data_date FROM tmp.cdr_staging GROUP BY data_date);

  -- data.cdr -- 

  -- Read compresslevel and cleanup status: 
  SELECT
    compresslevel,
    cleanup
  FROM core.partition_date_tables
  WHERE table_name = 'data.cdr'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT s.data_date
    FROM (SELECT unnest(datelist) AS data_date) AS s
    LEFT JOIN (SELECT data_date FROM core.partition_date_create_times WHERE table_name = 'data.cdr') AS p
    ON s.data_date = p.data_date
    WHERE p.data_date IS NULL -- make sure we do not try to create partitions that already exist
    AND s.data_date IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_date_create_times (table_name, data_date, time_created, cleanup)
    VALUES ('data.cdr', d.data_date, now(), clup);

    EXECUTE 'ALTER TABLE data.cdr '
         || 'ADD PARTITION "' || d.data_date::text || '" '
         || 'START (''' || d.data_date || '''::date) END (''' || (d.data_date + 1) || '''::date) '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

  -- data.call_types_weekly -- 

  -- Read compresslevel and cleanup status: 
  SELECT
    compresslevel,
    cleanup
  FROM core.partition_date_tables
  WHERE table_name = 'data.call_types_weekly'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT s.data_date
    FROM (SELECT date_trunc('week', unnest(datelist))::date AS data_date GROUP BY data_date) AS s
    LEFT JOIN (SELECT data_date FROM core.partition_date_create_times WHERE table_name = 'data.call_types_weekly') AS p
    ON s.data_date = p.data_date
    WHERE p.data_date IS NULL -- make sure we do not try to create partitions that already exist
    AND s.data_date IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_date_create_times (table_name, data_date, time_created, cleanup)
    VALUES ('data.call_types_weekly', d.data_date, now(), clup);

    EXECUTE 'ALTER TABLE data.call_types_weekly '
         || 'ADD PARTITION "' || d.data_date::text || '" '
         || 'START (''' || d.data_date || '''::date) END (''' || (d.data_date + 1) || '''::date) '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

  -- data.in_split_weekly --

  SELECT
    compresslevel,
    cleanup
  FROM core.partition_period_tables
  WHERE table_name = 'data.in_split_weekly'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT s.period
    FROM (SELECT core.date_to_yyyyww(unnest(datelist)) AS period GROUP BY period) AS s
    LEFT JOIN (SELECT period FROM core.partition_period_create_times WHERE table_name = 'data.in_split_weekly') AS p
    ON s.period = p.period
    WHERE p.period IS NULL -- make sure we do not try to create partitions that already exist
    AND s.period IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_period_create_times (table_name, period, time_created, cleanup)
    VALUES ('data.in_split_weekly', d.period, now(), clup);

    EXECUTE 'ALTER TABLE data.in_split_weekly '
         || 'ADD PARTITION "' || d.period || '" '
         || 'START (' || d.period || ') END (' || (d.period + 1) || ') '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

  -- data.in_split_aggregates --

  SELECT
    compresslevel,
    cleanup
  FROM core.partition_period_tables
  WHERE table_name = 'data.in_split_aggregates'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT s.period
    FROM (SELECT core.date_to_yyyyww(unnest(datelist)) AS period GROUP BY period) AS s
    LEFT JOIN (SELECT period FROM core.partition_period_create_times WHERE table_name = 'data.in_split_aggregates') AS p
    ON s.period = p.period
    WHERE p.period IS NULL -- make sure we do not try to create partitions that already exist
    AND s.period IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_period_create_times (table_name, period, time_created, cleanup)
    VALUES ('data.in_split_aggregates', d.period, now(), clup);

    EXECUTE 'ALTER TABLE data.in_split_aggregates '
         || 'ADD PARTITION "' || d.period || '" '
         || 'START (' || d.period || ') END (' || (d.period + 1) || ') '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.create_cdr_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.create_crm_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Creates partitions for CRM tables based on the contents of tmp.crm_staging. 
 * The partitioned tables are: 
 * - data.crm
 * - data.in_crm
 *
 * VERSION
 * 03.04.2013 HMa
 */
DECLARE

  datelist           date[];
  date_inserted_list date[];

  clevel             integer;
  clup               boolean;
  d                  record;

BEGIN

  -- Find the timestamps that are found in tmp.crm_staging:
  datelist := ARRAY(SELECT "timestamp"::date AS data_date FROM tmp.crm_staging GROUP BY data_date);
  -- Find the date inserted values: 
  date_inserted_list := ARRAY(SELECT max(switch_on_date)+1 FROM tmp.crm_staging GROUP BY source_file);

  -- data.crm -- 

  -- Read compresslevel and cleanup status: 
  SELECT
    compresslevel,
    cleanup
  FROM core.partition_date_tables
  WHERE table_name = 'data.crm'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT s.data_date
    FROM (SELECT unnest(datelist) AS data_date) AS s
    LEFT JOIN (SELECT data_date FROM core.partition_date_create_times WHERE table_name = 'data.crm') AS p
    ON s.data_date = p.data_date
    WHERE p.data_date IS NULL -- make sure we do not try to create partitions that already exist
    AND s.data_date IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_date_create_times (table_name, data_date, time_created, cleanup)
    VALUES ('data.crm', d.data_date, now(), clup);

    EXECUTE 'ALTER TABLE data.crm '
         || 'ADD PARTITION "' || d.data_date::text || '" '
         || 'START (''' || d.data_date || '''::date) END (''' || (d.data_date + 1) || '''::date) '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

  -- data.in_crm -- 

  -- Read compresslevel and cleanup status: 
  SELECT
    compresslevel,
    cleanup
  FROM core.partition_date_tables
  WHERE table_name = 'data.in_crm'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT 
      s.data_date
    FROM (SELECT unnest(date_inserted_list) AS data_date GROUP BY data_date) AS s
    LEFT JOIN (SELECT data_date FROM core.partition_date_create_times WHERE table_name = 'data.in_crm') AS p
    ON s.data_date = p.data_date
    WHERE p.data_date IS NULL -- make sure we do not try to create partitions that already exist
    AND s.data_date IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_date_create_times (table_name, data_date, time_created, cleanup)
    VALUES ('data.in_crm', d.data_date, now(), clup);

    EXECUTE 'ALTER TABLE data.in_crm '
         || 'ADD PARTITION "' || d.data_date::text || '" '
         || 'START (''' || d.data_date || '''::date) END (''' || (d.data_date + 1) || '''::date) '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.create_crm_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.create_topup_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Creates partitions for topup tables based on the contents of tmp.topup_staging. 
 * The partitioned tables are: 
 * - data.topup
 *
 * VERSION
 * 03.04.2013 HMa
 */
DECLARE

  datelist           date[];

  clevel             integer;
  clup               boolean;
  d                  record;

BEGIN

  -- Find the timestamps that are found in tmp.crm_staging:
  datelist := ARRAY(SELECT topup_timestamp::date AS data_date FROM tmp.topup_staging GROUP BY data_date);

  -- data.topup -- 

  -- Read compresslevel and cleanup status: 
  SELECT
    compresslevel,
    cleanup
  FROM core.partition_date_tables
  WHERE table_name = 'data.topup'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT s.data_date
    FROM (SELECT unnest(datelist) AS data_date) AS s
    LEFT JOIN (SELECT data_date FROM core.partition_date_create_times WHERE table_name = 'data.topup') AS p
    ON s.data_date = p.data_date
    WHERE p.data_date IS NULL -- make sure we do not try to create partitions that already exist
    AND s.data_date IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_date_create_times (table_name, data_date, time_created, cleanup)
    VALUES ('data.topup', d.data_date, now(), clup);

    EXECUTE 'ALTER TABLE data.topup '
         || 'ADD PARTITION "' || d.data_date::text || '" '
         || 'START (''' || d.data_date || '''::date) END (''' || (d.data_date + 1) || '''::date) '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.create_topup_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.create_product_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Creates partitions for product tables based on the contents of tmp.product_staging. 
 * The partitioned tables are: 
 * - data.product
 *
 * VERSION
 * 03.04.2013 HMa
 */
DECLARE

  datelist           date[];

  clevel             integer;
  clup               boolean;
  d                  record;

BEGIN

  -- Find the timestamps that are found in tmp.crm_staging:
  datelist := ARRAY(SELECT date_inserted AS data_date FROM tmp.product_staging GROUP BY data_date);

  -- data.topup -- 

  -- Read compresslevel and cleanup status: 
  SELECT
    compresslevel,
    cleanup
  FROM core.partition_date_tables
  WHERE table_name = 'data.product'
  INTO clevel, clup;

  -- Create partitions: 
  FOR d IN (
    SELECT s.data_date AS data_date
    FROM (SELECT unnest(datelist) AS data_date) AS s
    LEFT JOIN (SELECT data_date FROM core.partition_date_create_times WHERE table_name = 'data.product') AS p
    ON s.data_date = p.data_date
    WHERE p.data_date IS NULL -- make sure we do not try to create partitions that already exist
    AND s.data_date IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_date_create_times (table_name, data_date, time_created, cleanup)
    VALUES ('data.product', d.data_date, now(), clup);

    EXECUTE 'ALTER TABLE data.product '
         || 'ADD PARTITION "' || d.data_date::text || '" '
         || 'START (''' || d.data_date || '''::date) END (''' || (d.data_date + 1) || '''::date) '
         || COALESCE('WITH (appendonly=true, compresslevel=' || clevel || ')', '');
  END LOOP;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.create_product_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.remove_date_partitions(in_table_name text, start_date date, end_date date)
  RETURNS void AS
$BODY$
/* SUMMARY
 * Removes partitions from the given table for the given date range.
 * Only for manual use, does not obey cleanup = FALSE conditions.
 * Find and delete only existing partitions between start_date & end_date. If end_date is NULL - deletes all partitions AFTER start_date.
 *
 * VERSION
 * 02.04.2013 HMa
 * 24.05.2013 Timur   --find and delete only existing partitions between start_date & end_date. If end_date is NULL - deletes all partitions AFTER start_date.
 */
DECLARE
  partition_prop record;

BEGIN

  --find only existing partitions
  FOR partition_prop IN (
    SELECT data_date FROM core.partition_date_create_times
    where table_name = in_table_name
    and data_date >= start_date
    and CASE WHEN end_date is not null THEN data_date <= end_date ELSE true END
  )
  LOOP
    DELETE FROM core.partition_date_create_times
    WHERE table_name = in_table_name
    AND data_date = partition_prop.data_date;

    IF in_table_name = 'data.call_types_weekly' THEN
        FOR i in 0..6 LOOP
            DELETE FROM data.data_quality WHERE data_date = (partition_prop.data_date + i) AND data_source = 'cdr';
        END LOOP;
    ELSE
        DELETE FROM data.data_quality WHERE data_date = partition_prop.data_date AND data_source =
          (SELECT data_source FROM data.data_source_lookup WHERE table_name = in_table_name);
    END IF;

    EXECUTE 'ALTER TABLE ' || in_table_name || ' '
         || 'DROP PARTITION IF EXISTS FOR (''' || partition_prop.data_date || '''::date)';
  END LOOP;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.remove_date_partitions(table_name text, start_date date, end_date date) OWNER TO xsl;


----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.remove_period_partitions(in_table_name text, start_period integer, end_period integer)
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Removes partitions from the given table for the given period range (in format YYYYWW). 
 * Only for manual use, does not obey cleanup = FALSE conditions.
 * Find and delete only existing partitions between start_period & end_period. If end_period is NULL - deletes all partitions AFTER start_period.
 *
 * VERSION
 * 03.04.2013 HMa
 * 24.05.2013 Timur   --find and delete only existing partitions between start_period & end_period. If end_period is NULL - deletes all partitions AFTER start_period.
 */
DECLARE

  partition_prop record;

BEGIN

  FOR partition_prop IN (
    SELECT period  FROM core.partition_period_create_times
    where table_name = in_table_name
    and period >= start_period
    and CASE WHEN end_period is not null THEN period <= end_period ELSE true END

  )
  LOOP
    DELETE FROM core.partition_period_create_times
    WHERE table_name = in_table_name
    AND period = partition_prop.period;

    for i in 0..6 loop
        delete from data.data_quality where data_date = (core.yyyyww_to_date(partition_prop.period) + i) and data_source = 'cdr';
    end loop;

    EXECUTE 'ALTER TABLE ' || in_table_name || ' '
         || 'DROP PARTITION IF EXISTS FOR (' || partition_prop.period || ')';
  END LOOP;


END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.remove_period_partitions(table_name text, start_period integer, end_period integer) OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.cleanup_date_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Removes old partitions from all tables that are partitioned over a date / timestamp column. 
 * The retention times per table are stored in table core.partition_date_tables. 
 * This function removes partitions that are older than the retention time based on the
 * __content of the date field__, not the partition creation time. 
 *
 * Note: only cleanup condition in table core.partition_date_create_times is obeyed, not the
 * one in the core.partition_date_tables. 
 *
 * VERSION
 * 03.04.2013 HMa
 * Modified from core.cleanup_sequence_partitions() (28.02.2013 MOj)
 */
DECLARE
  partition_prop record;
BEGIN
  --find old partitions to delete
  FOR partition_prop IN (
    SELECT a.table_name, a.data_date
    FROM core.partition_date_create_times a
    INNER JOIN core.partition_date_tables b
    ON a.table_name = b.table_name
    WHERE a.data_date <= current_date - b.retention_dates
    AND a.cleanup IS TRUE
  )
  LOOP
    IF partition_prop.table_name = 'data.call_types_weekly' THEN
        FOR i in 0..6 LOOP
            DELETE FROM data.data_quality WHERE data_date = (partition_prop.data_date + i) AND data_source = 'cdr';
        END LOOP;
    ELSE
        DELETE FROM data.data_quality WHERE data_date = partition_prop.data_date AND data_source =
          (SELECT data_source FROM data.data_source_lookup WHERE table_name = partition_prop.table_name);
    END IF;

    DELETE FROM core.partition_date_create_times
    WHERE table_name = partition_prop.table_name
    AND data_date = partition_prop.data_date;

    EXECUTE 'ALTER TABLE ' || partition_prop.table_name || ' '
         || 'DROP PARTITION IF EXISTS FOR (''' || partition_prop.data_date || '''::date)';
  END LOOP;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.cleanup_date_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.cleanup_period_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Removes old partitions from all tables that are partitioned over a period column (format: YYYYWW). 
 * The retention times per table are stored in table core.partition_period_tables. 
 * This function removes partitions that are older than the retention time based on the
 * __content of the period field__, not the partition creation time. 
 *
 * Note: only cleanup condition in table core.partition_period_create_times is obeyed, not the
 * one in the core.partition_period_tables. 
 *
 * VERSION
 * 03.04.2013 HMa
 * Modified from core.cleanup_sequence_partitions() (28.02.2013 MOj)
 */
DECLARE
  partition_prop record;
BEGIN
  --find old partitions to delete
  FOR partition_prop IN (
    SELECT a.table_name, a.period
    FROM core.partition_period_create_times a
    INNER JOIN core.partition_period_tables b
    ON a.table_name = b.table_name
    WHERE core.yyyyww_to_date(a.period) + 6 <= current_date - b.retention_dates -- compare Sunday of the week with current date
    AND a.cleanup IS TRUE
  )
  LOOP
    DELETE FROM core.partition_period_create_times
    WHERE table_name = partition_prop.table_name
    AND period = partition_prop.period;

    FOR i in 0..6 LOOP
        DELETE FROM data.data_quality WHERE data_date = (core.yyyyww_to_date(partition_prop.period) + i) AND data_source = 'cdr';
    END LOOP;

    EXECUTE 'ALTER TABLE ' || partition_prop.table_name || ' '
         || 'DROP PARTITION IF EXISTS FOR (' || partition_prop.period || ')';
  END LOOP;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.cleanup_period_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.cleanup_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * A wrapper function that calls functions core.cleanup_date_partitions() and 
 *
 * VERSION
 * 03.04.2013 HMa
 */
DECLARE

BEGIN

  EXECUTE core.cleanup_date_partitions();
  EXECUTE core.cleanup_period_partitions();

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.cleanup_partitions() OWNER TO xsl;

-------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION core.date_to_yyyyww(in_date date)
  RETURNS integer AS
$BODY$ 

/* This is an assistance function that converts date to yearweek. The ISO 8601
 * definition is used for the first week of a year: it is the week with the
 * year's first Thursday in it. Week begins on Monday and ends on Sunday.
 * 
 * Updates:
 * 24.01.2012 TSi - Bug 821 fixed
 * 07.06.2010 JMa - Bug 103 fixed
 * 19.01.2010 JMa - Bug 103 fixed
 * 18.02.2009 MAk - Bug 138 fixed (old indexing of bugs)
 */

  SELECT to_char($1, 'IYYYIW')::integer

$BODY$
  LANGUAGE 'sql' STABLE;
ALTER FUNCTION core.date_to_yyyyww(date) OWNER TO xsl;


-------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION core.yyyyww_to_date(in_yyyyww integer)
  RETURNS date AS
$BODY$

/* This is an assistance function that converts yearweek to date (Monday). The
 * ISO 8601 definition is used for the first week of a year: it is the week with
 * the year's first Thursday in it.  Week begins on Monday and ends on Sunday.
 * 
 * Updates:
 * 24.01.2012 TSi - Bug 821 fixed
 * 08.07.2010 TSi - Bug 212 fixed
 * 07.06.2010 JMa - Bug 103 fixed
 * 19.01.2010 JMa - Bug 103 fixed
 */

  SELECT to_date($1::text, 'IYYYIW')

$BODY$
  LANGUAGE 'sql' STABLE;
ALTER FUNCTION core.yyyyww_to_date(integer) OWNER TO xsl;


----------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION data.check_operator_own_name(operator_own_name text)
  RETURNS void AS
$BODY$

DECLARE

/* SUMMARY:
 * Checks that the global parameter 'OperatorOwnName' configured in the SL UI has been changed from default. 
 */

BEGIN
 
  IF operator_own_name IS NULL OR operator_own_name = 'configure_this' THEN
    RAISE EXCEPTION 'Change the global parameter ''OperatorOwnName'' in the Social Links UI (Workflow -> Global parameters)! It should correspond to the operator''s own network name appearing in the ''a_network'' and ''b_network'' columns in the input CDR data.';
  END IF;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.check_operator_own_name(operator_own_name text) OWNER TO xsl;



-------------------------------------------------------------------------------


-- The functions needed in the loading of the CDR data

CREATE OR REPLACE FUNCTION data.tmp_cdr_staging(dataset text)
  RETURNS void AS
$BODY$

/* SUMMARY:
 * Insert the data into the tmp.cdr_staging table and transform the types of the fields
 */


DECLARE

BEGIN

  analyze tmp.cdr_err;

  insert into data.failed_rows_stats
  (data_source, source_file, rowcount)
  select 
    'cdr' as data_source,
    aa.source_file as source_file,
    count(*) as rowcount
  from (
    select substring(rawdata from 1 for position(';' in rawdata)-1) as source_file, * from tmp.cdr_err
  ) aa
  left outer join (
    select source_file from data.failed_rows_stats where data_source = 'cdr' group by source_file
  ) bb
  on aa.source_file = bb.source_file
  where bb.source_file is null
  group by aa.source_file;

  analyze data.failed_rows_stats;



  ANALYZE tmp.cdr;

  truncate table tmp.cdr_staging;

  INSERT INTO tmp.cdr_staging
  (source_file, a_number, b_number, a_network, b_network, call_length, call_type, 
   call_time, a_cell_id, b_cell_id, remaining_credits, a_call_cost, termination_reason)
  SELECT 
    case when d_new.source_file is not null and d_new.source_file != '' then trim(d_new.source_file) else NULL end as source_file,
    -- All the rest fields are defined in the data guide. Typically very customer/data spesific in practice.
    case when a_number is not null and a_number != '' then trim(a_number) else NULL end as a_number,
    case when b_number is not null and b_number != '' then trim(b_number) else NULL end as b_number,
    case when a_network is not null and a_network != '' then trim(a_network) else NULL end as a_network,
    case when b_network is not null and b_network != '' then trim(b_network) else NULL end as b_network,
    case when duration is not null and trim(duration) != '' then trim(duration)::double precision else null end as call_length,
    case when type is not null and trim(type) != '' then trim(type)::integer else null end as call_type,
    case when call_time is not null and trim(call_time) != '' then to_timestamp(trim(call_time), 'YYYY-MM-DD HH24:MI:SS')::timestamp without time zone else null end as call_time,
    case when a_cell_id is not null and trim(a_cell_id) != '' then (trim(a_cell_id)::numeric)::integer else null end as a_cell_id,  
    case when b_cell_id is not null and trim(b_cell_id) != '' then (trim(b_cell_id)::numeric)::integer else null end as b_cell_id,
    case when remaining_credits is not null and trim(remaining_credits) != '' then trim(remaining_credits)::double precision else null end as remaining_credits,
    case when a_call_cost is not null and trim(a_call_cost) != '' then trim(a_call_cost)::double precision else null end as a_call_cost,
    case when termination_reason is not null and termination_reason != '' then trim(termination_reason) else NULL end as termination_reason
  FROM (
    select * from tmp.cdr 
    where (case when lower(trim(a_number)) = 'a_number' then 1 else 0 end + 
           case when lower(trim(b_number)) = 'b_number' then 1 else 0 end) < 2
  ) as d_new
  LEFT JOIN (                  -- Make sure that given data file is not yet processed
    SELECT pf.source_file
    FROM data.processed_files AS pf
    WHERE pf.source_file LIKE '%cdr%'
  ) AS d_old
  ON d_new.source_file = d_old.source_file
  WHERE d_old.source_file IS NULL;
  
  ANALYZE tmp.cdr_staging;

  INSERT INTO data.processed_files 
  (source_file, max_timestamp, rowcount, parameter1, parameter2, dataset_id)
  SELECT 
    case 
      when source_file is not null and source_file != '' then trim(source_file) else NULL 
    end as source_file,
    max(call_time) as max_timestamp,
    count(*) as rowcount,
    count(distinct a_number) as parameter1,
    sum(case when call_type = 1 then call_length else 0 end)::real /
      sum(case when call_type = 1 then 1 else null end)::real as parameter2,
	dataset as dataset_id
  FROM tmp.cdr_staging GROUP BY 1;

  ANALYZE data.processed_files;
  
  INSERT INTO data.processed_data 
  (dataset_id, data_type, data_date, rowcount, parameter1, parameter2)
  SELECT 
    dataset as dataset_id,
    'cdr' as data_type,
    call_time::date as data_date,
    count(*) as rowcount,
    count(distinct a_number) as parameter1,
    sum(case when call_type = 1 then call_length else 0 end)::real /
      sum(case when call_type = 1 then 1 else null end)::real as parameter2
  FROM tmp.cdr_staging GROUP BY call_time::date;

  ANALYZE data.processed_data;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.tmp_cdr_staging(dataset text) OWNER TO xsl;

CREATE OR REPLACE FUNCTION data.network_list_cdr()
  RETURNS void AS
$BODY$

DECLARE

BEGIN
 
  INSERT INTO aliases.network_list (net_name)
  SELECT d_new.net_name
  FROM (
    SELECT d1.a_network AS net_name FROM tmp.cdr_staging AS d1 GROUP BY 1 UNION
    SELECT d2.b_network AS net_name FROM tmp.cdr_staging AS d2 GROUP BY 1
  ) AS d_new
  LEFT JOIN aliases.network_list AS d_old
  ON d_new.net_name = d_old.net_name
  WHERE d_old.net_name IS NULL;

  ANALYZE aliases.network_list;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.network_list_cdr() OWNER TO xsl;



CREATE OR REPLACE FUNCTION data.aliases_string_id_cdr()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO aliases.string_id 
  (string_id, date_inserted)
  SELECT 
    d_new.string_id, 
    CURRENT_DATE AS date_inserted
  FROM (
    SELECT d1.a_number AS string_id FROM tmp.cdr_staging AS d1 WHERE a_number IS NOT NULL GROUP BY 1 UNION
    SELECT d2.b_number AS string_id FROM tmp.cdr_staging AS d2 WHERE b_number IS NOT NULL GROUP BY 1
  ) AS d_new
  LEFT JOIN aliases.string_id AS d_old
  ON d_new.string_id = d_old.string_id
  WHERE d_old.string_id IS NULL;

  ANALYZE aliases.string_id;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.aliases_string_id_cdr() OWNER TO xsl;

CREATE OR REPLACE FUNCTION data.cdr()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO data.cdr
  (alias_a, alias_b, a_network, b_network, call_length, call_type, 
   call_time, a_cell_id, b_cell_id, remaining_credits, a_call_cost, termination_reason)
  SELECT
    b.alias_id AS alias_a,
    c.alias_id AS alias_b,
    d.net_id as a_network,
    e.net_id as b_network,
    a.call_length,
    a.call_type,         -- Make sure that voice calls = 1, sms = 2, video=3, mms=4, and data=5
    a.call_time,
    a_cell_id,
    b_cell_id,
    remaining_credits,
    a_call_cost,
    termination_reason
  FROM tmp.cdr_staging AS a
  INNER JOIN aliases.string_id AS b
  ON a.a_number = b.string_id
  INNER JOIN aliases.string_id AS c
  ON a.b_number = c.string_id
  INNER JOIN aliases.network_list AS d
  ON a.a_network = d.net_name 
  INNER JOIN aliases.network_list AS e
  ON a.b_network = e.net_name
  WHERE a.call_time IS NOT NULL;

  ANALYZE data.cdr;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.cdr() OWNER TO xsl;


-- 2013-03-08 HMa - ICIF-116 fixed
-- 2012-12-13 HMa - ICIF-87 fixed
CREATE OR REPLACE FUNCTION data.cdr_full_weeks()
  RETURNS void AS
$BODY$

DECLARE

  BEGIN

  TRUNCATE TABLE tmp.cdr_full_weeks;
  INSERT INTO tmp.cdr_full_weeks
  (alias_a, alias_b, a_network, b_network, call_length, call_type, 
   call_time, a_cell_id, b_cell_id, remaining_credits, a_call_cost, termination_reason)
  SELECT 
    a.alias_a,
    a.alias_b,
    a.a_network,
    a.b_network,
    a.call_length,
    a.call_type,
    a.call_time,
    a.a_cell_id,
    a.b_cell_id,
    a.remaining_credits,
    a.a_call_cost,
    a.termination_reason
  FROM data.cdr AS a, (
    SELECT 
      coalesce(b1.next_monday,b2.first_monday) AS first_date 
    FROM (
      SELECT core.yyyyww_to_date(max(period))+7 AS next_monday FROM data.in_split_aggregates -- Monday of the next week
    ) b1, (
      SELECT    -- Monday of the first full week
        CASE
          WHEN min_date_monday = min_date and min_call_time - min_date_monday::timestamp without time zone < '60 minutes'::interval THEN min_date_monday
          ELSE min_date_monday+7
        END AS first_monday
      FROM (
        SELECT
           min_call_time,
           min_date,
           min_date_monday
        FROM (
          SELECT 
            call_type, 
            min(call_time) AS min_call_time,
            min(call_time)::date AS min_date,
            date_trunc('week', min(call_time)::date)::date AS min_date_monday,
            row_number() over (ORDER BY min(call_time) DESC) AS rank           -- Take the maximum of the minimum values
          FROM data.cdr
          WHERE call_time >= '1900-01-01'::date   -- This quarantees that no data from far away history is not taken into account
          GROUP BY call_type
        ) tmp1
        WHERE rank = 1
      ) AS tmp
    ) b2
  ) b, (
    SELECT
      CASE  -- Next Monday of the last full week of cdr-data
        WHEN max_date = max_date_monday+6 AND (max_date_monday+7)::timestamp without time zone - max_call_time < '60 minutes'::interval THEN max_date_monday+7
        ELSE max_date_monday 
      END AS last_date
    FROM (
      SELECT
        max_call_time,
        max_date,
        max_date_monday      
      FROM (
        SELECT
          call_type, 
          max(call_time) AS max_call_time,
          max(call_time)::date AS max_date,
          date_trunc('week', max(call_time)::date)::date AS max_date_monday,
          row_number() over (ORDER BY max(call_time) asc) AS rank            -- Take the minimum of the maximum values
        FROM data.cdr
        WHERE call_type IN (1, 2, 5)
        GROUP BY call_type
      ) tmp1
      WHERE rank = 1
    ) c1
  ) c
  WHERE a.call_time >= b.first_date and a.call_time < c.last_date
  AND a.alias_a != a.alias_b;

  ANALYZE tmp.cdr_full_weeks;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.cdr_full_weeks() OWNER TO xsl;



CREATE OR REPLACE FUNCTION data.aliases_network_cdr()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO aliases.network
  (alias_id, validity, data_source_id, net_id)
  SELECT
    d_new.alias_id, 
    d_new.validity, 
    1::smallint AS data_source_id, -- 1="cdr", 2="topup", 3="crm"
    d_new.net_id 
  FROM (
    SELECT
      dd.alias_id,
      dd.net_id,
      dd.min_call_time::date AS validity,
      row_number() OVER (
        PARTITION BY 
          dd.monday, 
          dd.alias_id
        ORDER BY 
          dd.call_count DESC, 
          dd.min_call_time ASC, 
          dd.net_id ASC 
      ) AS rank_id
    FROM (
      SELECT
        u.monday,
        u.alias_id,
        u.net_id,
        min(u.timestamp) AS min_call_time,
        count(*) AS call_count
      FROM (
        SELECT u1.alias_a AS alias_id, u1.a_network AS net_id, date_trunc('week', u1.call_time::date) AS monday, u1.call_time AS timestamp FROM tmp.cdr_full_weeks AS u1 UNION ALL
        SELECT u2.alias_b AS alias_id, u2.b_network AS net_id, date_trunc('week', u2.call_time::date) AS monday, u2.call_time AS timestamp FROM tmp.cdr_full_weeks AS u2 
      ) AS u
      WHERE u.alias_id IS NOT NULL
      GROUP BY u.monday, u.alias_id, u.net_id
    ) AS dd
  ) AS d_new
  LEFT JOIN aliases.network AS d_old
  ON  d_new.alias_id = d_old.alias_id
  AND d_new.validity = d_old.validity
  AND              1 = d_old.data_source_id
  AND d_new.net_id   = d_old.net_id
  WHERE d_new.rank_id = 1
  AND d_old.alias_id IS NULL;

  ANALYZE aliases.network;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.aliases_network_cdr() OWNER TO xsl;



CREATE OR REPLACE FUNCTION data.call_types_weekly()
  RETURNS void AS
$BODY$
/*
 * 2013-03-07: ICIF 113
 */
DECLARE

BEGIN

  INSERT INTO data.call_types_weekly
  (alias_id, direction, monday, call_type, max_call_time, row_count, day_count, 
   neigh_count, sum_call_duration, sum_call_cost, cell_id_count,
   sum_parameter1, sum_parameter2, sum_parameter3, sum_parameter4)
  SELECT
    aa.alias_id,
    aa.direction,
    aa.monday,
    aa.call_type,
    aa.max_call_time,
    aa.row_count,
    aa.day_count,
    aa.neigh_count,
    aa.sum_call_duration,
    aa.sum_call_cost,
    aa.cell_id_count,
    aa.sum_parameter1,
    aa.sum_parameter2,
    aa.sum_parameter3,
    aa.sum_parameter4
  FROM (
    SELECT
      a.alias_a                             AS alias_id,
      'm'                                   AS direction, -- made transactions
      date_trunc('week', a.call_time)::date AS monday,
      a.call_type                           AS call_type,
      max(a.call_time)                      AS max_call_time,
      count(*)                              AS row_count,
      count(distinct a.call_time::date)     AS day_count,
      count(distinct alias_b)               AS neigh_count,
      count(distinct a.a_cell_id)           AS cell_id_count,
      sum(a.call_length)                    AS sum_call_duration,
      sum(a.a_call_cost)                    AS sum_call_cost,
      count(distinct b_cell_id)               AS sum_parameter1,  -- These sum_parameter1 are just examples. There could/should be some other aggregates as well.
      avg(case when termination_reason in ('Reason 3') then 1 else 0 end) AS sum_parameter2, -- This is for example a list of a abnormal termination reasons
      avg(call_length)                      AS sum_parameter3,  
      avg(remaining_credits)                AS sum_parameter4  
    FROM tmp.cdr_full_weeks AS a
    GROUP BY alias_id, direction, monday, call_type
  ) aa
  LEFT OUTER JOIN (
    SELECT monday from data.call_types_weekly where direction = 'm' GROUP BY monday
  ) bb
  ON aa.monday = bb.monday
  WHERE bb.monday is null;
  

  ANALYZE data.call_types_weekly;


  INSERT INTO data.call_types_weekly
  (alias_id, direction, monday, call_type, max_call_time, row_count, day_count, 
   neigh_count, sum_call_duration, sum_call_cost, cell_id_count,
   sum_parameter1, sum_parameter2, sum_parameter3, sum_parameter4)
  SELECT
    aa.alias_id, 
    aa.direction, 
    aa.monday, 
    aa.call_type, 
    aa.max_call_time, 
    aa.row_count, 
    aa.day_count, 
    aa.neigh_count,
    aa.sum_call_duration,
    aa.sum_call_cost,
    aa.cell_id_count,
    aa.sum_parameter1,
    aa.sum_parameter2,
    aa.sum_parameter3,
    aa.sum_parameter4
  FROM (
    SELECT
      a.alias_b                             AS alias_id,
      'r'                                   AS direction, -- received transactions
      date_trunc('week', a.call_time)::date AS monday,
      a.call_type                           AS call_type,
      max(a.call_time)                      AS max_call_time,
      count(*)                              AS row_count,
      count(distinct a.call_time::date)     AS day_count,
      count(distinct alias_a)               AS neigh_count,
      count(distinct a.b_cell_id)           AS cell_id_count,
      sum(a.call_length)                    AS sum_call_duration,
      NULL                                  AS sum_call_cost,  -- Call cost not well-defined for received calls
      count(distinct a_cell_id)               AS sum_parameter1,  -- These sum_parameter1 are just examples. There could/should be some other aggregates as well.
      avg(case when termination_reason in ('Reason 3') then 1 else 0 end) AS sum_parameter2, -- This is for example a list of a abnormal termination reasons
      avg(call_length)                      AS sum_parameter3,  
      NULL                                  AS sum_parameter4  -- Does not make sense collect here caller remaining credits since it is someting that receiver is not aware of at all
    FROM tmp.cdr_full_weeks AS a
    WHERE call_type in (1,2,3,4)        -- Take only those call types that have valid receiving party
    GROUP BY alias_id, direction, monday, call_type
  ) aa
  LEFT OUTER JOIN (
    SELECT monday from data.call_types_weekly where direction = 'r' GROUP BY monday
  ) bb
  ON aa.monday = bb.monday
  WHERE bb.monday is null;
  
  ANALYZE data.call_types_weekly;


  insert into data.data_usage_weekly_stats
  (table_name, period, rowcount, dis_alias, mms_percentage, mms_avg,
    mms_day, data_percentage, data_avg, data_day)
  select 
    'call_types_weekly'::text as table_name,
    aa.period,
    count(*) as rowcount,
    count(distinct alias_id) as dis_alias,
    sum(case when call_type = 4 then 1 else 0 end)::real / count(distinct alias_id)*100 as mms_percentage, 
    sum(case when call_type = 4 then row_count else 0 end)::real / nullif(sum(case when call_type = 4 then 1 else 0 end), 0) as mms_avg,
    sum(case when call_type = 4 then day_count else 0 end)::real / nullif(sum(case when call_type = 4 then 1 else 0 end), 0) as mms_day,    
    sum(case when call_type = 5 then 1 else 0 end)::real / count(distinct alias_id)*100 as data_percentage,  
    sum(case when call_type = 5 then row_count else 0 end)::real / nullif(sum(case when call_type = 5 then 1 else 0 end), 0) as data_avg,  
    sum(case when call_type = 5 then day_count else 0 end)::real / nullif(sum(case when call_type = 5 then 1 else 0 end), 0) as data_day
  from (
    select core.date_to_yyyyww(monday) as period, * from data.call_types_weekly
    where direction = 'm' 
  ) aa
  left outer join (
    select period from data.data_usage_weekly_stats where table_name = 'call_types_weekly'::text GROUP BY period
  ) bb 
  on aa.period = bb.period
  where bb.period is null
  group by aa.period;


END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.call_types_weekly() OWNER TO xsl;



CREATE OR REPLACE FUNCTION aliases.update()
  RETURNS void AS
$BODY$

/* SUMMARY:
 * To be added here...
 * HW: 2013-07-23: several lines have been commented out or replace to avoid the use of a
 * correlated subquery, which is not compatible with greenplum version 4.0.x.
 * These lines should be taken into use again when greenplum 
 * on host fabric has been upgraded to version 4.2.x.
 * All commented lines are preceeded by
 *   -- HW: correlated subquery commented out (n)
 * the number n indicates the number of commented out lines
 */

DECLARE
  net_id_default integer := -1;
  blacklist_default integer := 0;
  validity_default date := '1800-01-01'::date;

BEGIN

  TRUNCATE aliases.aliases_updated;

  INSERT INTO aliases.aliases_updated (
    string_id,
    alias_id,
    on_black_list,
    in_out_network,
    net_id,
    prev_net_id,
    validity )
  SELECT DISTINCT
    d.string_id,
    d.alias_id,
    -- HW: correlated subquery commented out (1)
    -- coalesce(d.on_black_list, blacklist_default),
    blacklist_default,
    CASE WHEN lower(coalesce(d.net_description, '')) = 'operator own net' THEN 1 ELSE 0 END AS in_out_network, 
    coalesce(d.net_id, net_id_default),
    coalesce(d.prev_net_id, net_id_default),
    coalesce(d.validity, validity_default) AS validity
  FROM (
    SELECT
      s.string_id,
      s.alias_id,
      -- HW: correlated subquery commented out
      -- b.on_black_list,
      n.net_name,
      n.net_description,
      n.net_id,
      n.prev_net_id,
      n.validity,
      row_number() OVER (
        PARTITION BY s.string_id, n.validity
        -- HW: correlated subquery commented out and replaced (1)
        -- ORDER BY b.validity DESC, b.on_black_list DESC, n.net_id ASC
        ORDER BY n.net_id ASC
      ) AS b_row_id
    FROM aliases.string_id AS s
    LEFT JOIN (
      SELECT
        nn.alias_id,
        nl.net_name,
        nl.net_description,
        nn.net_id,
        nn.prev_net_id,
        nn.validity
        -- HW: correlated subquery commented out
        -- nn.max_start_date
      FROM (
        SELECT  -- Notice: We could prioritize different data sources here.
          nnn.alias_id,
          nnn.validity,
          coalesce(nnn.net_id, net_id_default) AS net_id,
          lag(coalesce(nnn.net_id, net_id_default), 1, net_id_default) OVER (
            PARTITION BY nnn.alias_id 
            ORDER BY nnn.validity ASC, nnn.data_source_id DESC
          ) AS prev_net_id,
          row_number() OVER (
            PARTITION BY nnn.alias_id 
            ORDER BY nnn.validity ASC, nnn.data_source_id DESC
          ) AS n_row_id
          -- HW: correlated subquery commented out
          -- greenplum on host fabric has been updated to version 4.2.x
          -- (SELECT  coalesce(MAX(validity), '1800-01-01'::date) as max_start_date
          --  FROM aliases.blacklist
          --  WHERE coalesce(nnn.validity, '2100-01-01'::date) >= coalesce(validity, '1800-01-01'::date)) AS
          -- max_start_date
        FROM aliases.network AS nnn
      ) AS nn
      LEFT JOIN aliases.network_list AS nl
      ON nn.net_id = nl.net_id
      WHERE nn.n_row_id = 1
      OR nn.prev_net_id != nn.net_id
    ) AS n
    ON s.alias_id = n.alias_id
    -- HW: correlated subquery commented out
    -- LEFT JOIN aliases.blacklist AS b 
    -- ON s.alias_id = b.alias_id
    -- AND coalesce(b.validity, '1800-01-01'::date) = n.max_start_date
  ) AS d 
  WHERE d.b_row_id = 1; -- FIX ME: Black list information can only update when network information updates
  
  ANALYZE aliases.aliases_updated;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION aliases.update() OWNER TO xsl;


CREATE OR REPLACE FUNCTION data.in_split_weekly()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO data.in_split_weekly
  (alias_a, alias_b, cdr_w, sms_w, mms_w, video_w, v_c, v_s, v_c_week, v_s_week, v_c_day, v_s_day, v_c_eve, v_s_eve, sms_c_week, sms_c_day, sms_c_eve, data_usage_day, data_usage_eve,data_usage_weekday, weekly_data_usage,weekly_data_usage_cost, weekly_cost, period)
  SELECT
    aa.alias_a, 
    aa.alias_b, 
    aa.cdr_w, 
    aa.sms_w, 
    aa.mms_w, 
    aa.video_w, 
	aa.v_c,  
    aa.v_s, 
    aa.v_c_week, 
    aa.v_s_week, 
    aa.v_c_day, 
    aa.v_s_day, 
    aa.v_c_eve,  
    aa.v_s_eve, 
    aa.sms_c_week, 
    aa.sms_c_day, 
    aa.sms_c_eve, 
	aa.data_usage_day,	
	aa.data_usage_eve,
	aa.data_usage_weekday,
	aa.weekly_data_usage,
	aa.weekly_data_usage_cost,
	aa.weekly_cost,
    aa.period
  FROM (
    select
      aa.alias_a,
      aa.alias_b,
      sum(case when call_type = 1 then 0.4*sqrt(call_length) else 0 end) as cdr_w,
      sum(case when call_type = 2 then 6.0 else 0 end) as sms_w,
      sum(case when call_type = 4 then 12.0 else 0 end) as mms_w,
      sum(case when call_type = 3 then 0.8*sqrt(call_length) else 0 end) as video_w,
	  sum(case when call_type = 1 then 1 else 0 end) as v_c,
      sum(case when call_type = 1 then call_length else 0 end) as v_s,
      sum(case 
            when call_type = 1 and 
            call_time >= date_trunc('week', call_time)::timestamp without time zone and 
            call_time < (date_trunc('week', call_time)::date+4 + '18 hours'::interval)::timestamp without time zone 
            then 1 else 0
      end) as v_c_week,
      sum(
        case 
          when call_type = 1 and 
          call_time >= date_trunc('week', call_time)::timestamp without time zone and 
          call_time < (date_trunc('week', call_time)::date+4 + '18 hours'::interval)::timestamp without time zone 
          then call_length else 0
      end) as v_s_week,
      sum(
        case 
          when call_type = 1 and 
          call_time >= (date_trunc('day', call_time)::date + '7 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone 
        then 1 else 0
      end) as v_c_day,
      sum(
        case 
          when call_type = 1 and 
          call_time >= (date_trunc('day', call_time)::date + '7 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone 
        then call_length else 0
      end) as v_s_day,
      sum(
        case 
          when call_type = 1 and 
          call_time >= (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '22 hours'::interval)::timestamp without time zone 
        then 1 else 0
      end) as v_c_eve,
      sum(
        case 
          when call_type = 1 and 
          call_time >= (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '22 hours'::interval)::timestamp without time zone 
        then call_length else 0
      end) as v_s_eve,
      sum(
        case 
          when call_type = 2 and 
          call_time >= date_trunc('week', call_time)::timestamp without time zone and 
          call_time < (date_trunc('week', call_time)::date+4 + '18 hours'::interval)::timestamp without time zone 
        then 1 else 0
      end) as sms_c_week,
      sum(
        case 
          when call_type = 2 and 
          call_time >= (date_trunc('day', call_time)::date + '7 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone 
        then 1 else 0
      end) as sms_c_day,
      sum(
        case 
          when call_type = 2 and 
          call_time >= (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '22 hours'::interval)::timestamp without time zone 
        then 1 else 0
      end) as sms_c_eve,
	  sum(
        case 
          when call_type = 5 and 
          call_time >= (date_trunc('day', call_time)::date + '7 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone 
        then call_length else 0
      end) as data_usage_day,
      sum(
        case 
          when call_type = 5 and 
          call_time >= date_trunc('week', call_time)::date::timestamp without time zone and 
          call_time < (date_trunc('week', call_time)::date+4 + '18 hours'::interval)::timestamp without time zone 
        then call_length else 0
      end) as data_usage_weekday,
	  sum(
        case 
          when call_type = 5 and 
          call_time >= (date_trunc('day', call_time)::date + '17 hours'::interval)::timestamp without time zone and 
          call_time < (date_trunc('day', call_time)::date + '22 hours'::interval)::timestamp without time zone 
        then call_length else 0
      end) as data_usage_eve,
	 sum(case when call_type = 5 then call_length else 0 end) as weekly_data_usage,
	 sum(case when call_type = 5 then a_call_cost else 0 end) as weekly_data_usage_cost,
	 sum(a_call_cost) as weekly_cost,
      core.date_to_yyyyww(aa.call_time::date) as period 
    from tmp.cdr_full_weeks aa 
    where call_type in (1,2,3,4,5)
    group by alias_a, alias_b, period
  ) aa
  LEFT OUTER JOIN (
    SELECT period FROM data.in_split_weekly GROUP BY period
  ) bb
  ON aa.period = bb.period
  WHERE bb.period IS NULL;
  
  ANALYZE data.in_split_weekly;


  insert into data.cdr_weekly_statistics
  (table_name, period, rowcount, dis_alias_a, dis_alias_b,
   avg_voice, avg_voice_sum, avg_sms)
  select 
    'in_split_weekly'::text as table_name,
    aa.period,  
    count(*) as rowcount, 
    count(distinct aa.alias_a) as dis_alias_a, 
    count(distinct aa.alias_b) as dis_alias_b,
    avg(v_c) as avg_voice, 
    avg(v_s) as avg_voice_sum, 
    avg(sms_w/6.0) as avg_sms 
  from data.in_split_weekly aa
  left outer join (
    select period from data.cdr_weekly_statistics where table_name = 'in_split_weekly' group by period
  ) bb 
  on aa.period = bb.period 
  where bb.period is null
  group by aa.period;


END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.in_split_weekly() OWNER TO xsl;



CREATE OR REPLACE FUNCTION data.weekly_cdr_aggregates(yyyyww1 integer, yyyyww2 integer)
  RETURNS void AS
$BODY$

/* SUMMARY
 * This function calculates aggregates from data.in_split_weekly table and inserts
 * the results to data.in_split_aggregates table.
 *
 * INPUTS
 * yyyyww1 : The first week to be processed
 * yyyyww2 : The last week to be processed
 *
 * VERSION
 * 2012-05-07 MOj - Bug 886 fixed
 * 2011-10-13 TSi - Bug 742 fixed
 */

DECLARE

  monday1 date := core.yyyyww_to_date(yyyyww1);
  monday2 date := core.yyyyww_to_date(yyyyww2);
  monday_list date[] := (SELECT array(SELECT monday1 + 7 * generate_series(0, (monday2 - monday1) / 7)));

BEGIN

  INSERT INTO data.in_split_aggregates (
    alias_id,
    in_out_network,
    voicecount,
    voicesum,
    voicecountweekday,
    voicesumweekday,
    voicecountday,
    voicesumday,
    voicecountevening,
    voicesumevening,
    smscount,
    smscountweekday,
    smscountday,
    smscountevening,
    mc_alias_count,
    rc_alias_count,
    rc_voicecount,
    rc_smscount,
	data_usage_sumday,
    data_usage_sumevening,
    data_usage_sumweekday,
	weekly_data_usage_sum,
	weekly_data_usage_cost_sum,
	weekly_cost_sum,
    period )
  SELECT 
    aa.alias_id,
    aa.in_out_network,
    sum(aa.voicecount) AS voicecount,
    sum(aa.voicesum) AS voicesum,
    sum(aa.voicecountweekday) AS voicecountweekday,
    sum(aa.voicesumweekday) AS voicesumweekday,
    sum(aa.voicecountday) AS voicecountday,
    sum(aa.voicesumday) AS voicesumday,
    sum(aa.voicecountevening) AS voicecountevening,
    sum(aa.voicesumevening) AS voicesumevening,
    sum(aa.smscount) AS smscount,
    sum(aa.smscountweekday) AS smscountweekday,
    sum(aa.smscountday) AS smscountday,
    sum(aa.smscountevening) AS smscountevening,
    sum(aa.mc_alias_count) AS mc_alias_count,
    sum(aa.rc_alias_count) AS rc_alias_count,
    sum(aa.rc_voicecount) AS rc_voicecount,
    sum(aa.rc_smscount) AS rc_smscount,
	sum(aa.data_usage_sumday) AS data_usage_sumday,
	sum(aa.data_usage_sumevening) AS data_usage_sumevening,
	sum(aa.data_usage_sumweekday) AS data_usage_sumweekday,
	sum(aa.weekly_data_usage_sum) AS weekly_data_usage_sum,
	sum(aa.weekly_data_usage_cost_sum) AS weekly_data_usage_cost_sum,
	sum(aa.weekly_cost_sum) AS weekly_cost_sum,
    aa.period
  FROM (
    SELECT 
      sp1.alias_a AS alias_id,
      al1.in_out_network,
      sum(sp1.v_c) AS voicecount,
      sum(sp1.v_s) AS voicesum,
      sum(sp1.v_c_week) AS voicecountweekday,
      sum(sp1.v_s_week) AS voicesumweekday,
      sum(sp1.v_c_day) AS voicecountday,
      sum(sp1.v_s_day) AS voicesumday,
      sum(sp1.v_c_eve) AS voicecountevening,
      sum(sp1.v_s_eve) AS voicesumevening,
      sum(sp1.sms_w/6) AS smscount,
      sum(sp1.sms_c_week) AS smscountweekday,
      sum(sp1.sms_c_day) AS smscountday,
      sum(sp1.sms_c_eve) AS smscountevening,
      count(DISTINCT sp1.alias_b) AS mc_alias_count,
      0 AS rc_alias_count,
      0 AS rc_voicecount,
      0 AS rc_smscount,
	  sum(sp1.data_usage_day) AS data_usage_sumday,
	  sum(sp1.data_usage_eve) AS data_usage_sumevening,
	  sum(sp1.data_usage_weekday) AS data_usage_sumweekday,
	  sum(sp1.weekly_data_usage) AS weekly_data_usage_sum,
	  sum(sp1.weekly_data_usage_cost) AS weekly_data_usage_cost_sum,
	  sum(sp1.weekly_cost) AS weekly_cost_sum,
      sp1.period
    FROM data.in_split_weekly AS sp1
    INNER JOIN (
      SELECT
        au1.alias_id,
        weeklist1.yyyyww,
        au1.in_out_network,
        row_number() OVER (PARTITION BY au1.alias_id, weeklist1.yyyyww ORDER BY au1.validity DESC) AS validity_id 
      FROM aliases.aliases_updated AS au1
      INNER JOIN (
        SELECT DISTINCT    
          ml.monday,
          core.date_to_yyyyww(ml.monday) AS yyyyww
        FROM (
          SELECT unnest(monday_list) AS monday
        ) AS ml
      ) AS weeklist1
      ON au1.validity < weeklist1.monday + 7
    ) AS al1
    ON sp1.alias_b = al1.alias_id
    AND sp1.period = al1.yyyyww
    WHERE al1.validity_id = 1
    GROUP BY sp1.alias_a, sp1.period, al1.in_out_network
    UNION ALL
    SELECT 
      sp2.alias_b AS alias_id,
      al2.in_out_network,
      0 AS voicecount,
      0 AS voicesum,
      0 AS voicecountweekday,
      0 AS voicesumweekday,
      0 AS voicecountday,
      0 AS voicesumday,
      0 AS voicecountevening,
      0 AS voicesumevening,
      0 AS smscount,
      0 AS smscountweekday,
      0 AS smscountday,
      0 AS smscountevening,
      0 AS mc_alias_count,
      count(DISTINCT sp2.alias_a) AS rc_alias_count,
      sum(sp2.v_c) AS rc_voicecount,
      sum(sp2.sms_w/6) AS rc_smscount,
	  0 AS data_usage_sumday,
	  0 AS data_usage_sumevening,
	  0 AS data_usage_sumweekday,
	  0 AS weekly_data_usage_sum,
	  0 AS weekly_data_usage_cost_sum,
	  0 AS weekly_cost_sum,
      sp2.period
    FROM data.in_split_weekly AS sp2
    INNER JOIN (
      SELECT
        au2.alias_id,
        weeklist2.yyyyww,
        au2.in_out_network,
        row_number() OVER (PARTITION BY au2.alias_id, weeklist2.yyyyww ORDER BY au2.validity DESC) AS validity_id 
      FROM aliases.aliases_updated AS au2
      INNER JOIN (
        SELECT DISTINCT    
          ml.monday,
          core.date_to_yyyyww(ml.monday) AS yyyyww
        FROM (
          SELECT unnest(monday_list) AS monday
        ) AS ml
      ) AS weeklist2
      ON au2.validity < weeklist2.monday + 7
    ) AS al2
    ON sp2.alias_a = al2.alias_id
    AND sp2.period = al2.yyyyww
    WHERE al2.validity_id = 1
    GROUP BY sp2.alias_b, sp2.period, al2.in_out_network
  ) AS aa
  GROUP BY aa.alias_id, aa.period, aa.in_out_network;

  ANALYZE data.in_split_aggregates;


  insert into data.cdr_weekly_statistics
  (table_name, period, rowcount, dis_alias_a, dis_alias_b,
   avg_voice, avg_voice_sum, avg_sms)
  select 
    'in_split_aggregates'::text as table_name,
    aa.period,  
    count(*) as rowcount, 
    count(distinct aa.alias_id) as dis_alias_id, 
    0 as dis_alias_b,
    avg(voicecount) as avg_voice, 
    avg(voicesum) as avg_voice_sum, 
    avg(smscount) as avg_sms
  from data.in_split_aggregates aa
  left outer join (
    select period from data.cdr_weekly_statistics where table_name = 'in_split_aggregates' group by period
  ) bb
  on aa.period = bb.period
  where bb.period is null
  group by aa.period;


END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION data.weekly_cdr_aggregates(integer, integer) OWNER TO xsl;



CREATE OR REPLACE FUNCTION data.weekly_cdr_aggregates_wrapper()
  RETURNS void AS
$BODY$
/* SUMMARY
 * This function is a wrapper for the function data.weekly_cdr_aggregates(text1, text2)
 * which was originally called in the loading workflow. The arguments text1 and text2
 * in the loading worklflow where the 2 select statements below (for firstww and lastww).
 * Unfortunately the call to that function with the 2 select statements as
 * arguments failed with 
 *     RuntimeException: org.postgresql.util.PSQLException:
 *     ERROR: Unexpected internal error: Segment process received signal SIGSEGV (postgres.c:3360)
 *     (seg0 slice6 hrh5st26.comptel.com:40000 pid=32009) (cdbdisp.c:1457);
 *     Cause: org.postgresql.util.PSQLException:
 *     ERROR: Unexpected internal error: 
 *     Segment process received signal SIGSEGV (postgres.c:3360) 
 *     (seg0 slice6 hrh5st26.comptel.com:40000 pid=32009) (cdbdisp.c:1457)
 * 
 * Using the result of the 2 select statements as arguments to the function was 
 * a work around for the problem.
 * 
 * So we call in the workflow now data.weekly_cdr_aggregates_wrapper()
 * 
 * INPUTS
 * none
 *
 * VERSION
 * 2012-11-13 HWe
 */

DECLARE

  firstww integer := NULL;
  lastww  integer := NULL;

BEGIN
  SELECT INTO firstww yyyyww1 FROM 
  (
       	select
	min(aa.period) as yyyyww1
	from (
		select period from data.in_split_weekly group by period
	) as aa
	left join (
	select period from data.in_split_aggregates group by period
	) as bb
	on aa.period = bb.period
	where bb.period is null
  ) as ttt; 

  SELECT INTO lastww yyyyww2 FROM
  (
	select
	max(cc.period) as yyyyww2
	from (
		select period from data.in_split_weekly group by period
	) as cc
	left join (
		select period from data.in_split_aggregates group by period
	) as dd
	on cc.period = dd.period
	where dd.period is null
  ) as ttt;

  perform data.weekly_cdr_aggregates(firstww, lastww);

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION data.weekly_cdr_aggregates_wrapper() OWNER TO xsl;


--------------------------------------------------------------------------------


-- The functions needed in the loading of the CRM/SUBSCRIBER data
--DROP FUNCTION data.tmp_crm_staging();
CREATE OR REPLACE FUNCTION data.tmp_crm_staging(dataset text)
  RETURNS void AS
$BODY$

DECLARE

BEGIN


  analyze tmp.crm_err;

  insert into data.failed_rows_stats
  (data_source, source_file, rowcount)
  select 
    'crm' as data_source,
    aa.source_file as source_file,
    count(*) as rowcount
  from (
    select substring(rawdata from 1 for position(';' in rawdata)-1) as source_file, * from tmp.crm_err
  ) aa
  left outer join (
    select source_file from data.failed_rows_stats where data_source = 'crm' group by source_file
  ) bb
  on aa.source_file = bb.source_file
  where bb.source_file is null
  group by aa.source_file;

  analyze data.failed_rows_stats;


  ANALYZE tmp.crm;

  truncate tmp.crm_staging;
  INSERT INTO tmp.crm_staging
  (source_file, csp_internal_subs_id, string_id, subscription_type, 
   payment_type, gender, birth_date, switch_on_date, switch_off_date,
   churn_notification_date, binding_contract, binding_contract_start_date,
   binding_contract_length, subscriber_value, subscriber_segment, 
   handset_model, handset_begin_date, tariff_plan, zip, country, language,
   churn_score, "timestamp")
  SELECT
    d_new.source_file_edit as source_file,           -- Name of the flat file that uploaded to the table
    -- All the rest fields are defined in the data guide.
    case when csp_internal_subs_id is not null and trim(csp_internal_subs_id) != '' then trim(csp_internal_subs_id) else null end as csp_internal_subs_id,
    case when msisdn is not null and trim(msisdn) != '' then trim(msisdn) else null end as string_id,
    case when subscription_type is not null and trim(subscription_type) != '' then trim(subscription_type) else null end as subscription_type,
    case when payment_type is not null and trim(payment_type) != '' then trim(payment_type) else null end as payment_type,
    case when gender is not null and trim(gender) != '' then trim(gender) else null end as gender,
    case when birth_date is not null and trim(birth_date) != '' then to_date(trim(birth_date), 'YYYY-MM-DD') else null end as birth_date,
    case when switch_on_date is not null and trim(switch_on_date) != '' then to_date(trim(switch_on_date), 'YYYY-MM-DD') else null end as switch_on_date,
    case when switch_off_date is not null and trim(switch_off_date) != '' then to_date(trim(switch_off_date), 'YYYY-MM-DD') else null end as switch_off_date,
    case when churn_notification_date is not null and trim(churn_notification_date) != '' then to_date(trim(churn_notification_date), 'YYYY-MM-DD') else null end as churn_notification_date,
    case when binding_contract is not null and trim(binding_contract) != '' then trim(binding_contract) else null end as binding_contract,
    case when binding_contract_start_date is not null and trim(binding_contract_start_date) != '' then to_date(trim(binding_contract_start_date), 'YYYY-MM-DD') else null end as binding_contract_start_date,
    case when binding_contract_length is not null and trim(binding_contract_length) != '' then trim(binding_contract_length)::integer else null end as binding_contract_length,
    case when subscriber_value is not null and trim(subscriber_value) != '' then trim(subscriber_value)::double precision else null end as subscriber_value,
    case when subscriber_segment is not null and trim(subscriber_segment) != '' then trim(subscriber_segment) else null end as subscriber_segment,
    case when handset_model is not null and trim(handset_model) != '' then trim(handset_model) else null end as handset_model,
    case when handset_begin_date is not null and trim(handset_begin_date) != '' then to_date(trim(handset_begin_date), 'YYYY-MM-DD') else null end as handset_begin_date,
    case when tariff_plan is not null and trim(tariff_plan) != '' then trim(tariff_plan) else null end as tariff_plan,
    case when zip is not null and trim(zip) != '' then trim(zip) else null end as zip,
    case when country is not null and trim(country) != '' then trim(country) else null end as country,
    case when language is not null and trim(language) != '' then trim(language) else null end as language,
    case when churn_score is not null and trim(churn_score) != '' then trim(churn_score)::double precision else null end as churn_score,
    case when "timestamp" is not null and trim("timestamp") != '' then to_date(trim("timestamp"), 'YYYY-MM-DD') else null end as "timestamp"
  from (
    select 
      *,
      case when source_file is not null and source_file != '' then trim(source_file) else NULL end as source_file_edit  
    from tmp.crm 
    where msisdn is not null
    and lower(trim(msisdn)) != 'msisdn'
  ) d_new 
  LEFT JOIN (                 -- Make sure that given data file is not yet processed
    SELECT pf.source_file
    FROM data.processed_files AS pf
    WHERE pf.source_file LIKE '%crm%'
  ) AS d_old
  ON d_new.source_file_edit = d_old.source_file
  WHERE d_old.source_file IS NULL;

  ANALYZE tmp.crm_staging;

  INSERT INTO data.processed_files 
  (source_file, max_timestamp, rowcount, parameter1, parameter2, dataset_id)
  SELECT 
    case 
      when source_file is not null and source_file != '' then trim(source_file) else NULL 
    end as source_file,
    max(switch_on_date) as max_timestamp,
    count(*) as rowcount,
    count(distinct string_id) as parameter1,  
    sum(case when payment_type = 'prepaid' and switch_off_date is null then 1 else 0 end)::real /
      sum(case when switch_off_date is null then 1 else null end)::real*100 as parameter2,
	dataset as dataset_id
  FROM tmp.crm_staging GROUP BY 1;


  ANALYZE data.processed_files;
  
  INSERT INTO data.processed_data 
  (dataset_id, data_type, data_date, rowcount, parameter1, parameter2)
  SELECT 
    dataset as dataset_id,
    'crm' as data_type,
    "timestamp"::date as data_date,
	count(*) as rowcount,
    count(distinct string_id) as parameter1,  
    sum(case when payment_type = 'prepaid' and switch_off_date is null then 1 else 0 end)::real /
    sum(case when switch_off_date is null then 1 else null end)::real*100 as parameter2
  FROM tmp.crm_staging GROUP BY "timestamp"::date;

  ANALYZE data.processed_data;
END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.tmp_crm_staging(dataset text) OWNER TO xsl;


CREATE OR REPLACE FUNCTION data.network_list(operator_own_name text)
  RETURNS void AS
$BODY$

DECLARE

  /* SUMMARY:
  All the subscriber in the CRM are Operators own subscribers (at least have been once)
  */

BEGIN
 
  IF operator_own_name = 'configure_this' THEN
    RAISE EXCEPTION 'Provide the operator''s own name found in the CDR data as global parameter ''OperatorOwnName''!';
  END IF;

  INSERT INTO aliases.network_list 
  (net_name, net_description)
  SELECT 
    new_list.net_name,
    new_list.net_description
  FROM (
    SELECT 
      operator_own_name AS net_name,
      'operator own net'::text AS net_description   -- Do not change this!
  ) AS new_list
  LEFT JOIN aliases.network_list AS old_list
  ON new_list.net_name = old_list.net_name
  WHERE old_list.net_name IS NULL;

  ANALYZE aliases.network_list;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.network_list(operator_own_name text) OWNER TO xsl;


CREATE OR REPLACE FUNCTION data.aliases_string_id_crm()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO aliases.string_id 
  (string_id, date_inserted)
  SELECT 
    d_new.string_id, CURRENT_DATE AS date_inserted
  FROM (
    SELECT d.string_id FROM tmp.crm_staging AS d where string_id IS NOT NULL GROUP BY string_id
  ) AS d_new
  LEFT JOIN aliases.string_id AS d_old
  ON d_new.string_id = d_old.string_id
  WHERE d_old.string_id IS NULL;

  ANALYZE aliases.string_id;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.aliases_string_id_crm() OWNER TO xsl;


CREATE OR REPLACE FUNCTION data.aliases_network_crm()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  -- First, insert the dates when subscriptions start
  INSERT INTO aliases.network 
  (alias_id, validity, data_source_id, net_id)
  SELECT
    a_new.alias_id, 
    d_new.validity, 
    3::smallint AS data_source_id, -- 1="cdr", 2="topup", 3="crm"
    n_new.net_id AS net_id
  FROM (
    SELECT
      a.string_id,
      coalesce(a.switch_on_date, '1900-01-01'::date) AS validity   -- switch_on_date refers to activation date of subscription
    FROM tmp.crm_staging AS a
    WHERE string_id IS NOT NULL 
    GROUP BY a.string_id, validity
  ) AS d_new
  INNER JOIN aliases.string_id AS a_new
  ON d_new.string_id = a_new.string_id
  CROSS JOIN (
    SELECT coalesce(nl.net_id, -1) AS net_id
    FROM aliases.network_list AS nl
    WHERE lower(nl.net_description) LIKE 'operator own net'        -- these are operator's active subscribers
    ORDER BY nl.net_id ASC LIMIT 1
  ) AS n_new
  LEFT JOIN aliases.network AS d_old
  ON  a_new.alias_id = d_old.alias_id
  AND d_new.validity = d_old.validity
  AND              3 = d_old.data_source_id
  AND n_new.net_id   = d_old.net_id
  WHERE d_old.alias_id IS NULL;

  ANALYZE aliases.network;


  -- Second, insert the dates when subscriptions ends
  INSERT INTO aliases.network 
  (alias_id, validity, data_source_id, net_id)
  SELECT
    a_new.alias_id, 
    d_new.validity, 
    3::smallint AS data_source_id, -- 1="cdr", 2="topup", 3="crm"
    -1 AS net_id                   -- network id is not known
  FROM (
    SELECT
      a.string_id,
      coalesce(a.switch_off_date, '1800-01-01'::date) AS validity   -- switch_off_date refers to de-activation date of subscription
    FROM tmp.crm_staging AS a
    WHERE string_id IS NOT NULL 
    AND switch_off_date IS NOT NULL
    GROUP BY a.string_id, validity
  ) AS d_new
  INNER JOIN aliases.string_id AS a_new
  ON d_new.string_id = a_new.string_id
  LEFT JOIN aliases.network AS d_old                          -- If in-net validity starts at the same time as it ends, starting date is preferred (switch on and switch off dates are equeal)
  ON  a_new.alias_id = d_old.alias_id
  AND d_new.validity = d_old.validity
  AND              3 = d_old.data_source_id
  AND             -1 = d_old.net_id
  WHERE d_old.alias_id IS NULL;

  ANALYZE aliases.network;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.aliases_network_crm() OWNER TO xsl;



CREATE OR REPLACE FUNCTION data.crm()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO data.crm
  (source_file, alias_id, csp_internal_subs_id, subscription_type,
   payment_type, gender, birth_date, switch_on_date, switch_off_date,
   churn_notification_date, binding_contract, binding_contract_start_date,
   binding_contract_length, subscriber_value, subscriber_segment,
   handset_model, handset_begin_date, tariff_plan, zip, country,
   language, churn_score, "timestamp")
  SELECT
    a.source_file,
    b.alias_id,   -- This is alias for msisdn
    -- All the rest fields are defined in the data guide.
    a.csp_internal_subs_id,
    a.subscription_type,
    a.payment_type,
    a.gender,
    a.birth_date,
    a.switch_on_date,
    a.switch_off_date,
    a.churn_notification_date,
    a.binding_contract,
    a.binding_contract_start_date,
    a.binding_contract_length,
    a.subscriber_value,
    a.subscriber_segment,
    a.handset_model,
    a.handset_begin_date,
    a.tariff_plan,
    a.zip,
    a.country,
    a.language,
    a.churn_score,
    a."timestamp"
  FROM tmp.crm_staging AS a
  INNER JOIN aliases.string_id AS b
  ON a.string_id = b.string_id
  WHERE a."timestamp" IS NOT NULL;

  ANALYZE data.crm;

  -- HW:
  -- Note: The next INSERT should fail when a duplicate value for
  -- date_inserted is created. Therefore there is no
  -- LEFT JOIN data.crm_snapshot_date_inserted_mapping as d_old
  -- ON d_new.date_inserted = d_old.date_inserted
  -- WHERE d_old.date_inserted IS NULL;
  -- It should fail also for duplicate entries of snapshot_date which
  -- the statement currently does not prevent.
  -- But that case should not happen because the the snapshot date is
  -- currently taken from the crm input file name and the loading
  -- workflow does not allow to import 2 files with the having the same date

  INSERT INTO data.crm_snapshot_date_inserted_mapping
  (date_inserted, snapshot_date)
  SELECT d_new.date_inserted, d_new.snapshot_date from
  (
    SELECT * FROM
    (
      SELECT source_file, "timestamp" as snapshot_date FROM tmp.crm_staging GROUP BY source_file, "timestamp"
    ) as a1
    INNER JOIN ( -- Take only the latest data from the data.crm table and Define date_inserted date for each new crm data batch
        select source_file, max(switch_on_date)+1 as date_inserted from tmp.crm_staging group by source_file
      ) a2
      on a1.source_file = a2.source_file
  ) as d_new;
   

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.crm() OWNER TO xsl;




CREATE OR REPLACE FUNCTION data.in_crm()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  insert into data.in_crm
  (alias_id, csp_internal_subs_id, subscription_type, payment_type,
   gender, birth_date, switch_on_date, switch_off_date, churn_notification_date,
   binding_contract, binding_contract_start_date, binding_contract_length,
   subscriber_value, subscriber_segment, handset_model, handset_begin_date,
   tariff_plan, zip, country, language, churn_score, "timestamp", date_inserted) 
  select
    aa.alias_id,
    aa.csp_internal_subs_id,
    aa.subscription_type,
    aa.payment_type,
    aa.gender,
    aa.birth_date,
    aa.switch_on_date,
    aa.switch_off_date,
    aa.churn_notification_date,
    aa.binding_contract,
    aa.binding_contract_start_date,
    aa.binding_contract_length,
    aa.subscriber_value,
    aa.subscriber_segment,
    aa.handset_model,
    aa.handset_begin_date,
    aa.tariff_plan,
    aa.zip,
    aa.country,
    aa.language,
    aa.churn_score,
    aa."timestamp",
    aa.date_inserted
  from (
    select 
      b.*,
      row_number() over (
        partition by b.date_inserted, b.alias_id
        order by coalesce(b.switch_on_date,'1900-01-01'::date) desc, 
                 coalesce(b.subscriber_value,-10000) desc,
                 coalesce(b.binding_contract_start_date + (b.binding_contract_length*(365.0/12.0))::integer,'1900-01-01'::date) desc 
      ) as rank   -- This is just an example ordering, edit according to the available data, this quarantees that only one row for each msisdn is found from data.in_crm table
    from (
      select 
        a1.*,
        a2.date_inserted
      from data.crm a1
      inner join (                      -- Take only the latest data from the data.crm table and Define date_inserted date for each new crm data batch
        select source_file, max(switch_on_date)+1 as date_inserted from tmp.crm_staging group by source_file
      ) a2
      on a1.source_file = a2.source_file
    ) b
  ) aa
  left outer join data.in_crm bb
  on aa.date_inserted = bb.date_inserted  
  where aa.rank = 1
  and bb.date_inserted is null
  AND aa.date_inserted IS NOT NULL;

  ANALYZE data.in_crm;


  insert into data.crm_statistics
  (date_inserted, active_distinct_alias, prepaid_percentage, avg_tenure_months, new_subs_percentage)
  select 
    a.date_inserted, 
    count(distinct a.alias_id) as active_distinct_alias,
    sum(case when a.payment_type = 'prepaid' then 1 else 0 end)::real /
      count(*)::real*100.0 as prepaid_percentage,
    avg(a.date_inserted - a.switch_on_date)/30.0 as avg_tenure_months,
    avg(case when (a.date_inserted - a.switch_on_date) <= 30.0 then 1 else 0 end)*100.0 as new_subs_percentage
  from (
    select * from data.in_crm where switch_off_date is null
  ) a
  left outer join (
    select date_inserted from data.crm_statistics group by date_inserted
  ) b
  on a.date_inserted = b.date_inserted
  where b.date_inserted is null
  group by a.date_inserted;

  analyze data.crm_statistics;


END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.in_crm() OWNER TO xsl;


--------------------------------------------------------------------------------

-- The functions needed in the loading of the TOPUP/RECHARGE data
-- DROP FUNCTION data.tmp_topup_staging();
CREATE OR REPLACE FUNCTION data.tmp_topup_staging(dataset text)
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  analyze tmp.topup_err;

  insert into data.failed_rows_stats
  (data_source, source_file, rowcount)
  select 
    'topup' as data_source,
    aa.source_file as source_file,
    count(*) as rowcount
  from (
    select substring(rawdata from 1 for position(';' in rawdata)-1) as source_file, * from tmp.topup_err
  ) aa
  left outer join (
    select source_file from data.failed_rows_stats where data_source = 'topup' group by source_file
  ) bb
  on aa.source_file = bb.source_file
  where bb.source_file is null
  group by aa.source_file;

  analyze data.failed_rows_stats;


  ANALYZE tmp.topup;

  truncate table tmp.topup_staging;
  INSERT INTO tmp.topup_staging
  (source_file, charged_id, receiving_id, credit_amount, topup_cost, 
   is_credit_transfer, topup_channel,  credit_balance, topup_timestamp)
  select
    case when d_new.source_file is not null and d_new.source_file != '' then trim(d_new.source_file) else NULL end as source_file,
    case when charged_id is not null and charged_id != '' then trim(charged_id) else NULL end as charged_id,
    case when receiving_id is not null and receiving_id != '' then trim(receiving_id) else NULL end as receiving_id,
    case when credit_amount is not null and trim(credit_amount) != '' then trim(credit_amount)::double precision else null end as credit_amount,
    case when topup_cost is not null and trim(topup_cost) != '' then trim(topup_cost)::double precision else null end as topup_cost,
    case when is_credit_transfer is not null and trim(is_credit_transfer) != '' then trim(is_credit_transfer)::integer else NULL end as is_credit_transfer,
    case when topup_channel is not null and topup_channel != '' then trim(topup_channel) else NULL end as topup_channel,
    case when credit_balance is not null and trim(credit_balance) != '' then trim(credit_balance)::double precision else null end as credit_balance,
    case when topup_timestamp is not null and trim(topup_timestamp) != '' then to_timestamp(trim(topup_timestamp), 'YYYY-MM-DD HH24:MI:SS')::timestamp without time zone else null end as topup_timestamp
  from (
    select * from tmp.topup 
    where (case when lower(trim(charged_id)) = 'charged_id' then 1 else 0 end + 
           case when lower(trim(receiving_id)) = 'receiving_id' then 1 else 0 end) < 2
  ) as d_new
  LEFT JOIN (
    SELECT pf.source_file
    FROM data.processed_files AS pf
    WHERE pf.source_file LIKE '%topup%'
  ) AS d_old
  ON d_new.source_file = d_old.source_file
  WHERE d_old.source_file IS NULL;
  
  ANALYZE tmp.topup_staging;


  INSERT INTO data.processed_files 
  (source_file, max_timestamp, rowcount, parameter1, parameter2, dataset_id)
  SELECT 
    case 
      when source_file is not null and source_file != '' then trim(source_file) else NULL 
    end as source_file,
    max(topup_timestamp) as max_timestamp,
    count(*) as rowcount,
    count(distinct charged_id) as parameter1,
    sum(case when is_credit_transfer = 0 then credit_amount else 0 end)::real / 
      sum(case when is_credit_transfer = 0 then 1 else null end)::real as parameter2,
	dataset as dataset_id
  FROM tmp.topup_staging GROUP BY 1;
  

  ANALYZE data.processed_files;

  INSERT INTO data.processed_data 
  (dataset_id, data_type, data_date, rowcount, parameter1, parameter2)
  SELECT 
    dataset as dataset_id,
    'topup' as data_type,
    topup_timestamp::date as data_date,
    count(*) as rowcount,
    count(distinct charged_id) as parameter1,
    sum(case when is_credit_transfer = 0 then credit_amount else 0 end)::real / 
      sum(case when is_credit_transfer = 0 then 1 else null end)::real as parameter2
  FROM tmp.topup_staging GROUP BY topup_timestamp::date;

  ANALYZE data.processed_data;
END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.tmp_topup_staging(dataset text) OWNER TO xsl;


CREATE OR REPLACE FUNCTION data.aliases_string_id_topup()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO aliases.string_id 
  (string_id, date_inserted)
  SELECT 
    d_new.string_id, 
    CURRENT_DATE AS date_inserted
  FROM (
    SELECT d1.charged_id AS string_id FROM tmp.topup_staging AS d1 WHERE charged_id IS NOT NULL GROUP BY 1 UNION
    SELECT d2.receiving_id AS string_id FROM tmp.topup_staging AS d2 WHERE receiving_id IS NOT NULL GROUP BY 1
  ) AS d_new
  LEFT JOIN aliases.string_id AS d_old
  ON d_new.string_id = d_old.string_id
  WHERE d_old.string_id IS NULL;
 
  ANALYZE aliases.string_id;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.aliases_string_id_topup() OWNER TO xsl;


CREATE OR REPLACE FUNCTION data.aliases_network_topup()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO aliases.network 
  (alias_id, validity, data_source_id, net_id)
  SELECT
    a_new.alias_id, 
    d_new.validity, 
    2::smallint AS data_source_id, -- 1="cdr", 2="topup", 3="crm"
    n_new.net_id AS net_id
  FROM (
    SELECT
      string_id, min(validity) as validity  
    from (
      (select charged_id as string_id, coalesce(topup_timestamp::date, '1900-01-01'::date) AS validity FROM tmp.topup_staging where charged_id is not null)
      union 
      (select receiving_id as string_id , coalesce(topup_timestamp::date, '1900-01-01'::date) AS validity FROM tmp.topup_staging where receiving_id is not null)
    ) a
    GROUP BY string_id     -- It is assumed here that credits can be transfered only inside operator's network. Both charged and receiving are always assumed to be operator's own subs
  ) AS d_new
  INNER JOIN aliases.string_id AS a_new
  ON d_new.string_id = a_new.string_id
  CROSS JOIN (
    SELECT coalesce(nl.net_id, -1) AS net_id
    FROM aliases.network_list AS nl
    WHERE lower(nl.net_description) LIKE 'operator own net'
    ORDER BY nl.net_id ASC LIMIT 1
  ) AS n_new
  LEFT JOIN aliases.network AS d_old
  ON  a_new.alias_id = d_old.alias_id
  AND d_new.validity = d_old.validity
  AND              2 = d_old.data_source_id
  AND n_new.net_id   = d_old.net_id
  WHERE d_old.alias_id IS NULL;
  
  ANALYZE aliases.network;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.aliases_network_topup() OWNER TO xsl;


CREATE OR REPLACE FUNCTION data.topup()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO data.topup 
  (charged_id, receiving_id, credit_amount, topup_cost,
   is_credit_transfer, channel, credit_balance, "timestamp")
  SELECT
    bb.alias_id AS charged_id,
    cc.alias_id AS receiving_id,  
    aa.credit_amount AS credit_amount,
    aa.topup_cost AS topup_cost,
    case
      when aa.is_credit_transfer is not null and aa.is_credit_transfer = 1 then TRUE
      when aa.is_credit_transfer is not null and aa.is_credit_transfer = 0 then FALSE
      else NULL
    end as is_credit_transfer,
    aa.topup_channel as channel,
    aa.credit_balance as credit_balance,
    aa.topup_timestamp as "timestamp"
  FROM tmp.topup_staging aa
  INNER JOIN aliases.string_id bb
  on aa.charged_id = bb.string_id
  INNER JOIN aliases.string_id cc
  on aa.receiving_id = cc.string_id
  WHERE aa.topup_timestamp IS NOT NULL;
  
  ANALYZE data.topup;


  insert into data.topup_statistics
  (type_of_event, period, topup_events, dis_charged,
   dis_receiving, avg_credit_amount, avg_credit_balance)
  select 
    'Not credit transfer'::text as type_of_event,
    aa.period as period,
    count(*) as topup_events,
    count(distinct charged_id) as dis_charged,
    count(distinct receiving_id) as dis_receiving,
    avg(credit_amount) as avg_credit_amount,
    avg(credit_balance) as avg_credit_balance
  from (
    select
      core.date_to_yyyyww(timestamp::date) as period, 
      * 
    from (select * from data.topup where is_credit_transfer = FALSE) a, (
      SELECT
        CASE  -- Next Monday of the last full week of cdr-data
          WHEN max_date = max_date_monday+6 AND (max_date_monday+7)::timestamp without time zone - max_time < '60 minutes'::interval THEN max_date_monday+7
          ELSE max_date_monday 
        END AS last_date
      FROM (
        SELECT
          max(timestamp) AS max_time,
          max(timestamp)::date AS max_date,
          date_trunc('week', max(timestamp)::date)::date AS max_date_monday
        FROM data.topup where is_credit_transfer = FALSE
      ) tmp1
    ) b
    where a.timestamp < b.last_date
  ) aa
  left outer join (
    select period from data.topup_statistics where type_of_event = 'Not credit transfer'::text group by period
  ) bb
  on aa.period = bb.period 
  where bb.period is null
  group by aa.period;

  insert into data.topup_statistics
  (type_of_event, period, topup_events, dis_charged,
   dis_receiving, avg_credit_amount, avg_credit_balance)
  select 
    'Credit transfer'::text as type_of_event,
    aa.period as period,
    count(*) as topup_events,
    count(distinct charged_id) as dis_charged,
    count(distinct receiving_id) as dis_receiving,
    avg(credit_amount) as avg_credit_amount,
    avg(credit_balance) as avg_credit_balance
  from (
    select
      core.date_to_yyyyww(timestamp::date) as period, 
      * 
    from (select * from data.topup where is_credit_transfer = TRUE) a, (
      SELECT
        CASE  -- Next Monday of the last full week of cdr-data
          WHEN max_date = max_date_monday+6 AND (max_date_monday+7)::timestamp without time zone - max_time < '60 minutes'::interval THEN max_date_monday+7
          ELSE max_date_monday 
        END AS last_date
      FROM (
        SELECT
          max(timestamp) AS max_time,
          max(timestamp)::date AS max_date,
          date_trunc('week', max(timestamp)::date)::date AS max_date_monday
        FROM data.topup where is_credit_transfer = TRUE
      ) tmp1
    ) b
    where a.timestamp < b.last_date
  ) aa
  left outer join (
    select period from data.topup_statistics where type_of_event = 'Credit transfer'::text group by period
  ) bb
  on aa.period = bb.period 
  where bb.period is null
  group by aa.period;
  

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.topup() OWNER TO xsl;


--------------------------------------------------------------------------------

-- The functions needed in the loading of the PRODUCT data
-- DROP FUNCTION data.tmp_product_staging();
CREATE OR REPLACE FUNCTION data.tmp_product_staging(dataset text)
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  analyze tmp.product_err;

  insert into data.failed_rows_stats
  (data_source, source_file, rowcount)
  select 
    'product_takeup' as data_source,
    aa.source_file as source_file,
    count(*) as rowcount
  from (
    select substring(rawdata from 1 for position(';' in rawdata)-1) as source_file, * from tmp.product_err
  ) aa
  left outer join (
    select source_file from data.failed_rows_stats where data_source = 'product' group by source_file
  ) bb
  on aa.source_file = bb.source_file
  where bb.source_file is null
  group by aa.source_file;

  analyze data.failed_rows_stats;


  ANALYZE tmp.product;

  TRUNCATE TABLE tmp.product_staging;
  INSERT INTO tmp.product_staging 
  (
    source_file,
    string_id,
    product_id,
    subproduct_id,
    product_possible,
    product_score,
    product_taken_date,
    product_churn_date,
    date_inserted
  )
  SELECT
    d_new.source_file AS source_file, 
    CASE WHEN string_id IS NOT NULL AND TRIM(string_id)!='' THEN TRIM(string_id) ELSE NULL END AS string_id,
    CASE WHEN product_id IS NOT NULL AND TRIM(product_id)!='' THEN TRIM(product_id) ELSE NULL END AS product_id,
    CASE WHEN subproduct_id IS NOT NULL AND TRIM(subproduct_id)!='' THEN TRIM(subproduct_id) ELSE NULL END AS subproduct_id,
    CASE 
      WHEN lower(trim(product_possible)) = 'true' THEN TRUE 
      WHEN lower(trim(product_possible)) = 'false' THEN FALSE
      ELSE NULL
    END AS product_possible, 
    CASE WHEN product_score IS NOT NULL AND trim(product_score) != '' THEN product_score::double precision ELSE NULL END AS product_score, 
    CASE WHEN product_taken_date IS NOT NULL AND trim(product_taken_date) !='' THEN to_date(trim(product_taken_date),'YYYY-MM-DD')::date ELSE NULL END AS product_taken_date,
    CASE WHEN product_churn_date IS NOT NULL AND trim(product_churn_date) !='' THEN to_date(trim(product_churn_date),'YYYY-MM-DD')::date ELSE NULL END AS product_churn_date,
    CASE WHEN date_inserted IS NOT NULL AND trim(date_inserted) !='' THEN to_date(trim(date_inserted),'YYYY-MM-DD')::date ELSE NULL END AS date_inserted
  FROM (
    SELECT 
      *,
      CASE WHEN source_file IS NOT NULL AND source_file != '' THEN TRIM(source_file) ELSE NULL END AS source_file_edit  
    FROM tmp.product WHERE string_id IS NOT NULL 
  ) d_new 
  LEFT JOIN (                 -- Make sure that given data file is not yet processed
    SELECT pf.source_file
    FROM data.processed_files AS pf
    WHERE pf.source_file LIKE '%product_takeup%'
  ) AS d_old
  ON d_new.source_file_edit = d_old.source_file
  WHERE d_old.source_file IS NULL;
    
  ANALYZE tmp.product_staging;


  INSERT INTO data.processed_files 
  (source_file, max_timestamp, rowcount, dataset_id)
  SELECT 
    case when source_file is not null and source_file != '' then trim(source_file) else NULL end as source_file,
    max(date_inserted) AS max_timestamp, 
    count(*) as rowcount,
	dataset as dataset_id
  FROM tmp.product_staging GROUP BY 1;
  
  ANALYZE data.processed_files;

  INSERT INTO data.processed_data 
  (dataset_id, data_type, data_date, rowcount)
  SELECT 
    dataset as dataset_id,
    'product_takeup' as data_type,
    date_inserted as data_date,
    count(*) as rowcount
  FROM tmp.product_staging GROUP BY date_inserted;

  ANALYZE data.processed_data;
  
END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.tmp_product_staging(dataset text) OWNER TO xsl;

CREATE OR REPLACE FUNCTION data.aliases_string_id_product()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO aliases.string_id (
    string_id, 
    date_inserted
  )
  SELECT 
    d_new.string_id, d_new.date_inserted AS date_inserted
  FROM (
    SELECT d.string_id, max(date_inserted) AS date_inserted FROM tmp.product_staging AS d where string_id IS NOT NULL GROUP BY string_id
  ) AS d_new
  LEFT JOIN aliases.string_id AS d_old
  ON d_new.string_id = d_old.string_id
  WHERE d_old.string_id IS NULL;
 
  ANALYZE aliases.string_id;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.aliases_string_id_product() OWNER TO xsl;





CREATE OR REPLACE FUNCTION data.product()
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  INSERT INTO data.product (
    source_file, 
    alias_id,            
    product_id,
    subproduct_id,
    product_possible,
    product_score,
    product_taken_date,
    product_churn_date,
    date_inserted
  )
  SELECT
    a.source_file, 
    b.alias_id, 
    a.product_id,
    a.subproduct_id,
    a.product_possible,
    a.product_score,
    a.product_taken_date,
    a.product_churn_date,
    a.date_inserted
  FROM tmp.product_staging AS a
  INNER JOIN aliases.string_id AS b
  ON a.string_id = b.string_id
  WHERE a.date_inserted IS NOT NULL;
    
  ANALYZE data.product;  

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.product() OWNER TO xsl;

--------------------------------------------------------------------------------

-- The functions needed in the loading of the BLACKLIST data


CREATE OR REPLACE FUNCTION data.tmp_blacklist_staging(dataset text)
  RETURNS void AS
$BODY$

DECLARE

BEGIN

  analyze tmp.blacklist_err;

  insert into data.failed_rows_stats
  (data_source, source_file, rowcount)
  select 
    'blacklist' as data_source,
    aa.source_file as source_file,
    count(*) as rowcount
  from (
    select substring(rawdata from 1 for position('|' in rawdata)-1) as source_file, * from tmp.blacklist_err
  ) aa
  left outer join (
    select source_file from data.failed_rows_stats where data_source = 'blacklist' group by source_file
  ) bb
  on aa.source_file = bb.source_file
  where bb.source_file is null
  group by aa.source_file;

  analyze data.failed_rows_stats;

  ANALYZE tmp.blacklist;

  truncate table tmp.blacklist_staging;

  INSERT INTO tmp.blacklist_staging (
    source_file,
    subscriber_id,
    blacklist_id,
    timestamp)
  SELECT
    nullif(trim(d_new.source_file),'') AS source_file,
    nullif(trim(d_new.subscriber_id),'') AS subscriber_id,
    nullif(trim(d_new.blacklist_id),'')::text AS blacklist_id,
    to_date(nullif(trim(d_new.timestamp),''), 'YYYY-MM-DD') AS timestamp
    from tmp.blacklist as d_new
  LEFT JOIN (
    SELECT pf.source_file
    FROM data.processed_files AS pf
    WHERE pf.source_file LIKE '%blacklist%'
  ) AS d_old
  ON d_new.source_file = d_old.source_file
  WHERE d_old.source_file IS NULL;
  
  ANALYZE tmp.blacklist_staging;

  INSERT INTO data.processed_files 
  (source_file, max_timestamp, rowcount, parameter1, parameter2, dataset_id)
  SELECT 
    case 
      when source_file is not null and source_file != '' then trim(source_file) else NULL 
    end as source_file,
    max(timestamp) as max_timestamp,
    count(*) as rowcount,
    count(distinct subscriber_id) as parameter1,
    count(distinct blacklist_id) as parameter2,
    dataset as dataset_id
  FROM tmp.blacklist_staging GROUP BY 1;
  
  ANALYZE data.processed_files;

  INSERT INTO data.processed_data 
  (dataset_id, data_type, data_date, rowcount)
  SELECT 
    dataset as dataset_id,
    'blacklist' as data_type,
    timestamp as data_date,
    count(*) as rowcount
  FROM tmp.blacklist_staging GROUP BY timestamp;

  ANALYZE data.processed_data;
  
END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION data.tmp_blacklist_staging(text)
  OWNER TO xsl;

----------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION data.blacklist()
  RETURNS void AS
$BODY$

DECLARE

BEGIN
   
  INSERT INTO aliases.blacklist (
    alias_id,
    blacklist_id,
    validity,
    on_black_list)
  SELECT
    alias_id,
    blacklist_id,
    timestamp AS validity,
    1 AS on_black_list 
    FROM (
      -- remove duplicates from input file and 
      -- taking the maximum value of blacklist_id
      -- in this case
      SELECT alias_id,
             max(blacklist_id) as blacklist_id,
             timestamp
      FROM tmp.blacklist_staging AS d
      INNER JOIN aliases.string_id AS b
      ON d.subscriber_id = b.string_id
      GROUP BY alias_id, timestamp
    ) u;
  
  /*
     enable the feature that the blacklist
     is empty starting from a certain date
  */
  INSERT INTO aliases.blacklist (
    alias_id,
    blacklist_id,
    validity,
    on_black_list)
  SELECT 
    -1,
    blacklist_id,
    timestamp AS validity,
    1 AS on_black_list
  FROM (
      -- remove duplicates from input file and
      -- taking the maximum value of blacklist_id
      -- in this case
      SELECT subscriber_id,
             max(blacklist_id) as blacklist_id,
             timestamp
      FROM tmp.blacklist_staging AS d
      WHERE subscriber_id = 'no_blacklist'
      GROUP BY subscriber_id, timestamp
  ) u;

  ANALYZE aliases.blacklist;

END;

$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION data.blacklist() OWNER TO xsl;


----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION data.check_monday(in_date date)
  RETURNS void AS
$BODY$
DECLARE
BEGIN

	IF in_date != date_trunc('week', in_date)::date
	THEN
    RAISE EXCEPTION 'The starting date for deleting data must be a Monday';      
	END IF;

END;

$BODY$
  LANGUAGE plpgsql;
ALTER FUNCTION data.check_monday(in_date date) OWNER TO xsl;

----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.create_validation_errors_partitions()
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Creates partitions for data.data_quality table
 * VERSION
 * 29.05.2013 Timur
 */
DECLARE

  datelist  date[];

  clevel    integer;
  clup      boolean;
  d         record;

BEGIN

  -- Find the dates that are found in tmp.validation_errors:
  datelist := ARRAY(SELECT data_date FROM tmp.validation_errors GROUP BY data_date);

  -- data.validation_errors --

  -- Read compresslevel and cleanup status:
  SELECT
    compresslevel,
    cleanup
  FROM core.partition_date_tables
  WHERE table_name = 'data.validation_errors'
  INTO clevel, clup;

  -- Create partitions:
  FOR d IN (
    SELECT s.data_date
    FROM (SELECT unnest(datelist) AS data_date) AS s
    LEFT JOIN (SELECT data_date FROM core.partition_date_create_times WHERE table_name = 'data.validation_errors') AS p
    ON s.data_date = p.data_date
    WHERE p.data_date IS NULL -- make sure we do not try to create partitions that already exist
    AND s.data_date IS NOT NULL
  )
  LOOP
    INSERT INTO core.partition_date_create_times (table_name, data_date, time_created, cleanup)
    VALUES ('data.validation_errors', d.data_date, now(), clup);

    EXECUTE 'ALTER TABLE data.validation_errors '
         || 'ADD PARTITION "' || d.data_date::text || '" '
         || 'START (''' || d.data_date || '''::date) END (''' || (d.data_date + 1) || '''::date) '
         || COALESCE('WITH (appendonly=false)', '');
  END LOOP;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.create_validation_errors_partitions() OWNER TO xsl;

----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION data.purge_common_data(in_start_date date, in_data_type text)
  RETURNS void AS
$BODY$
DECLARE
BEGIN

	delete from data.data_quality 
	where data_date >= in_start_date
	and data_source = in_data_type
	;
	delete from data.validation_errors
	where data_date >= in_start_date
	and data_type = in_data_type
	;
	delete from data.processed_data 
	where data_date >= in_start_date
	and data_type = in_data_type
	;
	delete from data.processed_files 
	where max_timestamp::date >= in_start_date
	and source_file LIKE '%'||in_data_type||'%'
	;
	DELETE FROM data.failed_rows_stats
	WHERE data_source = in_data_type
	AND source_file NOT IN (SELECT source_file
	                        FROM   data.processed_files df
	                        WHERE  source_file LIKE '%'||in_data_type||'%'
	                       )
	;

END;

$BODY$
  LANGUAGE plpgsql;
ALTER FUNCTION data.purge_common_data(in_start_date date, in_data_type text) OWNER TO xsl;

----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION data.purge_cdr_data(in_start_date date)
  RETURNS void AS
$BODY$
DECLARE
BEGIN

	PERFORM core.remove_date_partitions('data.call_types_weekly', in_start_date, null)
	;
	PERFORM core.remove_date_partitions('data.cdr', in_start_date, null)
	;
	PERFORM core.remove_period_partitions('data.in_split_aggregates', core.date_to_yyyyww(in_start_date), null)
	;
	PERFORM core.remove_period_partitions('data.in_split_weekly', core.date_to_yyyyww(in_start_date), null)
	;
	delete from data.cdr_weekly_statistics
	where period >= core.date_to_yyyyww(in_start_date)
	;
	delete from data.data_usage_weekly_stats
	where period >= core.date_to_yyyyww(in_start_date) 
	;

END;

$BODY$
  LANGUAGE plpgsql;
ALTER FUNCTION data.purge_cdr_data(in_start_date date) OWNER TO xsl;

----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION data.purge_crm_data(in_start_date date)
  RETURNS void AS
$BODY$
DECLARE
	in_crm_start date;
BEGIN

	PERFORM core.remove_date_partitions('data.crm', in_start_date, null);

        -- delete data which is based on date_inserted entries

	select min(date_inserted) from data.crm_snapshot_date_inserted_mapping into in_crm_start
        where snapshot_date >= in_start_date;

        -- in case of in_crm_start = null, nothing is deleted

	PERFORM core.remove_date_partitions('data.in_crm', in_crm_start, null);

        delete from data.crm_statistics where date_inserted >= in_crm_start;

        delete from data.crm_snapshot_date_inserted_mapping where date_inserted >= in_crm_start;

END;

$BODY$
  LANGUAGE plpgsql;
ALTER FUNCTION data.purge_crm_data(in_start_date date) OWNER TO xsl;

----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION data.purge_topup_data(in_start_date date)
  RETURNS void AS
$BODY$
DECLARE
BEGIN

	PERFORM core.remove_date_partitions('data.topup', in_start_date, null);
	
	delete from data.topup_statistics 
	where period >= core.date_to_yyyyww(in_start_date) 
	;

END;

$BODY$
  LANGUAGE plpgsql;
ALTER FUNCTION data.purge_topup_data(in_start_date date) OWNER TO xsl;

----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION data.purge_product_data(in_start_date date)
  RETURNS void AS
$BODY$
DECLARE
BEGIN

	PERFORM core.remove_date_partitions('data.product', in_start_date, null);

END;

$BODY$
  LANGUAGE plpgsql;
ALTER FUNCTION data.purge_product_data(in_start_date date) OWNER TO xsl;

----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION data.purge_blacklist_data(in_start_date date)
  RETURNS void AS
$BODY$
DECLARE
BEGIN

	delete from aliases.blacklist 
	where validity >= in_start_date
	;

END;

$BODY$
  LANGUAGE plpgsql;
ALTER FUNCTION data.purge_blacklist_data(in_start_date date) OWNER TO xsl;
