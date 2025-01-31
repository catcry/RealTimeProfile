/*
This file contains the schema upgrade sql from SL 2.4 to SL 2.5.
Functions are updated automatically outside this sql by the shell
script. Here are included all the changes needed to modify the
core tables while preserving the data. Also if functions are dropped
from SL 2.4 or their arguments are changed, they are dropped here.

The changes are here in cronological order, earlier changes in the
beginning of the file. Before each change, is the date and the
initials of the responsible person.
*/

\set ON_ERROR_STOP 1

   -- TODO

     -- the ALTER table statements below which use
     --    SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY
     -- can be run multiple times. So we have not to
     -- protect the script against multiple execution, if these
     -- alter table statements are the only upgrade statements 
     -- for version 2.5. 
     -- Otherwise the function upgrade_check_to_25 has to include
     -- some checks

CREATE OR REPLACE FUNCTION upgrade_check_to_25(schema_name text, proc_name text)
  RETURNS void AS
$BODY$
  DECLARE
  proc_count integer;
  BEGIN
     -- the ALTER table statements below which use
     --    SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY
     -- can be run multiple times. So we have not to
     -- protect the script against multiple execution, if these
     -- alter table statements are the only upgrade statements 
     -- for version 2.5
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION upgrade_check_to_25(text, text) OWNER TO xsl;

select upgrade_check_to_25('results', 'copy_vargroup_output_to_results');

DROP FUNCTION upgrade_check_to_25(text, text);

-- start: changes for compatibility with Greenplum 4.3.3

ALTER TABLE tmp.crm_err SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.cdr_err SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.topup_err SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.product_err SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.blacklist_err SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.validation_errors_err SET  WITH (REORGANIZE=true) DISTRIBUTED RANDOMLY;

-- end: changes for compatibility with Greenplum 4.3.3


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

