-- Copyright (c) 2012 Comptel Oy. All rights reserved.  
-- This software is the proprietary information of Comptel Oy. 
-- Use is subject to license terms. --

-- *** TODO check and discuss definition of couple of variables in base_2 


CREATE OR REPLACE FUNCTION work.calculate_descriptive_variables(in_mod_job_id integer, force_new_networks boolean)
  RETURNS SETOF text AS
$BODY$
/*
 * SUMMARY
 * The function calculate_descriptie_variables creates the job that is used to 
 * calculate the descriptive variables. 
 * 
 * INPUT:
 * - in_mod_job_id integer, 
 * 
 * VERSION
 * 
 * 20.06.2013 AVe Churn inactivity and churn postpaid split added. 
 * 15.04.2013 KL Added arpu_query for network scorer as a parameter to be initialized
 * 21.03.2013 HMa ICIF-122
 * 18.02.2013 MOj ICIF-107
 * 23.01.2013 HMa Multiple usecase support
 * 2.1.2012 KL bug 545 (ICIF-37), ICIF-56, ICIF-77
 * 12.12.2012 KL
 * 25.10.2011 LBe bug 759
 * 18.10.2011 LBe bug 742 & 743
 * 26.08.2011 LBe bug 700
 * 24.02.2011 LBe bug 441
 * 22.02.2011 LBe bug 417
 * 20.02.2011 LBe 
 */
DECLARE
  mod_job_id_original integer :=$1;
  tmp integer[];
  module_id integer;
  net_id integer;
  t1_date date;
  t2_date date;
  t4 date;
  t5 date;
  t4_pp date;
  t5_pp date;
  t6 date;
  t7 date;
  t_crm date;
  weeklist text;
  param_status integer;   
  network_exists integer;
  job_use_cases text;
  descvar_churn_inactivity_included boolean;
  descvar_churn_postpaid_included boolean;

  network_job_ids record;

  tmp_id text;

  arpu_query_descvar text;

BEGIN

  /*****************************************************************************
   * Create the primary module job.
   * Most of the descriptive variables are calculated FROM the source period of
   * this module job.  
   */

  -- Select the newest (by t2) multiple use cases mod_job_id of a Fit + Apply or Apply run if not specified 
  IF mod_job_id_original < 1 THEN
    mod_job_id_original := bb.mod_job_id
    FROM work.module_job_parameters AS aa
    INNER JOIN work.module_job_parameters AS bb
    ON aa.mod_job_id = bb.mod_job_id
    INNER JOIN work.module_job_parameters AS cc
    ON aa.mod_job_id = cc.mod_job_id
    WHERE aa.key = 'job_type' AND aa.value = 'multiple_use_cases'
    AND cc.key = 'run_type' AND (cc.value ~ 'Apply') -- In practice, either 'Predictors + Fit + Apply' or 'Predictors + Apply'
    AND bb.key = 't2'
    ORDER BY bb.value DESC -- Take the latest t2
    LIMIT 1;
  END IF;

  SELECT core.nextval_with_partitions('work.module_sequence') INTO module_id;
 
  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t1_date
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 't1';

  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t2_date
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 't2';

  SELECT mjp.value INTO job_use_cases
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'job_use_cases';

  -- Check if churn_inactivity and churn_postpaid use cases are included in the current run to calculate the churners within connections: 
  SELECT job_use_cases LIKE '%churn_inactivity%' INTO descvar_churn_inactivity_included;
  SELECT job_use_cases LIKE '%churn_postpaid%' INTO descvar_churn_postpaid_included;
  
  IF (descvar_churn_inactivity_included) THEN
    SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t4
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'uc_churn_inactivity_t4';

    SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t5
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'uc_churn_inactivity_t5';

    SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t6
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'uc_churn_inactivity_t6';

    SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t7
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'uc_churn_inactivity_t7';
  END IF;
  
  IF (descvar_churn_postpaid_included) THEN
    SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t4_pp
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'uc_churn_postpaid_t4';

    SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t5_pp
    FROM work.module_job_parameters AS mjp
    WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'uc_churn_postpaid_t5';
  END IF;
  
  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t_crm
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = mod_job_id_original AND lower(mjp.key) = 'tcrm';

  SELECT core.concatenate_yyyyww(t1_date, t2_date, ',') INTO weeklist;

  -- Find if there are old network corresponding to the current t2 value:
  IF NOT force_new_networks THEN	
    -- this could be simplified now as there is only one network (there used to be second delayd - for this reason the added complexity)
    SELECT  
      max(CASE WHEN t.t1 = t1_date THEN t.job_id ELSE NULL END) AS job_id_network
    INTO network_job_ids
    FROM (
      SELECT 
        m1.value AS t1, 
        m2.value AS t2, 
        max(job_id) AS job_id
      FROM work.module_job_parameters m1
      JOIN work.module_job_parameters m2 ON m1.mod_job_id = m2.mod_job_id
      JOIN work.module_job_parameters m3 ON m2.mod_job_id = m3.mod_job_id
      JOIN work.module_job_parameters m4 ON m3.mod_job_id = m4.mod_job_id
      JOIN (
        SELECT job_id 
        FROM work.out_network 
        GROUP BY job_id
      ) o 
    ON o.job_id::text = m4.value
    WHERE m1.key='t1' AND m2.key='t2' 
    AND m2.value=t2_date::text AND m1.value=t1_date::text
    AND m3.key='job_type' AND (m3.value='multiple_use_cases' OR m3.value='descriptive_variables')
    AND m4.key='xsl_job_id'
    GROUP BY t1, t2
    ) t;

    network_exists := network_job_ids.job_id_network;
     
  END IF;


  -- Check if new network should be calculated: 
  IF network_exists IS NULL OR force_new_networks THEN
    network_exists :=-1;
    SELECT core.nextval_with_partitions('work.network_sequence') INTO net_id;
  ELSE 
    net_id:=network_exists;
  END IF;


  INSERT INTO work.module_job_parameters
   (mod_job_id, "key", value)
  VALUES
   (module_id, 't1', t1_date),
   (module_id, 't2', t2_date),  
   (module_id, 'tCRM', t_crm),
   (module_id, 'job_type', 'descriptive_variables'),
   (module_id, 'job_use_cases', job_use_cases), 
   (module_id, 'in_mod_job_id', mod_job_id_original), --FROM which job this is created 
   (module_id, 'xsl_job_id', net_id);
  
  IF (descvar_churn_inactivity_included) THEN
    INSERT INTO work.module_job_parameters
     (mod_job_id, "key", value)
    VALUES
     (module_id, 'uc_churn_inactivity_t4', t4),
     (module_id, 'uc_churn_inactivity_t5', t5),
     (module_id, 'uc_churn_inactivity_t6', t6),
     (module_id, 'uc_churn_inactivity_t7', t7);
  END IF;
  
  IF (descvar_churn_postpaid_included) THEN
    INSERT INTO work.module_job_parameters
     (mod_job_id, "key", value)
    VALUES
     (module_id, 'uc_churn_postpaid_t4', t4_pp),
     (module_id, 'uc_churn_postpaid_t5', t5_pp);
  END IF;
  
  INSERT INTO work.module_job_parameters (mod_job_id, "key", "value")
  SELECT module_id AS mod_job_id, 'time_inserted' AS "key", to_char(CURRENT_TIMESTAMP(0), 'YYYY-MM-DD HH24:MI:SS') AS "value";

  -- Update or insert the descriptive variables mod_job_id to the multiple usecases run module job parameters:
  tmp_id := value FROM work.module_job_parameters WHERE mod_job_id = mod_job_id_original AND key = 'descvar_mod_job_id';
  IF tmp_id IS NULL THEN
    INSERT INTO work.module_job_parameters (mod_job_id, "key", value)
    VALUES (mod_job_id_original, 'descvar_mod_job_id', module_id);
  ELSE 
    RAISE WARNING 'Resetting the descriptive variables module job id in work.module_job_parameters of the multiple usecases run!';
    UPDATE work.module_job_parameters mjp SET "value" = module_id::text WHERE mjp.mod_job_id = mod_job_id_original AND mjp.key = 'descvar_mod_job_id';
  END IF;

   
  arpu_query_descvar='SELECT alias_id, monthly_arpu AS arpu
              FROM work.monthly_arpu
              WHERE mod_job_id = '|| module_id ||';'; 
       
   PERFORM core.analyze('work.module_job_parameters', in_mod_job_id);

   RETURN NEXT param_status::text;--------------------------------------------------------- 0
   RETURN NEXT mod_job_id_original::text; ------------------------------------------------- 1
   RETURN NEXT module_id::text; ----------------------------------------------------------- 2
   RETURN NEXT net_id::text;  ------------------------------------------------------------- 3
   RETURN NEXT weeklist; ------------------------------------------------------------------ 4
   RETURN NEXT t_crm::text; --------------------------------------------------------------- 5
   RETURN NEXT t1_date::text; ------------------------------------------------------------- 6
   RETURN NEXT t2_date::text; ------------------------------------------------------------- 7
   RETURN NEXT network_exists::text;------------------------------------------------------- 8
   RETURN NEXT CASE WHEN descvar_churn_inactivity_included THEN 'true' ELSE 'false' END; --- 9
   RETURN NEXT CASE WHEN descvar_churn_postpaid_included  THEN 'true' ELSE 'false' END; -- 10
   RETURN NEXT arpu_query_descvar; ---------------------------------------------------------11

  RETURN;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.calculate_descriptive_variables(integer, boolean) OWNER TO xsl;

------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.descriptive_variables_fill_subscriber_list(in_mod_job_id integer)
  RETURNS SETOF integer AS
$BODY$
/*
 * This function runs creates the target list and fills the table 
 * descriptive_variables_subscriberlist, which contains the alias_id's 
 * for which the descriptive variables will be calculated.
 *
 * VERSION
 *
 * 2.1.2013KL bug 545 (ICIF-37), ICIF-56, ICIF-77
 * 14.12. 2012 KL corrected missing start and end dates
  * 20.02.2011 LBe 
 */
DECLARE
  netjobid integer = (SELECT value::integer FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id and key = 'xsl_job_id');
  start_date date = (SELECT value::date FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id and key = 't1');
  end_date date = (SELECT value::date FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id and key = 't2');
  from_mod_job_id integer= (SELECT value::integer FROM work.module_job_parameters WHERE mod_job_id = in_mod_job_id and key = 'in_mod_job_id');
BEGIN

  IF (SELECT count(*) FROM work.aliases WHERE job_id = netjobid) = 0 THEN
    PERFORM work.fetch_aliases(netjobid, start_date, end_date);
  END IF;


  IF (SELECT count(*) FROM work.module_targets WHERE mod_job_id = from_mod_job_id) = 0 THEN
    SELECT min(b.mod_job_id) INTO from_mod_job_id
    FROM work.module_targets a
    JOIN work.module_job_parameters b
    ON a.mod_job_id = b.mod_job_id   
    WHERE b.key = 't2'
    AND b.value::date = end_date; 
  END IF;

  TRUNCATE TABLE tmp.descriptive_variables_subscriberlist;

  INSERT INTO tmp.descriptive_variables_subscriberlist (alias_id)
  SELECT m.alias_id 
  FROM (
    SELECT a.alias_id 
    FROM work.module_targets m 
    JOIN work.aliases a 
    ON m.alias_id=a.alias_id 
    WHERE mod_job_id = from_mod_job_id 
    AND a.job_id=netjobid 
  ) m
  LEFT JOIN (
    SELECT alias_id 
    FROM work.networkscorer_blacklist a 
    WHERE job_id=netjobid
  ) a 
  ON a.alias_id=m.alias_id 
  WHERE a.alias_id IS NULL
  GROUP BY m.alias_id;

 END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.descriptive_variables_fill_subscriber_list(integer) OWNER TO xsl;

---------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.descriptive_variables_temporary_networks(netjob_id integer)
  RETURNS SETOF integer AS
$BODY$
/*
 * SUMMARY
 * Fill the table tmp.out_network_raw for the descriptive variables computations. 
 * 
 * VERSION
 * 18.02.2013 MOj ICIF-107
 * 2.1.2013 KL bug 545 (ICIF-37), ICIF-56, ICIF-77
 * 18.12.2012 KL
 * 21.02.2011 LBe
 */
BEGIN

 /*****************************************************************************
   * Copy the primary network into tmp table. Most of the descriptive variables 
   * are calculated FROM the this network. 
   */

  TRUNCATE TABLE tmp.out_network_raw;

  INSERT INTO tmp.out_network_raw (alias_a, alias_b, job_id, weight)
  SELECT alias_a, alias_b, aa.job_id, weight
  FROM work.out_network aa
  LEFT OUTER JOIN work.networkscorer_blacklist bb 
  ON aa.job_id = bb.job_id AND aa.alias_a = bb.alias_id
  LEFT OUTER JOIN work.networkscorer_blacklist cc 
  ON aa.job_id = cc.job_id AND aa.alias_b = cc.alias_id
  WHERE aa.job_id = netjob_id 
  AND bb.alias_id IS NULL 
  AND cc.alias_id IS NULL;

  PERFORM core.analyze('tmp.out_network_raw', netjob_id);

END;  
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.descriptive_variables_temporary_networks(integer) OWNER TO xsl;

---------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION work.calculate_descriptive_variables_base_1(in_mod_job_id integer, netjob_id integer)
  RETURNS integer AS
$BODY$
/* Function work.calculate_descriptive_variables_base_1()
 * 
 * SUMMARY
 * The descriptive variables that are computed in function work.calculate_descriptive_variables_base_2
 * are more time-intensive and require the availability of a network the is computed on both 
 * on-net as off-net subscribers. The function work.calculate_descriptive_variables_base_1 can be used 
 * after a basis network computation, for example for the UI Target and Analyse jobs.
 * Limit for new subscribers is 30 days
 * 
 * All descriptive variables which are computed using the tables:
 * -- work.out_network (tmp.out_network_raw)
 * -- work.out_scores
 * can be found in the function work.calculate_descriptive_variables_base_2
 * 
 * VERSION
 * 21.8.2013 QYu added data usage related variables (call type = 5)
 * 5.2.2013 KL added another input variable so that also network roles will be filled
 * 2.1.2013 KL bug 545 (ICIF-37), ICIF-56, ICIF-77
 * 05.10.2011 LBe fixed bug 732
 * 15.04.2011 LBe fixed bug 532
 * 22.02.2011 LBe
 * < 22.02.2011 MAk
 */
DECLARE
  core_job_id integer := -1 ;
  net_job_id integer := -1 ;
  num_weeks integer := 1 ;
  retval integer= 1;
  in_var_id integer;
  t1 date;
  t2 date;
  weeklist text;  
  new_limit integer = 30; -- new subscribers are subscribers for which t2- crm.switch_on_date < new_limit
  this_date date;
  var_id_i integer;
  mb_scale integer:=1;
BEGIN
  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t1
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1';

  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t2
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2';

  num_weeks:=floor((t2-t1)/7);
  
  this_date := t2; --(monday end of source period)

  INSERT INTO tmp.descriptive_variables_subscriber_matrix_base_1a
  (mod_job_id, 
   alias_id, 
   neigh_count_rec,
   neigh_count_made,
   voice_rec_duration,
   neigh_count_made_weekly,
   neigh_count_rec_weekly,
   voice_made_nmb,
   voice_rec_nmb,
   sms_made_nmb,
   sms_rec_nmb,
   share_of_voice,
   voice_made_duration,
   voice_made_nmb_day,
   voice_made_nmb_eve,
   voice_made_nmb_night,
   voice_made_nmb_weekday,
   voice_made_nmb_weekend,
   voice_made_duration_day,
   voice_made_duration_eve,
   voice_made_duration_night,
   voice_made_duration_weekday,
   voice_made_duration_weekend,
   sms_made_day,
   sms_made_eve,
   sms_made_night,
   sms_made_weekday,
   sms_made_weekend,
   data_usage_day,
   data_usage_eve,
   data_usage_night,
   data_usage_weekday,
   data_usage_weekend,
   weekly_data_usage,
   weekly_data_usage_cost)
--   weekly_cost,
--   share_of_data_usage)
  SELECT 
    in_mod_job_id, 
    aa.alias_id, 
    aa.who_called                  AS neigh_count_rec,
    aa.ids_called                  AS neigh_count_made,
    aa.duration                    AS voice_rec_duration,
    nn.weekly_called               AS neigh_count_made_weekly,
    nn.weekly_who_called           AS neigh_count_rec_weekly,
    nn.made_calls                  AS voice_made_nmb,
    nn.received_calls              AS voice_rec_nmb,
    nn.sent_sms                    AS sms_made_nmb,
    nn.received_sms                AS sms_rec_nmb,
    nn.share_voice_activity        AS share_of_voice,
    nn.duration_made               AS voice_made_duration,
    nn.voice_calls_day             AS voice_made_nmb_day,
    nn.voice_calls_evening         AS voice_made_nmb_eve,
    nn.voice_calls_night           AS voice_made_nmb_night,
    nn.voice_calls_weekday         AS voice_made_nmb_weekday,
    nn.voice_calls_weekend         AS voice_made_nmb_weekend,
    nn.voice_calls_day_vol         AS voice_made_duration_day,
    nn.voice_calls_evening_vol     AS voice_made_duration_eve,
    nn.voice_calls_night_vol       AS voice_made_duration_night,
    nn.voice_calls_weekday_vol     AS voice_made_duration_weekday,
    nn.voice_calls_weekend_vol     AS voice_made_duration_weekend,
    nn.sent_sms_day                AS sms_made_day,
    nn.sent_sms_evening            AS sms_made_eve,
    nn.sent_sms_night              AS sms_made_night,
    nn.sent_sms_weekday            AS sms_made_weekday,
    nn.sent_sms_weekend            AS sms_made_weekend,
    nn.data_usage_day              AS data_usage_day,
    nn.data_usage_eve              AS data_usage_eve,
    nn.data_usage_night            AS data_usage_night,
    nn.data_usage_weekday          AS data_usage_weekday,
    nn.data_usage_weekend          AS data_usage_weekend,
    nn.weekly_data_usage           AS weekly_data_usage,
    nn.weekly_data_usage_cost      AS weekly_data_usage_cost
  --nn.weekly_cost                 AS weekly_cost,
  --nn.share_of_data_usage         AS share_of_data_usage
  FROM (
    SELECT 
      t.alias_id, 
      t.who_called, 
      u.ids_called, 
      t.duration 
    FROM (
      SELECT 
        a.alias_id, 
        COALESCE(count(distinct oo.alias_a), 0) AS who_called,   
        COALESCE(sum(oo.v_s), 0)::double precision / (60.0 * num_weeks) AS duration  
      FROM tmp.descriptive_variables_subscriberlist a
      JOIN data.in_split_weekly oo 
	ON a.alias_id = oo.alias_b
      LEFT JOIN work.networkscorer_blacklist oo2 
      ON oo.alias_a=oo2.alias_id AND oo2.job_id=netjob_id
      LEFT JOIN work.networkscorer_blacklist oo1 
      ON oo.alias_b=oo1.alias_id AND oo1.job_id=netjob_id
      WHERE oo.period >= core.date_to_yyyyww(t1) AND oo.period < core.date_to_yyyyww(t2)
      AND oo1.alias_id IS NULL AND oo2.alias_id IS NULL
      GROUP BY a.alias_id
    ) t  
    JOIN (
      SELECT 
        a.alias_id, 
        COALESCE(count(distinct oo.alias_b), 0) AS ids_called   
      FROM tmp.descriptive_variables_subscriberlist a
      JOIN data.in_split_weekly oo 
	ON a.alias_id = oo.alias_a
      LEFT JOIN work.networkscorer_blacklist oo2 
      ON oo.alias_a=oo2.alias_id  AND oo2.job_id=netjob_id 
      LEFT JOIN work.networkscorer_blacklist oo1 
      ON oo.alias_b=oo1.alias_id  AND oo1.job_id=netjob_id
      WHERE oo.period >= core.date_to_yyyyww(t1) AND oo.period < core.date_to_yyyyww(t2)
      AND oo1.alias_id IS NULL AND oo2.alias_id IS NULL
      GROUP BY a.alias_id 
    ) u 
    ON u.alias_id=t.alias_id
  ) aa  
  JOIN (
    SELECT 
      bb.alias_id AS aa_i, 
      COALESCE(sum(gg.mc_alias_count), 0.0)::double precision / num_weeks  AS weekly_called,
      COALESCE(sum(gg.rc_alias_count), 0.0)::double precision / num_weeks  AS weekly_who_called,
      COALESCE(sum(CASE WHEN voicecount > 0 THEN voicecount ELSE 0 END)::double precision, 0) / num_weeks AS made_calls,
      COALESCE(sum(CASE WHEN rc_voicecount > 0 THEN rc_voicecount ELSE 0 END)::double precision, 0) / num_weeks AS received_calls,
      COALESCE(sum(CASE WHEN smscount > 0 THEN smscount ELSE 0 END), 0)::double precision / num_weeks AS sent_sms,
      COALESCE(sum(CASE WHEN rc_smscount > 0 THEN rc_smscount ELSE 0 END), 0)::double precision / num_weeks received_sms,
      COALESCE(CASE WHEN sum(smscount + voicecount) > 0 THEN sum(voicecount)::double precision / sum(smscount + voicecount) ELSE 0 END) AS share_voice_activity,
      COALESCE(sum(CASE WHEN voicesum > 0 THEN voicesum ELSE 0 END)::double precision, 0) / (60.0 * num_weeks) AS duration_made,
      COALESCE(sum(voicecountday), 0)::double precision / num_weeks AS voice_calls_day,
      COALESCE(sum(voicecountevening), 0)::double precision / num_weeks AS voice_calls_evening,
      COALESCE(sum(voicecount - voicecountday - voicecountevening), 0)::double precision / num_weeks AS voice_calls_night,
      COALESCE(sum(voicecountweekday), 0)::double precision / num_weeks AS voice_calls_weekday,
      COALESCE(sum(voicecount - voicecountweekday), 0)::double precision / num_weeks AS voice_calls_weekend,
      COALESCE(sum(voicesumday), 0)::double precision / (60.0 * num_weeks) AS voice_calls_day_vol,
      COALESCE(sum(voicesumevening), 0)::double precision / (60.0 * num_weeks) AS voice_calls_evening_vol,
      COALESCE(sum(voicesum - voicesumday - voicesumevening), 0)::double precision / (60.0 * num_weeks) voice_calls_night_vol,
      COALESCE(sum(voicesumweekday), 0)::double precision / (60.0 * num_weeks) AS voice_calls_weekday_vol,
      COALESCE(sum(voicesum - voicesumday), 0)::double precision / (60.0 * num_weeks)  AS voice_calls_weekend_vol,
      COALESCE(sum(smscountday), 0)::double precision / num_weeks AS sent_sms_day,
      COALESCE(sum(smscountevening), 0)::double precision / num_weeks AS sent_sms_evening,
      COALESCE(sum(smscount - smscountday - smscountevening), 0)::double precision / num_weeks AS sent_sms_night,
      COALESCE(sum(smscountweekday), 0)::double precision / num_weeks AS sent_sms_weekday,
      COALESCE(sum(smscount - smscountweekday), 0)::double precision / num_weeks sent_sms_weekend,
	COALESCE(sum(data_usage_sumday), 0)::double precision / (mb_scale * num_weeks) AS data_usage_day,
      COALESCE(sum(data_usage_sumevening), 0)::double precision / (mb_scale * num_weeks) AS data_usage_eve,
    COALESCE(sum(weekly_data_usage_sum - data_usage_sumday-data_usage_sumevening), 0)::double precision / (mb_scale * num_weeks)  AS data_usage_night,
      COALESCE(sum(data_usage_sumweekday), 0)::double precision / (mb_scale * num_weeks) AS data_usage_weekday,
      COALESCE(sum(weekly_data_usage_sum - data_usage_sumweekday), 0)::double precision / (mb_scale * num_weeks)  AS data_usage_weekend,
    COALESCE(sum(weekly_data_usage_sum), 0)::double precision / (mb_scale * num_weeks)  AS weekly_data_usage,
	COALESCE(sum(weekly_data_usage_cost_sum), 0)::double precision / (mb_scale * num_weeks)  AS weekly_data_usage_cost
	--COALESCE(sum(weekly_cost_sum), 0)::double precision / (mb_scale * num_weeks)  AS weekly_cost,
    --COALESCE(CASE WHEN sum(smscount + voicecount) > 0 THEN sum( 0.8 * sqrt(weekly_data_usage_sum))::double precision / sum(smscount + voicecount) ELSE 0 END) AS share_of_data_usage
    FROM tmp.descriptive_variables_subscriberlist bb
    JOIN data.in_split_aggregates gg 
    ON bb.alias_id = gg.alias_id
    WHERE gg.period >=core.date_to_yyyyww(t1) AND gg.period <core.date_to_yyyyww(t2)
    GROUP BY bb.alias_id
  ) nn 
  ON aa.alias_id=nn.aa_i;
  
  INSERT INTO tmp.descriptive_variables_subscriber_matrix_base_1b (mod_job_id, alias_id, new_subs_within_connections)
  SELECT 
    in_mod_job_id, 
    aa.alias_a, 
    sum(case when cc.switch_on_date is not null and (cc.switch_on_date > this_date::date - new_limit) THEN 1 ELSE 0 END) AS new_subs_within_connections
  FROM tmp.out_network_raw aa
  JOIN tmp.descriptive_variables_subscriberlist sl ON aa.alias_a = sl.alias_id 
  JOIN (
    SELECT alias_id, max(switch_on_date) AS switch_on_date
    FROM data.in_crm 
    WHERE switch_on_date IS NOT NULL 
    AND switch_on_date < t2
    GROUP BY alias_id
  ) cc
  ON aa.alias_b = cc.alias_id 
  GROUP BY aa.alias_a;

  INSERT INTO tmp.descriptive_variables_subscriber_matrix_base_1c (mod_job_id, alias_id, social_role)
  SELECT 
    in_mod_job_id, 
    aa.alias_id,
    CASE 
      WHEN ishub = 1 THEN 'Hub'
      WHEN isbridge = 1 THEN 'Bridge'
      WHEN isoutlier = 1 THEN 'Outlier' 
    END AS social_role
  FROM work.network_roles aa
  JOIN tmp.descriptive_variables_subscriberlist sl ON aa.alias_id = sl.alias_id 
  WHERE aa.job_id = netjob_id;
  
  RETURN retval;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.calculate_descriptive_variables_base_1(integer, integer)
  OWNER TO xsl;

---------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.calculate_descriptive_variables_base_2(in_mod_job_id integer, net_job_id integer)
  RETURNS integer AS
$BODY$
/* Function work.calculate_descriptive_variables_base_2(integer, integer, integer)
 * 
 * SUMMARY
 * The descriptive variables that are computed in function work.calculate_descriptive_variables_base_2
 * are more time-intensive and require the availability of a network that is computed on both 
 * on-net as off-net subscribers. The function work.calculate_descriptive_variables_base_1 can be used 
 * after a basic network computation, for example for the UI Target and Analyse jobs.
 * 
 * All descriptive variables which are computed using the tables:
 * -- work.out_network (tmp.out_network_raw)
 * -- work.out_scores
 * -- data.in_split_weekly and work.aliases
 * can be found in the function work.calculate_descriptive_variables_base_2
 * 
 * Note! This function is an addition to the function calculate_descriptive_variables_base_1. 
 * 
 * VERSION
 * 14.06.2013 HMa ICIF-146
 * 18.02.2013 MOj ICIF-107
 * 24.01.2013 HMa Added in_delayed_mob_job_id to parameters to check churners within connections correctly
 * 22.01.2013 HMa changed 'target' to 'target_churn_inactivity'
 * 2.1.2013 KL bug 545 (ICIF-37), ICIF-56, ICIF-77
 * 18.09.2012 HMa Bug ICIF-70
 * 29.06.2012 HMa Bug 890
 * 26.10.2011 LBe Bug 760
 * 15.04.2011 LBe Bug 532
 * 22.02.2011 LBe
 * < 22.02.2011 MAk
 */
DECLARE
  core_job_id integer := -1 ;
  num_weeks integer := 1 ;
  t1 date;
  t2 date;
 
  -- the variable 'null_values_as_churn': 
  --   == 0 if subscribers for which module_results.target = 'null' are _NOT_ considered as churners
  --   == 1 if subscribers for which module_results.target = 'null' are considered as churners
  null_values_as_churn  integer = 1;   
 
  retval integer= 1;
  in_var_id integer;
BEGIN

  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t1
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1';
 
  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t2
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2';

  num_weeks:=floor((t2-t1)/7);
  
  INSERT INTO tmp.descriptive_variables_subscriber_matrix_base_2a 
  (mod_job_id,
   alias_id,
   social_revenue,
   community_id,
   connectedness_of_connections,
   connections,
   social_connectivity_score,
   offnet_connections_of_onnet_connections,
   connections_of_onnet_connections,
   offnet_connections,
   share_of_offnet_connections)
  SELECT
    in_mod_job_id                        AS mod_job_id,
    ac.alias_id                          AS alias_id,
    ac.socrev                            AS social_revenue,
    ac.community_id                      AS community_id,
    ac.C                                 AS connectedness_of_connections,
    ac.k                                 AS connections,
    ac.alpha                             AS social_connectivity_score,
    ab.off_conn_onnet_conn               AS offnet_connections_of_onnet_connections,
    ab.conn_onnet_conn                   AS connections_of_onnet_connections,
    ad.off_net_conn                      AS offnet_connections,
    ad.share_off_net_conn                AS share_of_offnet_connections
  FROM (
    SELECT 
      aa.alias_id, 
      aa.community_id AS community_id, 
      alpha,  
      COALESCE(k, 0) AS k, 
      COALESCE(c, 0) AS C, 
      COALESCE(socrev, 0) AS socrev
    FROM work.out_scores aa 
    JOIN tmp.descriptive_variables_subscriberlist sl
    ON aa.alias_id = sl.alias_id
    WHERE aa.job_id = net_job_id 
  ) ac
  LEFT  JOIN (
    SELECT 
      sl.alias_id AS a_i,
      count(distinct cc.alias_b) AS conn_onnet_conn, 
      count(distinct (CASE WHEN bb.in_out_network = 0 THEN cc.alias_b ELSE NULL END)) AS off_conn_onnet_conn
    FROM tmp.descriptive_variables_subscriberlist sl 
    JOIN tmp.out_network_raw dd ON sl.alias_id = dd.alias_a
    JOIN tmp.out_network_raw cc ON cc.alias_a = dd.alias_b
    JOIN work.aliases aa ON aa.alias_id = cc.alias_a 
    JOIN work.aliases bb ON bb.alias_id = cc.alias_b AND aa.job_id = bb.job_id
    WHERE aa.job_id = net_job_id 
    AND aa.in_out_network = 1 -- require subscribers' connections to be on-net
    GROUP BY sl.alias_id
  ) ab  
  ON ab.a_i=ac.alias_id
  LEFT JOIN (
    SELECT 
      sl.alias_id AS al_id, 
      COALESCE(sum(1-bb.in_out_network), 0) AS off_net_conn, 
      CASE 
        WHEN (sum(bb.in_out_network) + sum(1 - bb.in_out_network)) > 0 THEN 
          COALESCE(sum(1-bb.in_out_network), 0)::double precision / (sum(bb.in_out_network) + sum(1 - bb.in_out_network)) 
        ELSE 0 
      END AS share_off_net_conn
    FROM work.aliases bb
    JOIN tmp.out_network_raw cc ON bb.alias_id = cc.alias_b
    JOIN tmp.descriptive_variables_subscriberlist sl ON alias_a = sl.alias_id
    WHERE bb.job_id = net_job_id 
    GROUP BY sl.alias_id
  ) AS ad 
  ON ad.al_id=alias_id; 

  INSERT INTO  tmp.descriptive_variables_subscriber_matrix_base_2b (mod_job_id, alias_id, no_connections) 
  SELECT 
    in_mod_job_id AS mod_job_id, 
    aa.alias_id,
    CASE WHEN ss.k IS  NULL THEN 1 ELSE 0 END AS no_connections
  FROM tmp.descriptive_variables_subscriberlist aa 
  LEFT OUTER JOIN (
    SELECT alias_id, k 
    FROM work.out_scores 
    WHERE k>0 
    AND job_id = net_job_id
  ) ss 
  ON aa.alias_id = ss.alias_id;

  -- *** TODO check and discuss definition ***    
  INSERT INTO  results.descriptive_variables_community_matrix 
  (mod_job_id, 
   community_id, 
   "Linkedness within community",
   "Intracommunication ratio",
   "Communication density")
  SELECT
    in_mod_job_id,
    af.community_id,
    af.linkedness            AS "Linkedness within community",
    af.intra_communication   AS "Intracommunication ratio",
    ae.communication_density AS "Communication density"
  FROM (
    SELECT 
      aa.community_id,  
      sum(
        CASE 
          WHEN cc.weight > 0 AND aa.community_id = bb.community_id 
          THEN cc.weight
          ELSE 0 
        END
      ) / (
        1 + sum(
          CASE 
            WHEN cc.weight > 0 AND aa.community_id != bb.community_id 
            THEN cc.weight
            ELSE 0 
          END
        )
      ) AS intra_communication, 
      CASE 
        WHEN sum(CASE WHEN aa.community_id != bb.community_id THEN 1 ELSE 0 END) > 0 
        THEN 
          sum(CASE WHEN aa.community_id = bb.community_id THEN 1 ELSE 0 END)::double precision / 
          sum(CASE WHEN aa.community_id != bb.community_id THEN 1 ELSE 0 END)::double precision
        ELSE 0 
      END AS linkedness
    FROM work.out_scores aa
    JOIN work.out_scores bb ON aa.job_id = bb.job_id
    JOIN tmp.out_network_raw cc ON aa.alias_id = cc.alias_a AND bb.alias_id = cc.alias_b
    JOIN tmp.descriptive_variables_subscriberlist sl ON aa.alias_id = sl.alias_id
    WHERE bb.job_id = net_job_id         
    GROUP BY aa.community_id
  ) af
  JOIN (
    SELECT 
      aa.community_id, 
      CASE 
        WHEN cc.weight > 0 AND aa.community_id = bb.community_id 
        THEN avg(cc.weight) 
        ELSE NULL 
      END AS communication_density 
    FROM work.out_scores aa
    JOIN work.out_scores bb ON aa.job_id = bb.job_id
    JOIN tmp.out_network_raw cc ON aa.alias_id = cc.alias_a AND bb.alias_id = cc.alias_b
    JOIN tmp.descriptive_variables_subscriberlist sl ON aa.alias_id = sl.alias_id
    WHERE bb.job_id = net_job_id
    GROUP by aa.community_id, cc.weight, bb.community_id --THIS IS WRONG, SEE BUG ICIF-95!!
  ) ae
  ON af.community_id = ae.community_id; 

  /* Descriptive variable 'Share of on-net calls among calls made'
   * -------------------------------------------------------------
   * fraction (# voice_calls+SMSses made during the source period 
   * to on-net subscribers)/(# voice_calls+SMSses made during the 
   * source period)
   */
 
  INSERT INTO tmp.descriptive_variables_subscriber_matrix_base_2c (
    mod_job_id, 
    alias_id, 
    share_of_onnet_made
  )
  SELECT 
    in_mod_job_id,
    alias_id, 
    CASE 
      WHEN (coalesce(n_calls_innet,0) + coalesce(n_calls_outnet,0)) > 0 
        THEN coalesce(n_calls_innet,0) / (coalesce(n_calls_innet,0) + coalesce(n_calls_outnet,0))::double precision
      ELSE 0
    END AS share_of_onnet_made
  FROM (
    SELECT
      alias_id, 
      sum(CASE WHEN in_out_network = 1 THEN call_count ELSE NULL END) AS n_calls_innet,
      sum(CASE WHEN in_out_network = 0 THEN call_count ELSE NULL END) AS n_calls_outnet
    FROM (
      SELECT
        sl.alias_id,
        ia.in_out_network,
        ia.voicecount + ia.smscount AS call_count
      FROM tmp.descriptive_variables_subscriberlist AS sl 
      INNER JOIN data.in_split_aggregates as ia
      ON sl.alias_id = ia.alias_id
      WHERE ia.period >= core.date_to_yyyyww(t1) 
        AND ia.period < core.date_to_yyyyww(t2)
    ) AS p
    GROUP BY p.alias_id
  ) aa;

  /* Descriptive variable 'Share of on-net calls among calls received'
   * -----------------------------------------------------------------
   * fraction (# voice_calls+SMSses received during the source period 
   * FROM on-net subscribers)/(# voice_calls+SMSses received during 
   * the source period)
   */
 
  INSERT INTO tmp.descriptive_variables_subscriber_matrix_base_2d (
    mod_job_id, 
    alias_id, 
    share_of_onnet_rec
  )
  SELECT 
    in_mod_job_id,
    alias_id, 
    CASE 
      WHEN (coalesce(n_calls_innet,0) + coalesce(n_calls_outnet,0)) > 0 
        THEN coalesce(n_calls_innet,0) / (coalesce(n_calls_innet,0) + coalesce(n_calls_outnet,0))::double precision
      ELSE 0
    END AS share_of_onnet_rec
  FROM (
    SELECT
      alias_id, 
      sum(CASE WHEN in_out_network = 1 THEN call_count ELSE NULL END) AS n_calls_innet,
      sum(CASE WHEN in_out_network = 0 THEN call_count ELSE NULL END) AS n_calls_outnet
    FROM (
      SELECT
        sl.alias_id,
        ia.in_out_network,
        ia.rc_voicecount + ia.rc_smscount AS call_count
      FROM tmp.descriptive_variables_subscriberlist AS sl 
      INNER JOIN data.in_split_aggregates as ia
      ON sl.alias_id = ia.alias_id
      WHERE ia.period >= core.date_to_yyyyww(t1) 
        AND ia.period < core.date_to_yyyyww(t2)
    ) AS p
    GROUP BY p.alias_id
  ) aa;

  RETURN retval;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.calculate_descriptive_variables_base_2(integer, integer) OWNER TO xsl;

---------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.calculate_descriptive_variables_topup(in_mod_job_id integer)
  RETURNS integer AS
$BODY$
/* -- Copyright (c) 2011 Xtract Oy. All rights reserved --
 *         -- Use is subject to license terms --
 * 
 * 
 * SUMMARY
 * Supplementary function for the descriptive variables. This function 
 * is called FROM the function 'work.calculate_descriptive_variables'.
 * 
 * INPUT
 * module job identifier
 * 
 * OUTPUT
 * integer: -1 when the is something wrong, else the module job id
 *
 * VERSION
 * 2.1.2013 KL bug 545 (ICIF-37), ICIF-56, ICIF-77
 * 24.10.2011 LBe 
 */
DECLARE
 retval integer = 1;
 core_job_id integer := -1 ;
 net_job_id integer := -1 ;
 num_weeks integer := 1 ;
 t1 date;
 t2 date;
BEGIN
 SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t1
 FROM work.module_job_parameters AS mjp
 WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't1';

 SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t2
 FROM work.module_job_parameters AS mjp
 WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2';

 num_weeks:=floor((t2-t1)/7);

  IF (num_weeks IS NULL OR num_weeks = 0) THEN 
    SELECT 1 INTO num_weeks; 
  END IF;

  INSERT INTO tmp.descriptive_variables_subscriber_matrix_topup (
    mod_job_id, 
    alias_id,
    topup_count,
    topup_amount,
    topup_days_from_last,
    topup_typical,
    topup_shareof_typical,
    topup_count_firsthalf_lastmonth,
    topup_count_lasthalf_lastmonth,
    topup_count_day,
    topup_count_eve,
    topup_count_night,
    topup_count_weekday,
    topup_count_weekend ) 
  SELECT 
    in_mod_job_id                                                                         AS mod_job_id,
    aa.alias_id                                                                           AS alias_id,
    aa.topup_number                                                                       AS topup_count,
    aa.topup_avg                                                                          AS topup_amount,
    aa.num_days_last_topup                                                                AS topup_days_from_last,
    aa.typical_topup                                                                      AS topup_typical,
    aa.num_typical_topup::double precision / GREATEST(aa.topup_number, 1)                 AS topup_shareof_typical,
    aa.num_topups_first_half_last_month                                                   AS topup_count_firsthalf_lastmonth,
    aa.num_topups_last_half_last_month                                                    AS topup_count_lasthalf_lastmonth,
    aa.topup_daytime::double precision / num_weeks                                        AS topup_count_day,
    aa.topup_evening::double precision / num_weeks                                        AS topup_count_eve,
    (aa.topup_number - aa.topup_daytime - aa.topup_evening)::double precision / num_weeks AS topup_count_night,
    aa.topup_weekday::double precision / num_weeks                                        AS topup_count_weekday,
    (aa.topup_number - aa.topup_weekday)::double precision / num_weeks                    AS topup_count_weekend 
  FROM (
    SELECT *
    FROM (
      SELECT
        a.alias_id,
        -- Total number of top-ups during the source period:
        COALESCE(SUM(CASE WHEN b.credit_amount > 0 AND b.timestamp >= t1 THEN 1 ELSE 0 END),0)::double precision AS topup_number,
        -- Average top-up amount during the source period:
        COALESCE(SUM(CASE WHEN b.timestamp >= t1 THEN b.credit_amount ELSE 0 END),0)::double precision / 
          GREATEST(SUM(CASE WHEN b.timestamp >= t1 AND b.credit_amount > 0 THEN 1 ELSE 0 END),1) AS topup_avg,
        -- Number of days between the last top-up and t2:
        MIN(t2 - b.timestamp::date) AS num_days_last_topup,
        -- Typical topup amount during the source period:
        AVG(CASE WHEN c.row_number = 1 THEN c.credit_amount ELSE NULL END) AS typical_topup,  
        -- Number of topup's with the typical topup amount during the source period:
        AVG(CASE WHEN c.row_number = 1 THEN c.count ELSE NULL END)::double precision AS num_typical_topup,
        -- Number of topup's during days 1-15 of last (whole) month before t2
        SUM(CASE WHEN b.credit_amount > 0 AND b.timestamp >= date_trunc('month',(SELECT t2 - interval '1 month'))::date 
          AND b.timestamp < (SELECT date_trunc('month',(SELECT t2 - interval '1 month'))::date + interval '15 days')::date
          THEN 1 ELSE 0 END) AS num_topups_first_half_last_month,
        -- Number of topup's during days 16-end of last (whole) month before t2
        SUM(CASE WHEN b.credit_amount > 0 AND b.timestamp < date_trunc('month',t2)::date 
          AND b.timestamp >= (SELECT date_trunc('month',(SELECT t2 - interval '1 month'))::date + interval '15 days')::date
          THEN 1 ELSE 0 END) AS num_topups_last_half_last_month,
        -- Number of topups during the source period during daytime, 
        SUM(CASE WHEN b.credit_amount > 0 AND b.timestamp >= t1 
          AND b.timestamp::time >= '07:00:00' AND b.timestamp::time < '17:00:00'
          THEN 1 ELSE 0 END)::double precision AS topup_daytime,  
        -- Number of topups during the source period during evenings, 
        SUM(CASE WHEN b.credit_amount > 0 AND b.timestamp >= t1 
          AND b.timestamp::time >= '17:00:00' AND b.timestamp::time < '22:00:00'
          THEN 1 ELSE 0 END)::double precision AS topup_evening, 
        -- Number of topups during weekdays:
        SUM(CASE WHEN b.credit_amount > 0 AND b.timestamp >= t1
          AND timestamp < (date_trunc('week', timestamp) + interval '4 days' + interval '17 hours')
          THEN 1 ELSE 0 END) AS topup_weekday 
      FROM tmp.descriptive_variables_subscriberlist AS a
      LEFT JOIN ( 
        SELECT * FROM data.topup WHERE timestamp < t2
      ) AS b
      ON a.alias_id = b.charged_id
      LEFT JOIN ( -- DV's related to the 'typical' (most frequent) topup amount during the source period
        SELECT 
          charged_id, 
          credit_amount, 
          COUNT(*) AS count,
          row_number() over (PARTITION BY charged_id ORDER BY count(*) DESC, credit_amount DESC)
        FROM data.topup 
        WHERE timestamp >= t1 AND timestamp < t2
        GROUP BY charged_id, credit_amount
      ) AS c
      ON a.alias_id = c.charged_id
      WHERE c.row_number = 1
      GROUP BY a.alias_id
    ) b
  ) aa;

  RETURN in_mod_job_id;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.calculate_descriptive_variables_topup(integer) OWNER TO xsl;

---------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.descriptive_variables_to_matrix(in_mod_job_id integer)
  RETURNS SETOF text AS
$BODY$
DECLARE
  n INTEGER := 1;
  k INTEGER;
  k_min INTEGER;
  k_max INTEGER;
  sql TEXT;
  base_sql TEXT;
  final_sql TEXT;
  var_sql TEXT;
  in_aliases_array INTEGER[];
  in_var_name text;
  in_var_type TEXT;
  batch_size INTEGER := 1000000;
BEGIN
  SELECT core.ARRAY_SORT(ARRAY(
   SELECT DISTINCT alias_id
   FROM work.descriptive_variables_subscriber
   WHERE mod_job_id = in_mod_job_id
  )) INTO in_aliases_array;

  -- Looping over the batches
  WHILE n <= 1 + ARRAY_UPPER(in_aliases_array, 1)::double precision/batch_size::double precision
  LOOP    
    base_sql := 'INSERT INTO results.descriptive_variables_subscriber_matrix (mod_job_id, alias_id, ';
    sql := '';
    
    -- Looping through this batch's variables
    -- NOTE: No n should be inputed so that k_min is outside the array bounds
    k_min := 1 + (n - 1)*batch_size;
    k_max := CASE WHEN n*batch_size <= ARRAY_UPPER(in_aliases_array, 1) THEN n*batch_size ELSE ARRAY_UPPER(in_aliases_array, 1) END;
    
    FOR in_var_name IN (SELECT DISTINCT var_name 
                      FROM work.descriptive_variables_subscriber aa, work.descriptive_variables_list bb
                      WHERE mod_job_id = in_mod_job_id
                      AND aa.var_id = bb.var_id
                      ORDER BY var_name ASC)
    LOOP
      
      -- Get the data type of this variable
      SELECT 'double precision' INTO in_var_type;
      
      IF (in_var_name = 'social_role') THEN 
        SELECT 'text' INTO in_var_type;
      END IF;

      IF (in_var_name = 'community_id') THEN 
        SELECT 'integer' INTO in_var_type;
      END IF;
      
      -- Appending the variable name to the base sql
      base_sql := base_sql || '"' || in_var_name || '", ';
      
      -- Forming the sql for aggregating the variable to matrix format
      var_sql := 'MAX(CASE WHEN var_name = ''' || in_var_name || ''' THEN value ELSE NULL END)::' || in_var_type || ' AS "' || in_var_name || '", ';
      -- Appending to the sql
      sql := sql || var_sql;
      
    END LOOP;
    
    -- Parsing the sqls together
    base_sql := TRIM(TRAILING ', ' FROM base_sql) || ')' || 'SELECT mod_job_id, alias_id, ';
    sql := TRIM(TRAILING ', ' FROM sql) || ' ' || '
           FROM work.descriptive_variables_subscriber aa, work.descriptive_variables_list bb
           WHERE mod_job_id = ' || in_mod_job_id || ' AND 
           alias_id >= ' || in_aliases_array[k_min] || ' AND 
           alias_id <= ' || in_aliases_array[k_max] || ' AND aa.var_id = bb.var_id
           GROUP BY mod_job_id, alias_id'; -- Including clause for this batch's aliases.
    final_sql := base_sql || sql;
    
    -- Query ready, execute it
    EXECUTE(final_sql);
    
    -- Inserting new batch finished, analyze the target table
    PERFORM core.analyze('results.descriptive_variables_subscriber_matrix', in_mod_job_id);
    
    -- Return the executed sqls for debugging
    RETURN NEXT final_sql;
    
    -- Increment the batch
    n := n + 1;
    
  END LOOP;

  SELECT '
    INSERT INTO results.descriptive_variables_community_matrix (mod_job_id, community_id, "Communication density", "Intracommunication ratio","Linkedness within community")
    SELECT mod_job_id, community_id, 
      MAX(CASE WHEN var_name = ''Communication density'' THEN value::double precision ELSE NULL END) AS "Communication density",
      MAX(CASE WHEN var_name = ''Intracommunication ratio'' THEN value::double precision ELSE NULL END) AS "Intracommunication ratio",
      MAX(CASE WHEN var_name = ''Linkedness within community'' THEN value::double precision ELSE NULL END) AS "Linkedness within community"
    FROM work.descriptive_variables_community aa, work.descriptive_variables_list bb
    WHERE mod_job_id = ' || in_mod_job_id || ' and aa.var_id = bb.var_id
    GROUP BY mod_job_id, community_id
  ' into final_sql;

  -- Query ready, execute it
  EXECUTE(final_sql);

  return next final_sql;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.descriptive_variables_to_matrix(integer) OWNER TO xsl;

---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION work.calculate_network_roles_tmp_network(integer)
  RETURNS integer AS
$BODY$
/* Version
 * 2.1.2013 KL corrected a bug: maximum is taken now from numeric value not from text
 * <4.12.2012 Someone
 */   
DECLARE
  xsljobid ALIAS for $1;
  klimit integer; 
  klimitpos integer;  
  wlimit double precision;
  wlimitpos integer;
BEGIN
  DELETE FROM work.network_roles 
  WHERE job_id = xsljobid;
  
  SELECT (count(*)*0.1 )::integer INTO klimitpos 
  FROM work.out_scores 
  WHERE job_id = xsljobid;

  SELECT (count(*)*0.9)::integer INTO wlimitpos 
  FROM tmp.out_network_raw;

  SELECT min(k) INTO klimit 
  FROM (
    SELECT k 
    FROM work.out_scores 
    WHERE job_id = xsljobid 
    ORDER BY k DESC 
    LIMIT klimitpos
  ) aa;

  SELECT max(weight) INTO wlimit 
  FROM (
    SELECT weight 
    FROM tmp.out_network_raw 
    ORDER BY weight
    LIMIT wlimitpos
  ) aa;

  TRUNCATE tmp.nwr_linkcount;
  TRUNCATE tmp.nwr_linkcount_tmp;
  TRUNCATE tmp.hublist;
  TRUNCATE tmp.bridgelist;
  TRUNCATE tmp.network_roles_fix;

  INSERT INTO tmp.nwr_linkcount_tmp (alias_id, lc) 
  SELECT 
    alias_a, 
    CASE WHEN weight > wlimit THEN 1 ELSE 0 END 
  FROM tmp.out_network_raw;

  INSERT INTO tmp.nwr_linkcount_tmp (alias_id, lc) 
  SELECT 
    alias_b, 
    CASE WHEN weight > wlimit THEN 1 ELSE 0 END 
  FROM tmp.out_network_raw;
 
  INSERT INTO tmp.nwr_linkcount (alias_id, lc) 
  SELECT alias_id, sum(lc) 
  FROM tmp.nwr_linkcount_tmp 
  GROUP BY alias_id;

  INSERT INTO tmp.hublist 
  SELECT alias_id 
  FROM (
    SELECT aa.alias_id 
    FROM tmp.out_network_raw oo 
    JOIN tmp.nwr_linkcount aa ON aa.alias_id = oo.alias_a 
    JOIN tmp.nwr_linkcount bb ON bb.alias_id = oo.alias_b 
    WHERE aa.lc > klimit AND bb.lc <= klimit
    UNION 
    SELECT bb.alias_id 
    FROM tmp.out_network_raw oo
    JOIN tmp.nwr_linkcount aa ON aa.alias_id = oo.alias_a 
    JOIN tmp.nwr_linkcount bb ON bb.alias_id = oo.alias_b
    WHERE aa.lc <= klimit AND bb.lc > klimit
  ) bb
  GROUP BY bb.alias_id;
  
  INSERT INTO tmp.network_roles_fix (job_id,alias_id, ishub, isbridge, isoutlier)
  SELECT 
    xsljobid,
    aa.alias_id,
    CASE WHEN aa.lc > klimit THEN 1 ELSE 0 END AS ishub,
    CASE WHEN bb.sumk > 1 THEN 1 ELSE 0 END AS isbridge,
    CASE WHEN aa.lc < 2 THEN 1 ELSE 0 END AS isoutlier
  FROM tmp.nwr_linkcount aa, (
    SELECT alias_id, sum(sumk) AS sumk 
    FROM (
      SELECT dd.alias_a AS alias_id, count(*) AS sumk  
      FROM tmp.hublist cc 
      JOIN tmp.out_network_raw dd ON dd.alias_b = cc.alias_id
      GROUP BY dd.alias_a 
      UNION ALL
      SELECT dd.alias_b AS alias_id, count(*) AS sumk  
      FROM tmp.hublist cc
      JOIN tmp.out_network_raw dd ON dd.alias_a = alias_id 
      GROUP BY dd.alias_b 
    ) cc 
    GROUP BY alias_id
  ) bb 
  WHERE aa.alias_id = bb.alias_id;

  INSERT INTO tmp.bridgelist
  SELECT alias_id 
  FROM (
    SELECT alias_id 
    FROM (
      SELECT cc.alias_id, oo1.alias_b, oo2.alias_a 
      FROM tmp.network_roles_fix cc 
      JOIN tmp.out_network_raw oo1 ON oo1.alias_a = cc.alias_id    
      JOIN tmp.network_roles_fix cc1 ON cc1.alias_id = oo1.alias_b 
      JOIN tmp.out_network_raw oo2 ON oo2.alias_b = cc.alias_id 
      JOIN tmp.network_roles_fix cc2 ON cc2.alias_id = oo2.alias_a 
      WHERE cc.isbridge=1 AND cc.ishub=0 AND cc1.ishub=1 AND cc2.ishub=1 AND cc1.alias_id <> cc2.alias_id 
      AND cc.job_id = xsljobid AND oo1.job_id = xsljobid AND oo2.job_id = xsljobid
    ) x1
    LEFT OUTER JOIN tmp.out_network_raw oo ON (oo.alias_a = x1.alias_a AND oo.alias_b = x1.alias_b) 
    WHERE oo.alias_a IS NULL 
    UNION 
    SELECT alias_id 
    FROM ( 
      SELECT cc.alias_id, oo1.alias_b, oo2.alias_a 
      FROM tmp.network_roles_fix cc 
      JOIN tmp.out_network_raw oo1 ON oo1.alias_a = cc.alias_id 
      JOIN tmp.network_roles_fix cc1 ON cc1.alias_id = oo1.alias_b 
      JOIN tmp.out_network_raw oo2 ON oo2.alias_b = cc.alias_id 
      JOIN tmp.network_roles_fix cc2 ON cc2.alias_id = oo2.alias_a 
      WHERE cc.isbridge=1 AND cc.ishub=0 AND cc1.alias_id <> cc2.alias_id AND cc1.ishub=1 AND cc2.ishub=1 AND cc.job_id = xsljobid
    ) x2
    LEFT OUTER JOIN tmp.out_network_raw oo ON (oo.alias_b = x2.alias_a AND oo.alias_a = x2.alias_b) 
    WHERE oo.alias_a IS NULL
  ) x0
  GROUP BY alias_id;

  INSERT INTO work.network_roles (job_id,alias_id, ishub, isbridge, isoutlier)
  SELECT 
    xsljobid, aa.alias_id, aa.ishub,
    CASE WHEN bb.alias_id IS NOT NULL THEN 1 ELSE 0 END AS isbridge,
    aa.isoutlier
  FROM tmp.network_roles_fix aa 
  LEFT OUTER JOIN tmp.bridgelist bb ON aa.alias_id = bb.alias_id;

  RETURN wlimit::integer;
  
END; 
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.calculate_network_roles_tmp_network(integer) OWNER TO xsl;

----------------------------

CREATE OR REPLACE FUNCTION work.add_descriptive_variables_values_to_chart_tables(in_mod_job_id integer, target_list_mod_job_id integer)
RETURNS VOID AS 
$BODY$
/*
 * SUMMARY
 * 
 * Adds the values (averages) of descriptive variables needed for charts at the end of descriptive variables workflow
 * This is the function for churn inactivity!
 * 
 * VERSION
 * 30.07.2014 JVi Fixed bug where wrong mod job id was used for monthly arpu retrieval in some cases
 * 30.07.2014 JVi Modifications related to fix done for ICIF-217 (removed reference to table work.descriptive_variables_subscriber)
 * 17.06.2014 JVi Added support for ZDP (otherwise would crash if only ZDP was included in run)
 * 20.06.2013 AVe When calculating All/Mature/New diff factors, audience condition is calculated using use cases into account 
 * 18.04.2013 HMa ICIF-126
 * 22.02.2013 HMa Made the descriptive variables calculation dynamic so that all the variables found in table work.differentiating_factors are calculated. 
 * 18.02.2013 MOj ICIF-107
 * 04.02.2013 KL
 */

DECLARE

  t2 date;
  marpu_mod_job_id integer;
  tcrm date;
  i integer;
  query text;
  query_all text := '';
  query_new text := '';
  query_mature text := '';
  this_long_name text;
  this_var_name text;
  target_t2 date;
  products text;
  product_arr text[];
  p_id integer; 
  this_product_id text;
  query_product text = '';
  audience_text text;
  job_use_cases text;
  use_cases text[];
  use_case text;

BEGIN


  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO t2
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = in_mod_job_id AND mjp.key = 't2';

  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO tcrm
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = in_mod_job_id AND lower(mjp.key) = 'tcrm';
  
  SELECT value INTO products
  FROM work.module_job_parameters
  WHERE key='uc_product_products'
  AND mod_job_id=target_list_mod_job_id;
  
  SELECT to_date(mjp.value, 'YYYY-MM-DD') INTO target_t2
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = target_list_mod_job_id AND mjp.key = 't2';
  
  SELECT value INTO job_use_cases
  FROM work.module_job_parameters AS mjp
  WHERE mjp.mod_job_id = target_list_mod_job_id AND mjp.key = 'job_use_cases';

  -- For monthly ARPU, use data that has same t2 as descvar run and same mod job id as 
  -- either descvar run or target list run
  SELECT MAX(mjp.mod_job_id) INTO marpu_mod_job_id
  FROM work.module_job_parameters AS mjp
  INNER JOIN ( 
    SELECT DISTINCT(mod_job_id) FROM work.monthly_arpu
    WHERE mod_job_id IN (in_mod_job_id, target_list_mod_job_id)
  ) ma
  USING (mod_job_id)
  WHERE mjp.key = 't2' AND mjp.value::date = t2;

  query_all := '';
  query_new := '';
  query_mature := '';
  audience_text := '(';

  use_cases := string_to_array(trim(job_use_cases),',');
  
  FOR uc_ind IN 1..array_upper(use_cases, 1) LOOP
    
    use_case := use_cases[uc_ind];
    IF use_case ~ 'churn' THEN
      audience_text := audience_text || ' c.audience_' || use_case || '=1 OR';
    END IF;
    IF use_case ~ 'product' THEN
      audience_text := audience_text || ' c.audience_' || use_case || '=1 OR';
    END IF;
    IF use_case ~ 'zero_day_prediction' THEN
      audience_text := audience_text || ' c.audience_' || use_case || '=1 OR';
    END IF;
  
  END LOOP;
  audience_text := trim(TRAILING ' OR' FROM audience_text);
  audience_text := audience_text || ')';
  
  FOR i IN
    SELECT var_id FROM work.differentiating_factors
  LOOP

    this_long_name := long_name FROM work.differentiating_factors WHERE var_id = i;
    this_var_name  := var_name  FROM work.differentiating_factors WHERE var_id = i;


    IF this_long_name = 'Typical top-up amount' THEN
      query_all    := query_all    || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||') '; -- no coalesce for typical top-up amount (because no top-up is not amount 0)
      query_new    := query_new    || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||' * new_1) ';
      query_mature := query_mature || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||' * mature) ';
    ELSE
      query_all    := query_all    || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0)) ';
      query_new    := query_new    || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0) * new_1) ';
      query_mature := query_mature || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0) * mature) ';
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
      WHEN u.gr = ''ALL'' THEN bb.average_all
      WHEN u.gr = ''NEW'' THEN bb.average_new_1
      WHEN u.gr = ''MATURE'' THEN bb.average_mature
      ELSE NULL
    END AS var_value,
    df.order_id
  FROM (
    SELECT
      CASE 
        '||query_all||'
      ELSE NULL END AS average_all, 
      CASE 
        '||query_new||'
      ELSE NULL END AS average_new_1, 
      CASE 
        '||query_mature||'
      ELSE NULL END AS average_mature, 
      long_name AS var_name
    FROM (
      SELECT 
        af.*,
        keys.long_name,
        keys.var_name,
        CASE WHEN b.switch_on_date>'''||t2||'''::date-30 THEN 1 
          ELSE NULL END AS new_1, 
        CASE WHEN b.switch_on_date<'''||t2||'''::date-90 THEN 1 
          ELSE NULL END AS mature 
      FROM results.descriptive_variables_subscriber_matrix AS af
      INNER JOIN work.module_targets c 
      ON  af.alias_id=c.alias_id
      INNER JOIN data.in_crm b 
      ON af.alias_id=b.alias_id 
      CROSS JOIN
      (SELECT long_name, var_name FROM work.differentiating_factors) AS keys
      WHERE af.mod_job_id = '||in_mod_job_id||'
      AND c.mod_job_id = '||target_list_mod_job_id||'
      AND '|| audience_text ||'
      AND b.switch_on_date IS NOT NULL
      AND b.date_inserted = '''||tcrm||'''::date
    ) aa
    GROUP BY long_name
  ) bb
  INNER JOIN work.differentiating_factors df
  ON bb.var_name = df.long_name
  INNER JOIN (
    SELECT ''MATURE'' AS gr UNION 
    SELECT ''NEW'' AS gr UNION 
    SELECT ''ALL'' AS gr
  ) AS u
  ON (u.gr = ''MATURE'' AND bb.average_mature IS NOT NULL)
  OR (u.gr = ''NEW'' AND bb.average_new_1 IS NOT NULL)
  OR (u.gr = ''ALL'' AND bb.average_all IS NOT NULL);';

  EXECUTE query;
  IF products IS NOT NULL THEN 
  
    FOR i IN
      SELECT var_id FROM work.differentiating_factors
    LOOP

      this_long_name := long_name FROM work.differentiating_factors WHERE var_id = i;
      this_var_name  := var_name  FROM work.differentiating_factors WHERE var_id = i;


      IF this_long_name = 'Typical top-up amount' THEN
        query_product:= query_product || 'WHEN long_name = '''||this_long_name||''' THEN avg('||this_var_name||' * has_product) '; -- no coalesce for typical top-up amount (because no top-up is not amount 0)
      ELSE
        query_product:= query_product || 'WHEN long_name = '''||this_long_name||''' THEN avg(coalesce('||this_var_name||', 0) * has_product) ';
      END IF;

    END LOOP;
    
    product_arr := string_to_array(products,',');

    -- Loop over products
    FOR p_id IN 1..array_upper(product_arr,1) LOOP

      this_product_id := product_arr[p_id];
      
      query := '
      INSERT INTO charts.chart_data(mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
      SELECT
        '||in_mod_job_id||' AS mod_job_id,
        df.stat_name,
        pi.product_name AS group_name, 
        '''||t2||'''::date AS data_date,
        bb.var_name,
        bb.var_value,
        df.order_id
      FROM (
        SELECT
          CASE 
            '||query_product||'
          ELSE NULL END AS var_value, 
          long_name AS var_name
        FROM (
          SELECT 
            af.*,
            keys.long_name,
            keys.var_name,
            CASE WHEN p.product_taken_date IS NOT NULL AND p.product_id = '''|| this_product_id ||''' AND (p.product_churn_date IS NULL OR p.product_churn_date >= '''||target_t2||'''::date-7) THEN 1 -- Has the product at some point during the last week of the source period (latest data if in_mod_job_id corresponds to apply period)
            ELSE NULL
            END AS has_product
          FROM results.descriptive_variables_subscriber_matrix AS af
          INNER JOIN work.module_targets c 
          ON  af.alias_id=c.alias_id
          LEFT JOIN (SELECT * FROM data.product WHERE date_inserted = '''||target_t2||'''::date - 1) AS p
          ON c.alias_id = p.alias_id
          CROSS JOIN
          (SELECT long_name, var_name FROM work.differentiating_factors) AS keys
          WHERE af.mod_job_id = '||in_mod_job_id||'
          AND c.mod_job_id = '||target_list_mod_job_id||'
        ) aa
        GROUP BY long_name
      ) bb
      INNER JOIN work.differentiating_factors df
      ON bb.var_name = df.long_name
      INNER JOIN data.product_information pi
      ON pi.product_id = '''|| this_product_id ||''';';
    
      EXECUTE query; 
      
      
    END LOOP;  
  END IF;

    INSERT INTO charts.chart_data 
    SELECT  in_mod_job_id, hu.stat_name, 'ALL', current_date, hu.value_class,
      COALESCE(var_value, 0), ord
    FROM (
      SELECT stat_name, value_class, count(*) as var_value
      FROM(
        SELECT   stat_name,   (CASE WHEN stat_name='SUBS_ARPU' THEN value_class_arpu 
           WHEN stat_name='SUBS_SOC_REVENUE' THEN value_class_sr
           END) as value_class
        FROM (
          SELECT arpu, sr,
            (CASE WHEN sr<=5 and sr>=0 THEN '0-5'
               WHEN sr<=10 THEN '5-10'
               WHEN sr<=15 THEN '10-15'
               WHEN sr<=20 THEN '15-20'
               WHEN sr<=30 THEN '20-30'
               WHEN sr<=40 THEN '30-40'
               WHEN sr<=50 THEN '40-50'
               WHEN sr<=75 THEN '50-75'
               WHEN sr<=100 THEN '75-100'
               WHEN sr<=150 THEN '100-150'
               WHEN sr>150 THEN 'over 150'
             END) as value_class_sr,
            (CASE WHEN arpu<=5 and arpu>=0 THEN '0-5'
               WHEN arpu<=10 THEN '5-10'
               WHEN arpu<=15 THEN '10-15'
               WHEN arpu<=20 THEN '15-20'
               WHEN arpu<=30 THEN '20-30'
               WHEN arpu<=40 THEN '30-40'
               WHEN arpu<=50 THEN '40-50'
               WHEN arpu<=75 THEN '50-75'
               WHEN arpu<=100 THEN '75-100'
               WHEN arpu<=150 THEN '100-150'
               WHEN arpu>150 THEN 'over 150'
             END) as value_class_arpu        
          FROM(
            SELECT  
              a.alias_id AS alias_id,
	      a.monthly_arpu AS arpu, 
              b.social_revenue AS sr
            FROM work.monthly_arpu a
            RIGHT JOIN results.descriptive_variables_subscriber_matrix b
            ON a.alias_id = b.alias_id
            WHERE  b.mod_job_id = in_mod_job_id
            AND a.mod_job_id= marpu_mod_job_id
            GROUP BY a.alias_id, a.monthly_arpu, b.social_revenue 
          ) d 
        ) g
        CROSS JOIN (
          SELECT 'SUBS_SOC_REVENUE' as stat_name UNION SELECT 'SUBS_ARPU' as stat_name
         ) k  
        WHERE (value_class_sr IS NOT NULL AND stat_name='SUBS_SOC_REVENUE') OR
          (value_class_arpu IS NOT NULL AND stat_name='SUBS_ARPU') 
        ) j
      GROUP BY stat_name, value_class
      ) ji
      RIGHT OUTER JOIN ((
         SELECT  '0-5' as value_class, 1 as ord
         UNION
         SELECT  '5-10' as value_class, 2 as ord
         UNION
         SELECT  '10-15' as value_class, 3  as ord 
         UNION
         SELECT  '15-20' as value_class,  4 as ord 
         UNION
         SELECT  '20-30' as value_class,  5 as ord 
         UNION
         SELECT  '30-40' as value_class,  6 as ord 
         UNION
         SELECT  '40-50' as value_class,  7 as ord 
         UNION
         SELECT  '50-75' as value_class,  8 as ord 
         UNION
         SELECT  '75-100' as value_class,  9 as ord 
         UNION
         SELECT  '100-150' as value_class,  10 as ord 
         UNION
         SELECT  'over 150' as value_class,  11 as ord 
      ) ui
      CROSS JOIN 
        (SELECT 'SUBS_SOC_REVENUE' as stat_name UNION SELECT 'SUBS_ARPU' as stat_name) ti
    ) hu 
    ON hu.value_class=ji.value_class AND hu.stat_name=ji.stat_name;


  -- social connectivity score 
  INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id) 
  SELECT target_list_mod_job_id AS mod_job_id, 
    'SCORE_DISTRIBUTION' AS stat_name,
    'sna' AS group_name, 
    now() AS data_date,
    var_name, 
    var_value, 
    NULL AS order_id
    FROM
    (SELECT 
      ROUND(social_connectivity_score::numeric-(0.005),1)::text AS var_name,
      count(*) AS var_value
      FROM results.descriptive_variables_subscriber_matrix
      WHERE mod_job_id = in_mod_job_id
      GROUP BY var_name
    ) aa;	

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.add_descriptive_variables_values_to_chart_tables(integer, integer) OWNER TO xsl;

------------------------------------------------------------------------------------------------------------

-- Function: work.create_descriptive_variable_score_distribution_charts(integer, text)

-- DROP FUNCTION work.create_descriptive_variable_score_distribution_charts(integer, text);

CREATE OR REPLACE FUNCTION work.create_descriptive_variable_score_distribution_charts(in_mod_job_id integer, in_use_case_score_names text)
RETURNS text AS 
$BODY$
/*
 * SUMMARY
 * 
 * Calculates the distributions of descriptive variables as a function of use case propensity score and
 * stores them in charts.chart_data. Checks if propensity scores are available in results.module_results 
 * for current mod job id, distributions are not calculated for missing scores. Returns query used to 
 * insert into charts.chart_data descvar distributions as a function of the last propensity score provided.
 *
 * INPUTS
 *
 * in_mod_job_id:       Mod job id of current run
 * use_case_score_name: Comma-separated list of propensity score fields in results.module_results table, 
 *                      e.g. 'churn_inactivity_propensity_score,churn_postpaid_propensity_score' etc.
 * 
 * VERSION
 * 2013-10-15 JVi: Created
 */

DECLARE

  t2 date;
  descvar_mod_job_id integer;

  i integer;
  j integer;
  this_use_case_score_names text;
  use_case_score_name text;
  row_count_variable integer;
  query_header text;
  query_score_part text;
  query_descvar_part text;
  query text;
  loop_names record;
  var_name_i text;

  var_name_array text[];

BEGIN
  
  SELECT value::date INTO t2 
  FROM work.module_job_parameters
  WHERE mod_job_id = in_mod_job_id
  AND   "key" = 't2';
  
  SELECT value::integer INTO descvar_mod_job_id
  FROM       work.module_job_parameters
  WHERE mod_job_id = in_mod_job_id
  AND   "key" = 'descvar_mod_job_id';

  IF NOT descvar_mod_job_id > -1 THEN
    RAISE NOTICE 'No descvar data found, not calculating descvar distributions.';
    RETURN NULL;
  END IF;

  this_use_case_score_names = regexp_replace(in_use_case_score_names, E'\n', '', 'g');
  this_use_case_score_names = regexp_replace(this_use_case_score_names, ' ', '', 'g');

  FOR i IN 1..array_upper(regexp_split_to_array(this_use_case_score_names, ','),1) LOOP

    -- Get next use case score name from input
    use_case_score_name = trim(both from split_part(this_use_case_score_names, ',', i));

    -- Make sure that there are scores calculated for given score name in this run
    query := 'SELECT COUNT('|| use_case_score_name ||') FROM results.module_results WHERE mod_job_id = '||in_mod_job_id||';';
    EXECUTE query INTO row_count_variable;
    
    IF row_count_variable IS NOT NULL THEN

      -- Prepare query to insert chart data
      query_header := '
    INSERT INTO charts.chart_data (mod_job_id, stat_name, group_name, data_date, var_name, var_value, order_id)
    SELECT
      ' || in_mod_job_id || ' AS mod_job_id,
      ''DESC_VAR_VS_SCORE_DISTRIBUTION'' AS stat_name,
      dvs.var_name || ''_vs_'' || ''' || use_case_score_name || ''' AS group_name,
      ''' || t2::text || '''::date AS data_date,
      dvs.long_name AS var_name,
      AVG(dvs.desc_var_value) AS var_value,
      mr.score_quantile AS order_id';

      -- Get score bins and descvar values
      query_score_part := '
      SELECT 
        alias_id,
        ' || use_case_score_name || ',
        ntile(100) OVER (ORDER BY ' || use_case_score_name || ' DESC) AS score_quantile
      FROM results.module_results
      WHERE mod_job_id = ' || in_mod_job_id || '
      AND   ' || use_case_score_name || ' IS NOT NULL';

      var_name_array := array_agg(var_name ORDER BY var_id) FROM work.descriptive_variables_list WHERE calculate_distributions > 0;

      -- Get values of descvars for which distribution makes sense - e.g. text-based
      -- descvars should have calculate_distributions = 0 in the descriptive_variables_list table
      -- Note - need to convert matrix to stack format

      query_descvar_part := '
      SELECT
        dvsm.alias_id,
        dvl.var_id,
        dvl.var_name,
        dvl.long_name,
        MAX(CASE ';
      
      FOR j IN 1..array_upper(var_name_array, 1) LOOP
      query_descvar_part := query_descvar_part || '
            WHEN dvl.var_name = ''' || var_name_array[j] || ''' THEN dvsm.' || var_name_array[j];
      END LOOP;

      query_descvar_part := query_descvar_part || '
          ELSE NULL END)::real AS desc_var_value
      FROM results.descriptive_variables_subscriber_matrix dvsm
      CROSS JOIN (
        SELECT * FROM work.descriptive_variables_list WHERE calculate_distributions > 0
      ) dvl
      WHERE dvsm.mod_job_id = ' || descvar_mod_job_id || '
      GROUP BY alias_id, var_id, var_name, long_name';
    
      -- Form and execute final query
      query := query_header || '
    FROM (' || query_score_part || '
    ) mr
    INNER JOIN (' || query_descvar_part || '
    ) dvs
    USING (alias_id)
    GROUP BY dvs.var_name, dvs.long_name, mr.score_quantile;';
        
      EXECUTE(query);

    END IF;
  END LOOP;
  
  PERFORM core.analyze('charts.chart_data', in_mod_job_id);
  
  RETURN query;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.create_descriptive_variable_score_distribution_charts(integer, text) OWNER TO xsl;


-- Function: work.initialize_descriptive_variables_initialization(integer)

-- DROP FUNCTION work.initialize_descriptive_variables_initialization(integer);


CREATE OR REPLACE FUNCTION work.initialize_descriptive_variables_initialization(in_mod_job_id integer)
  RETURNS SETOF text AS
$BODY$
/*
 * SUMMARY
 * 
 * Returns parameters for descriptive_variables_workflow initialization
 * 
 * VERSION
 * 23.07.2014 JVi: Implemented small fix related to ICIF-216
 * 22.07.2013 KL
 */

DECLARE

  t2 date;
  job_use_cases text;
  t2_last date;
  churn_inactivity_t7_1 date;
  churn_inactivity_t7 date;
  churn_inactivity_t6 date;
  churn_inactivity_t5 date;
  churn_inactivity_t4 date;
  churn_postpaid_t5_1 date;
  churn_postpaid_t5 date;
  churn_postpaid_t4 date;
  source_length integer;
  gap_length integer;
  target_length integer;
  target_eval_length integer;
  eval_length integer;
  pp_t5_t1_length integer;
  pp_t5_t2_length integer;
  pp_target_length integer;
  t1 date;
  tcrm date;
  initialization_needed boolean;
  use_case_list_desc_var text;
  mod_job_id_desc_var integer;
  include_inactivity boolean;
  include_postpaid boolean;
  include_ia_text text;
  include_pp_text text;
  min_mod_job_id integer;
  mod_job_id_tmp integer;
  calculate_targets boolean;
  calculate_targets_ia boolean;
  calculate_targets_pp boolean;
  calculate_targets_ia_text text;
  calculate_targets_pp_text text;
  job_type text;

BEGIN

  initialization_needed = false;
  calculate_targets = false;
  include_postpaid = false;
  include_inactivity = false;
  mod_job_id_desc_var = -1;
  include_ia_text = 'false';
  include_pp_text = 'false';
  calculate_targets_ia = false;
  calculate_targets_pp = false;
  calculate_targets_ia_text = 'false';
  calculate_targets_pp_text = 'false';

  SELECT value INTO t2_last
  FROM work.module_job_parameters
  WHERE key='t2'
  AND mod_job_id=in_mod_job_id;

  SELECT value INTO job_use_cases
  FROM work.module_job_parameters 
  WHERE mod_job_id = in_mod_job_id 
  AND key = 'job_use_cases';

  -- Read the use case names from job_use_cases:

  IF job_use_cases ~ 'churn_inactivity' THEN

    --check if there is descriptive_variables computed for churn_inactivity having same t7 as this t2(so churners are known already)

    SELECT max(a.mod_job_id) INTO mod_job_id_tmp
    FROM work.module_job_parameters a
    JOIN work.module_job_parameters b
    ON a.mod_job_id = b.mod_job_id
    JOIN work.module_job_parameters c
    ON a.mod_job_id=c.mod_job_id
    JOIN results.descriptive_variables_subscriber_matrix d --make sure desc var are computed indeed
    ON d.mod_job_id = a.mod_job_id
    WHERE a.value ~ 'descriptive_variables'
    AND a.key = 'job_type'
    AND b.key = 'job_use_cases'
    AND b.value ~ 'churn_inactivity'
    AND c.key = 'uc_churn_inactivity_t7'
    AND c.value = t2_last;


    IF mod_job_id_tmp IS NULL THEN 
      include_inactivity = true;
      include_ia_text = 'true';
    END IF; 

    -- check if targets need to be calculated (these will be computed anyways, if descriptive variables will be run

    IF NOT include_inactivity THEN 
      SELECT max(m2.mod_job_id) INTO mod_job_id_tmp
      FROM work.module_targets m1
      JOIN work.module_job_parameters m2
      ON m2.mod_job_id=m1.mod_job_id
      INNER JOIN work.module_job_parameters m3
      ON m3.mod_job_id=m1.mod_job_id
      WHERE m1.target_churn_inactivity IS NOT NULL
      AND m2.key='uc_churn_inactivity_t7'
      AND m2.value=t2_last
      AND m3.key='job_use_cases'
      AND m3.value ~ 'churn_inactivity';
    

      IF mod_job_id_tmp IS NULL THEN 
        calculate_targets_ia = true;
        calculate_targets_ia_text = 'true';
      END IF; 
    END IF;

    IF include_inactivity OR calculate_targets_ia THEN 
      
      churn_inactivity_t7 = t2_last;

      SELECT min(a.mod_job_id) INTO min_mod_job_id --job to check period lengths
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      JOIN work.module_job_parameters c
      ON a.mod_job_id=c.mod_job_id
      WHERE a.value = 'Predictors + Fit + Apply'
      AND a.key = 'run_type'
      AND b.key = 'job_use_cases'
      AND b.value ~ 'churn_inactivity'
      AND c.key = 'uc_churn_inactivity_t7';


      SELECT a.value::date-b.value::date INTO source_length  
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE a.mod_job_id = min_mod_job_id
      AND a.key = 't2'
      AND b.key = 't1';

      SELECT a.value::date-b.value::date INTO gap_length  
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE a.mod_job_id = min_mod_job_id
      AND a.key = 'uc_churn_inactivity_t4'
      AND b.key = 't2';

      SELECT a.value::date-b.value::date INTO target_length  
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE a.mod_job_id = min_mod_job_id
      AND a.key = 'uc_churn_inactivity_t5'
      AND b.key = 'uc_churn_inactivity_t4';

      SELECT a.value::date-b.value::date INTO target_eval_length  
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE a.mod_job_id = min_mod_job_id
      AND a.key = 'uc_churn_inactivity_t6' 
      AND b.key = 'uc_churn_inactivity_t5';

      SELECT a.value::date-b.value::date INTO eval_length  
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE a.mod_job_id = min_mod_job_id
      AND a.key = 'uc_churn_inactivity_t7'
      AND b.key = 'uc_churn_inactivity_t6';

      churn_inactivity_t6 = churn_inactivity_t7 - eval_length;
      churn_inactivity_t5 = churn_inactivity_t6 - target_eval_length;
      churn_inactivity_t4 = churn_inactivity_t5 - target_length;
      t2 = churn_inactivity_t4 - gap_length;        
      t1 = t2 - source_length;
         
    --if there is both use cases 
      IF job_use_cases ~ 'churn_postpaid' AND include_inactivity THEN --same t2 is used in postpaid and prepaid, if prepaid is included, we assume, that desc var has not been computed for this period for postpaid, either

        include_postpaid = true;
        include_pp_text = 'true';
      
        churn_postpaid_t5 = churn_inactivity_t5;


      END IF;
 
    END IF;
  
  END IF;

  --if  postpaid

  IF job_use_cases ~ 'churn_postpaid' THEN

    --if only postpaid

    IF NOT include_inactivity THEN 

      SELECT max(a.mod_job_id) INTO mod_job_id_tmp
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      JOIN work.module_job_parameters c
      ON a.mod_job_id=c.mod_job_id
      JOIN results.descriptive_variables_subscriber_matrix d --make sure desc var are computed indeed
      ON d.mod_job_id = a.mod_job_id
      WHERE a.value ~ 'descriptive_variables'
      AND a.key = 'job_type'
      AND b.key = 'job_use_cases'
      AND b.value ~ 'churn_postpaid'
      AND c.key = 'uc_churn_postpaid_t5'
      AND c.value = t2_last;


      IF mod_job_id_tmp IS NULL THEN 
        include_postpaid= true;
        include_pp_text = 'true';
      END IF; 

    -- check if targets need to be calculated

      IF NOT include_postpaid  THEN 
    
        SELECT max(m2.mod_job_id) INTO mod_job_id_tmp
        FROM work.module_targets m1
        JOIN work.module_job_parameters m2
        ON m2.mod_job_id=m1.mod_job_id
        INNER JOIN work.module_job_parameters m3
        ON m3.mod_job_id=m1.mod_job_id
        WHERE m1.target_churn_postpaid IS NOT NULL
        AND m2.key='uc_churn_postpaid_t5'
        AND m2.value=t2_last
        AND m3.key='job_use_cases'
        AND m3.value ~ 'churn_postpaid';
    
        IF mod_job_id_tmp IS NULL THEN 
          calculate_targets_pp = true;
          calculate_targets_pp_text = 'true';
        END IF; 
      END IF;

      IF include_postpaid OR calculate_targets_pp THEN 
        
        churn_postpaid_t5 = t2_last;

        SELECT min(a.mod_job_id) INTO min_mod_job_id --job to check period lengths
        FROM work.module_job_parameters a
        JOIN work.module_job_parameters b
        ON a.mod_job_id = b.mod_job_id
        JOIN work.module_job_parameters c
        ON a.mod_job_id=c.mod_job_id
        WHERE a.value = 'Predictors + Fit + Apply'
        AND a.key = 'run_type'
        AND b.key = 'job_use_cases'
        AND b.value ~ 'churn_postpaid'
        AND c.key = 'uc_churn_postpaid_t5';


        SELECT a.value::date-b.value::date INTO pp_t5_t1_length  
        FROM work.module_job_parameters a 
        JOIN work.module_job_parameters b
        ON a.mod_job_id = b.mod_job_id
        WHERE a.mod_job_id = min_mod_job_id
        AND a.key = 'uc_churn_postpaid_t5'
        AND b.key = 't1';
           
        SELECT a.value::date-b.value::date INTO pp_t5_t2_length  
        FROM work.module_job_parameters a
        JOIN work.module_job_parameters b
        ON a.mod_job_id = b.mod_job_id
        WHERE a.mod_job_id = min_mod_job_id
        AND a.key = 'uc_churn_postpaid_t5'
        AND b.key = 't2';

        t2 = churn_postpaid_t5 - pp_t5_t2_length;
        t1 = churn_postpaid_t5 - pp_t5_t1_length;

      END IF; 
    END IF;

    IF include_postpaid OR calculate_targets_pp THEN 
    --this is done always if postpaid is there

      SELECT a.value::date-b.value::date INTO pp_target_length  
      FROM work.module_job_parameters a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE a.mod_job_id = min_mod_job_id
      AND a.key = 'uc_churn_postpaid_t5'
      AND b.key = 'uc_churn_postpaid_t4';

      churn_postpaid_t4 = churn_postpaid_t5 - pp_target_length;
    END IF;
    
  END IF;

  IF include_inactivity AND include_postpaid THEN
    use_case_list_desc_var = 'churn_inactivity,churn_postpaid';
  ELSEIF  include_inactivity THEN
    use_case_list_desc_var = 'churn_inactivity';
  ELSEIF  include_postpaid THEN 
    use_case_list_desc_var = 'churn_postpaid';
  END IF;

  IF include_inactivity OR include_postpaid THEN
    initialization_needed = true;
    job_type = 'descriptive_variables_initialization';
  END IF;

  IF calculate_targets_ia OR calculate_targets_pp THEN
    calculate_targets = true;
    job_type = 'desc_var_targets';
  END IF;

  IF calculate_targets OR initialization_needed THEN 
  
    tcrm := max(m.date_inserted) 
      FROM data.data_quality dq
      JOIN data.crm_snapshot_date_inserted_mapping m
      ON dq.data_date = m.snapshot_date
      WHERE dq.data_source = 'crm' AND dq.status = 2
      AND m.date_inserted <= t2; 
 
  END IF;

  IF initialization_needed THEN
         
    SELECT core.nextval_with_partitions('work.module_sequence') INTO mod_job_id_desc_var;

    INSERT INTO work.module_job_parameters
      (mod_job_id, "key", value)
      VALUES
      (mod_job_id_desc_var, 't1', t1::text),
      (mod_job_id_desc_var, 't2', t2::text),
      (mod_job_id_desc_var, 'job_type', job_type),
      (mod_job_id_desc_var, 'tCRM', tcrm::text),
      (mod_job_id_desc_var, 'job_use_cases', use_case_list_desc_var); 

    IF include_inactivity  THEN

      INSERT INTO work.module_job_parameters
        (mod_job_id, "key", value)
         VALUES
        (mod_job_id_desc_var, 'uc_churn_inactivity_t4', churn_inactivity_t4::text),
        (mod_job_id_desc_var, 'uc_churn_inactivity_t5', churn_inactivity_t5::text),
        (mod_job_id_desc_var, 'uc_churn_inactivity_t6', churn_inactivity_t6::text),
        (mod_job_id_desc_var, 'uc_churn_inactivity_t7', churn_inactivity_t7::text);

    END IF;

    IF include_postpaid  THEN
      INSERT INTO work.module_job_parameters
        (mod_job_id, "key", value)
        VALUES
        (mod_job_id_desc_var, 'uc_churn_postpaid_t4', churn_postpaid_t4::text),
        (mod_job_id_desc_var, 'uc_churn_postpaid_t5', churn_postpaid_t5::text);

    END IF;
  END IF;  

  IF NOT initialization_needed THEN 
     
    IF calculate_targets_ia AND calculate_targets_pp THEN
      use_case_list_desc_var = 'churn_inactivity,churn_postpaid';
    ELSEIF calculate_targets_ia THEN
      use_case_list_desc_var = 'churn_inactivity';
    ELSEIF calculate_targets_pp THEN 
      use_case_list_desc_var = 'churn_postpaid';
    END IF;

 
    IF calculate_targets_ia OR calculate_targets_pp THEN
      calculate_targets = true;
      job_type = 'desc_var_targets';
    END IF;


    IF calculate_targets THEN
         
      SELECT core.nextval_with_partitions('work.module_sequence') INTO mod_job_id_desc_var;

      INSERT INTO work.module_job_parameters
        (mod_job_id, "key", value)
        VALUES
        (mod_job_id_desc_var, 't1', t1::text),
        (mod_job_id_desc_var, 't2', t2::text),
        (mod_job_id_desc_var, 'job_type', job_type),
        (mod_job_id_desc_var, 'tCRM', tcrm::text),
        (mod_job_id_desc_var, 'job_use_cases', use_case_list_desc_var); 
 
      IF calculate_targets_ia  THEN

        INSERT INTO work.module_job_parameters
          (mod_job_id, "key", value)
           VALUES
          (mod_job_id_desc_var, 'uc_churn_inactivity_t4', churn_inactivity_t4::text),
          (mod_job_id_desc_var, 'uc_churn_inactivity_t5', churn_inactivity_t5::text),
          (mod_job_id_desc_var, 'uc_churn_inactivity_t6', churn_inactivity_t6::text),
          (mod_job_id_desc_var, 'uc_churn_inactivity_t7', churn_inactivity_t7::text);

      END IF;

      IF calculate_targets_pp  THEN
        INSERT INTO work.module_job_parameters
          (mod_job_id, "key", value)
          VALUES
          (mod_job_id_desc_var, 'uc_churn_postpaid_t4', churn_postpaid_t4::text),
          (mod_job_id_desc_var, 'uc_churn_postpaid_t5', churn_postpaid_t5::text);

      END IF;
    END IF;  
  END IF;


  IF initialization_needed THEN
    calculate_targets = true;
    calculate_targets_ia_text = include_ia_text;
    calculate_targets_pp_text = include_pp_text;
  END IF;

    
  RETURN NEXT mod_job_id_desc_var::text;
  RETURN NEXT include_ia_text;
  RETURN NEXT include_pp_text;
  RETURN NEXT calculate_targets_ia_text;
  RETURN NEXT calculate_targets_pp_text;
  RETURN NEXT calculate_targets;

  RETURN;
  
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION work.initialize_descriptive_variables_initialization(integer)
  OWNER TO xsl;


