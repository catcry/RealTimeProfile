-- Copyright (c) 2013 Comptel Oy. All rights reserved.  
-- This software is the proprietary information of Comptel Oy. 
-- Use is subject to license terms. --

-- charts schema should exist

-- common scoring charts

-- DROP VIEW charts.view_scoring
CREATE OR REPLACE VIEW charts.view_scoring AS
SELECT 
  mod_job_id, 
  run_id,
  run_name,
  var_name,
  var_value
  FROM (
  (SELECT mod_job_id, run_id, run_type, run_name FROM
    (SELECT 
      mod_job_id, 
      MAX(CASE WHEN key = 'workflow_run_id' THEN value ELSE null END) AS run_id,
      MAX(CASE WHEN key = 'run_type' THEN value ELSE null END) AS run_type, 
      MAX(CASE WHEN key = 'time_finished' THEN value ELSE null END) AS run_name
      FROM work.module_job_parameters 
      GROUP BY mod_job_id
     ) a
     WHERE run_type ~ 'Apply' AND run_type !~ 'Fit'
  ) aa
  INNER JOIN
  (SELECT 
    mod_job_id AS mji, 
    var_name, 
    var_value
  FROM charts.chart_data
  WHERE stat_name = 'SCORING') bb
  ON aa.mod_job_id = bb.mji
) cc;
ALTER TABLE charts.view_scoring OWNER to xsl;

-- score distribution charts

-- DROP VIEW charts.view_score_distribution
CREATE OR REPLACE VIEW charts.view_score_distribution AS
SELECT 
  mod_job_id, 
  run_id,
  run_name,
  var_name,
  var_value, 
  group_name
  FROM (
  (SELECT mod_job_id, run_id, run_type, run_name FROM
    (SELECT 
      mod_job_id, 
      MAX(CASE WHEN key = 'workflow_run_id' THEN value ELSE null END) AS run_id,
      MAX(CASE WHEN key = 'run_type' THEN value ELSE null END) AS run_type, 
      MAX(CASE WHEN key = 'time_finished' THEN value ELSE null END) AS run_name
      FROM work.module_job_parameters 
      GROUP BY mod_job_id
     ) a
     WHERE run_type ~ 'Apply' AND run_type !~ 'Fit'
  ) aa
  INNER JOIN
  (SELECT 
    mod_job_id AS mji, 
    var_name, 
    var_value, 
    group_name
  FROM charts.chart_data
  WHERE stat_name = 'SCORE_DISTRIBUTION') bb
  ON aa.mod_job_id = bb.mji
) cc;
ALTER TABLE charts.view_score_distribution OWNER to xsl;

-- model quality - cumulative gain

-- DROP VIEW charts.view_model_quality_cumulative_gain
CREATE OR REPLACE VIEW charts.view_model_quality_cumulative_gain AS
SELECT 
  use_case, 
  round((aa.order_id - 1) / 10.0) AS "Proportion of subscribers", -- The -1 forces chart x-axis labels in the UI to start from 0 instead of 1
  round(model_value::numeric, 2) * 100 as "Model", -- * 100 to make percentages
  round(random_value::numeric, 2) * 100 AS "Random", -- * 100 to make percentages
  cc.run_id,
  aa.order_id 
FROM (
  SELECT model_id, value AS use_case
  FROM work.module_models
  WHERE key='use_case'
) bb
INNER JOIN (
  SELECT
    a.mod_job_id,
    a.value AS model_id,
    b.value AS run_id,
    c.value AS submodel_id
  FROM work.module_job_parameters AS a
  INNER JOIN work.module_job_parameters AS b
  ON a.mod_job_id = b.mod_job_id
  LEFT JOIN (SELECT * FROM work.module_models WHERE key = 'model_id') AS c -- submodel ids
  ON a.value = c.model_id
  WHERE a.key ~ 'model_id'
  AND b.key = 'workflow_run_id'
) cc
ON bb.model_id = cc.model_id
OR bb.model_id = cc.submodel_id
INNER JOIN (
  SELECT
    a.model_value,
    b.random_value,
    a.model_id,
    a.order_id
  FROM (
    (
      SELECT
        id AS order_id,
        mod_job_id,
        model_id,
        value AS model_value
      FROM results.module_results_verification
      WHERE key LIKE '%share_of_positives'
      AND   chart = 'cumulative_gains'
    ) a
    INNER JOIN (
      SELECT
        id,
        mod_job_id,
        model_id AS model_id,
        value AS random_value
      FROM results.module_results_verification
      WHERE key LIKE '%share_of_all'
      AND   chart = 'cumulative_gains'
    ) b
    ON a.order_id = b.id
    AND a.mod_job_id = b.mod_job_id
    AND a.model_id = b.model_id
  )
) aa
ON bb.model_id = aa.model_id
ORDER BY order_id ASC;
ALTER TABLE charts.view_model_quality_cumulative_gain OWNER to xsl;

-- DROP VIEW charts.view_cumulative_gain_zero_day_prediction;
CREATE OR REPLACE VIEW charts.view_cumulative_gain_zero_day_prediction AS
SELECT 
  mrv2.use_case                           AS use_case,
  round(round(mrv2.order_id/nullif(max(mrv2.order_id) over (partition by use_case),0)::numeric, 3) * 100 - 1) AS "Proportion of all zero day subscribers",
  round(mrv2.model_low::numeric, 2) * 100       AS "Low value", 
  round(mrv2.model_medium::numeric, 2) * 100    AS "Medium value", 
  round(mrv2.model_high::numeric, 2) * 100     AS "High value", 
  round(mrv2.random::numeric, 2) * 100         AS "Random", 
  mrv2.run_id,
  mrv2.order_id
FROM (
  SELECT mrv1.use_case, 
       mjp.run_id, 
       mrv1.mod_job_id, 
       mrv1.model_id, 
       mrv1.order_id, 
       max(case when mrv1.key LIKE '%_share_of_all'          then mrv1.model_value else null end) AS random,
       max(case when mrv1.key LIKE '%_share_of_target_1'     then mrv1.model_value else null end)  AS model_low,
       max(case when mrv1.key LIKE '%_share_of_target_2'     then mrv1.model_value else null end)     AS model_medium,
       max(case when mrv1.key LIKE '%_share_of_target_3'     then mrv1.model_value else null end)   AS model_high
  FROM ( 
    SELECT
      CASE WHEN mrv0.key LIKE 'target_1%'    THEN 'zero_day_prediction_low_value'
           WHEN mrv0.key LIKE 'target_2%' THEN 'zero_day_prediction_medium_value'
           WHEN mrv0.key LIKE 'target_3%'   THEN 'zero_day_prediction_high_value' END         
                       AS use_case,
      mrv0.mod_job_id  AS mod_job_id, 
      mrv0.model_id    AS model_id,
      mrv0.id          AS order_id, 
      mrv0.key         AS key,
      mrv0.value       AS model_value
    FROM results.module_results_verification mrv0
    WHERE chart = 'cumulative_gains'
--    AND key LIKE 'target%' -- Finds only multinomial models
  ) mrv1
  INNER JOIN  (
    SELECT 
      a.mod_job_id, 
      a.value AS model_id,
      b.value AS run_id,
      c.value AS submodel_id
    FROM work.module_job_parameters AS a
    INNER JOIN work.module_job_parameters AS b
    ON a.mod_job_id = b.mod_job_id
    LEFT JOIN (SELECT * FROM work.module_models WHERE key = 'model_id') AS c -- submodel ids
    ON a.value::integer = c.model_id
    INNER JOIN (SELECT model_id FROM work.module_models WHERE key = 'use_case' AND value = 'zero_day_prediction') d
    ON c.model_id = d.model_id
    WHERE a.key ~ 'model_id'
    AND b.key = 'workflow_run_id'
  ) mjp
  ON mrv1.model_id = mjp.model_id::integer
  OR mrv1.model_id = mjp.submodel_id::integer
  GROUP BY mrv1.use_case, mjp.run_id, mrv1.mod_job_id, mrv1.model_id, mrv1.order_id
) mrv2
ORDER BY order_id ASC;
ALTER TABLE charts.view_cumulative_gain_zero_day_prediction OWNER to xsl;


-- model quality - lift 

-- DROP VIEW charts.view_model_quality_lift;
-- Shows the lift from the fit job whose model is used in the apply job with certain run_id: 
CREATE OR REPLACE VIEW charts.view_model_quality_lift AS
SELECT
  use_case,
  order_id - 1 AS "Proportion of subscribers", -- The -1 forces the chart x-axis tickmarks will start from 0 instead of 1
  model_value AS "Model",
  random_value AS "Random",
  run_id,
  order_id
FROM (
SELECT 
  bb.use_case,
  aa.order_id, 
  round((sum(aa.model_value) OVER (PARTITION BY cc.run_id, bb.use_case ORDER BY aa.order_id)/aa.order_id)::numeric, 2) AS model_value,
  cc.run_id,
  aa.random_value,
  row_number() over(partition by bb.use_case, cc.run_id, aa.order_id order by aa.model_value desc) AS rn
FROM (
  SELECT 
    model_id, 
    value AS use_case
  FROM work.module_models
  WHERE key='use_case'
) bb
INNER JOIN (
  SELECT 
    a.mod_job_id, 
    a.value AS model_id,
    b.value AS run_id,
    c.value AS submodel_id
  FROM work.module_job_parameters AS a
  INNER JOIN work.module_job_parameters AS b
  ON a.mod_job_id = b.mod_job_id
  INNER JOIN work.module_job_parameters AS e
  ON a.mod_job_id = e.mod_job_id
  LEFT JOIN (SELECT * FROM work.module_models WHERE key = 'model_id') AS c -- submodel ids
  ON a.value = c.model_id
  WHERE a.key ~ 'model_id'
  AND b.key = 'workflow_run_id'
  AND e.key= 'run_type'
  AND e.value !~ 'Fit'
) cc
ON bb.model_id = cc.model_id
OR bb.model_id = cc.submodel_id
INNER JOIN (
  SELECT 
    id as order_id, 
    model_id as model_id, 
    value as model_value, 
    1::numeric as random_value 
  FROM results.module_results_verification
  WHERE key LIKE '%group_target_avg_per_global_target_avg' 
  AND chart = 'lift'
) aa
ON bb.model_id = aa.model_id
ORDER BY order_id ASC
) dd
WHERE rn = 1;
ALTER TABLE charts.view_model_quality_lift OWNER to xsl;

-- DROP VIEW charts.view_lift_zero_day_prediction;
CREATE OR REPLACE VIEW charts.view_lift_zero_day_prediction AS
SELECT DISTINCT 
  CASE WHEN dd.key LIKE 'target_1_%'    THEN 'zero_day_prediction_low_value'
       WHEN dd.key LIKE 'target_2_%'    THEN 'zero_day_prediction_medium_value'
       WHEN dd.key LIKE 'target_3_%'    THEN 'zero_day_prediction_high_value' END         
                 AS use_case,
  round(round(dd.order_id/nullif(max(dd.order_id) over (partition by dd.key),0)::numeric, 3) * 100 - 1)::numeric AS "Proportion of all zero day subscribers",
  round((sum(dd.model_value) OVER (PARTITION BY dd.run_id, dd.key, dd.submodel_id ORDER BY dd.order_id)/dd.order_id)::numeric, 2)    AS "Model", 
  dd.random_value   AS "Random",
  dd.run_id, 
  dd.order_id 
FROM (
  (SELECT id AS order_id, mod_job_id AS mji, model_id AS model_id, key AS key, value AS model_value, 1::numeric AS random_value 
  FROM results.module_results_verification
  WHERE chart = 'lift' 
  AND   key   LIKE 'target_%_group_target_avg_per_global_target_avg'
  ) mrv
  INNER JOIN (
    SELECT model_id, value AS use_case
    FROM work.module_models
    WHERE key='use_case'
    AND   value LIKE 'zero_day_prediction%'
  ) mm
  ON mrv.model_id = mm.model_id
  INNER JOIN (
    SELECT DISTINCT
      a.mod_job_id, 
      a.value AS model_id,
      b.value AS run_id,
      c.value AS submodel_id
    FROM work.module_job_parameters AS a
    INNER JOIN work.module_job_parameters AS b
    ON a.mod_job_id = b.mod_job_id
    INNER JOIN work.module_job_parameters AS e
    ON a.mod_job_id = e.mod_job_id
    LEFT JOIN (SELECT * FROM work.module_models WHERE key = 'model_id') AS c -- submodel ids
    ON a.value = c.model_id
    WHERE a.key ~ 'model_id'
    AND b.key = 'workflow_run_id'
    AND e.key= 'run_type'
    AND e.value !~ 'Fit'
  ) mjp
  ON mrv.model_id = mjp.model_id
  OR mrv.model_id = mjp.submodel_id
) dd
ORDER BY order_id ASC;
ALTER TABLE charts.view_lift_zero_day_prediction OWNER to xsl;


-- DROP VIEW charts.view_descriptive_variable_distributions
CREATE OR REPLACE VIEW charts.view_descriptive_variable_distributions AS
SELECT 
  cd.var_name    AS descriptive_variable,
  mjp.t1         AS t1,
  mjp.t2         AS t2,
  CASE WHEN cd.group_name LIKE '%churn_inactivity_propensity_score' THEN 'Churn inactivity'
       WHEN cd.group_name LIKE '%churn_postpaid_propensity_score' THEN 'Churn postpaid'
       WHEN cd.group_name LIKE '%product_product_x_propensity_score' THEN 'Product X'
       WHEN cd.group_name LIKE '%product_product_y_propensity_score' THEN 'Product Y'
       WHEN cd.group_name LIKE '%zero_day_prediction_low_value_propensity_score' THEN 'Zero day prediction - Low value'
       WHEN cd.group_name LIKE '%zero_day_prediction_medium_value_propensity_score' THEN 'Zero day prediction - Medium value'
       WHEN cd.group_name LIKE '%zero_day_prediction_high_value_propensity_score' THEN 'Zero day prediction - High value'
  END AS use_case,
  cd.var_value,
  cd.order_id
FROM charts.chart_data cd
INNER JOIN (
  SELECT mjp1.mod_job_id, mjp1.value AS t2, mjp2.value AS t1, rank() OVER (ORDER BY mjp1."value"::date DESC, mjp1.mod_job_id DESC)
  FROM  work.module_job_parameters mjp1
  INNER JOIN work.module_job_parameters mjp2
  USING (mod_job_id)
  INNER JOIN work.module_job_parameters mjp3
  USING (mod_job_id)
  WHERE mjp1."key" = 't2'
  AND   mjp2."key" = 't1'
  AND   mjp3."key" = 'descvar_mod_job_id'
  AND   mjp3."value"::integer > -1
) mjp
ON cd.mod_job_id = mjp.mod_job_id
WHERE mjp.rank = 1
AND   cd.stat_name = 'DESC_VAR_VS_SCORE_DISTRIBUTION';
ALTER TABLE charts.view_descriptive_variable_distributions OWNER to xsl;

-- DROP VIEW charts.view_differentiating_factors_mature
CREATE OR REPLACE VIEW charts.view_differentiating_factors_mature AS
SELECT 
  t.value_a AS "Descriptive variable",
  t.value_b AS "Value for all subs",
  t.value_c AS "Value for subscribers whose subscription is older than 90 days",
  t.value_d AS "Relative difference, in percentages", 
  t.order_id,
  a.value AS "t1",
  b.value AS "t2"
FROM (
  (SELECT DISTINCT 
    aa.mod_job_id,  
    aa.var_name AS value_a,
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    round(((bb.var_value-aa.var_value)/aa.var_value*100.00)::numeric,2) AS value_d,
    aa.order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.var_name = bb.var_name
  AND aa.var_value>0
  AND aa.stat_name = 'DESC_VAR_SNA'
  AND aa.group_name = 'ALL'
  AND bb.group_name = 'MATURE') a
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'ALL'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'ALL'
    AND d.key='t2'
  ) b
  ON a.mod_job_id=b.max_mod_job_id
) t
JOIN work.module_job_parameters a
ON a.mod_job_id=t.mod_job_id
JOIN work.module_job_parameters b
ON b.mod_job_id=t.mod_job_id
WHERE b.key = 't2'
AND a.key= 't1'
ORDER BY t.order_id, 4 DESC;
ALTER TABLE charts.view_differentiating_factors_mature OWNER to xsl;


CREATE OR REPLACE VIEW charts.view_differentiating_factors_new AS
SELECT 
  t.value_a AS "Descriptive variable",
  t.value_b AS "Value for all subs",
  t.value_c AS "Value for subscribers whose subscription is newer than 30 days",
  t.value_d AS "Relative difference, in percentages", 
  t.order_id, 
  a.value AS "t1",
  b.value AS "t2"
FROM (
  (SELECT DISTINCT 
    aa.mod_job_id,  
    aa.var_name AS value_a,
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    round(((bb.var_value-aa.var_value)/aa.var_value*100.00)::numeric,2) AS value_d,
    aa.order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.var_name = bb.var_name
  AND aa.var_value>0
  AND aa.stat_name = 'DESC_VAR_SNA'
  AND aa.group_name = 'ALL'
  AND bb.group_name = 'NEW') a
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'ALL'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'ALL'
    AND d.key='t2'
  ) b
  ON a.mod_job_id=b.max_mod_job_id
) t
JOIN work.module_job_parameters a
ON a.mod_job_id=t.mod_job_id
JOIN work.module_job_parameters b
ON b.mod_job_id=t.mod_job_id
WHERE b.key = 't2'
AND a.key= 't1'
ORDER BY t.order_id, 4 DESC;
ALTER TABLE charts.view_differentiating_factors_new OWNER to xsl;


CREATE OR REPLACE VIEW charts.view_arpu_and_social_revenue AS
  SELECT t.value_a AS "Segment",
    t.value_b AS "ARPU",
    t.value_c AS "Social revenue", 
    t.order_id, 
    a.value AS "t1",
    b.value AS "t2"
  FROM ((
    SELECT DISTINCT aa.mod_job_id,
      aa.var_name AS value_a,
      aa.var_value AS value_b, 
      bb.var_value AS value_c, 
      aa.order_id
    FROM charts.chart_data AS aa, charts.chart_data AS bb
    WHERE aa.stat_name = 'SUBS_ARPU'
    AND bb.stat_name = 'SUBS_SOC_REVENUE'
    AND aa.group_name = bb.group_name
    AND aa.order_id = bb.order_id
    AND aa.mod_job_id = bb.mod_job_id
    AND aa.var_name = bb.var_name) a
    JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'SUBS_ARPU' AND group_name = 'ALL'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'SUBS_ARPU' AND group_name = 'ALL'
    AND d.key='t2'
    ) b
    ON a.mod_job_id=b.max_mod_job_id
  ) t  
  JOIN work.module_job_parameters a
  ON a.mod_job_id=t.mod_job_id
  JOIN work.module_job_parameters b
  ON b.mod_job_id=t.mod_job_id
  WHERE b.key = 't2'
  AND a.key= 't1'
  ORDER BY t.order_id ASC;
ALTER TABLE charts.view_arpu_and_social_revenue OWNER to xsl;

CREATE OR REPLACE VIEW charts.view_age_class_churner AS
SELECT 
  aa.group_name AS "Group",
  aa.var_name AS "Age class",
  aa.var_value AS "Count of churners", 
  aa.order_id, 
  a.value AS "t1", 
  b.value AS "t2"
  FROM (
  SELECT DISTINCT group_name, var_name, var_value, order_id, a.mod_job_id 
  FROM charts.chart_data  a
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'AGE_CLASS' AND group_name = 'CHURNER_I'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'AGE_CLASS' AND group_name = 'CHURNER_I'
    AND d.key='t2'
  ) b
  ON max_mod_job_id=a.mod_job_id
  WHERE stat_name = 'AGE_CLASS' AND group_name = 'CHURNER_I'
  UNION
  SELECT DISTINCT group_name, var_name, var_value, order_id, a.mod_job_id
  FROM charts.chart_data  a
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'AGE_CLASS' AND group_name = 'CHURNER_P'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'AGE_CLASS' AND group_name = 'CHURNER_P'
    AND d.key='t2'
  ) b
  ON max_mod_job_id=a.mod_job_id
  WHERE stat_name = 'AGE_CLASS' AND group_name = 'CHURNER_P'
) aa
JOIN work.module_job_parameters a
ON a.mod_job_id=aa.mod_job_id
JOIN work.module_job_parameters b
ON b.mod_job_id=aa.mod_job_id
WHERE b.key = 't2'
AND a.key= 't1'
ORDER BY order_id ASC;
ALTER TABLE charts.view_age_class_churner OWNER to xsl;

CREATE OR REPLACE VIEW charts.view_6_time_from_last_topup AS
SELECT 
  aa.var_name AS "Time from last topup",
  round((aa.var_value/greatest((sum(var_value) over()),1)*100)::numeric, 2) AS "Share of pre-paid subs, in percentage", 
  aa.order_id, 
  a.value AS "t1", 
  b.value AS "t2"
FROM (
    SELECT DISTINCT var_name, var_value, order_id, a.mod_job_id 
  FROM charts.chart_data  a
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'LAST_TOPUP' AND group_name = 'CHURNER'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'LAST_TOPUP' AND group_name = 'CHURNER'
    AND d.key='t2'
  ) b
  ON max_mod_job_id=a.mod_job_id
  WHERE stat_name = 'LAST_TOPUP' AND group_name = 'CHURNER'
  ) aa
  JOIN work.module_job_parameters a
ON a.mod_job_id=aa.mod_job_id
JOIN work.module_job_parameters b
ON b.mod_job_id=aa.mod_job_id
WHERE b.key = 't2'
AND a.key= 't1'
ORDER BY order_id ASC;
ALTER TABLE charts.view_6_time_from_last_topup OWNER to xsl;


CREATE OR REPLACE VIEW charts.view_differentiating_factors_churner AS
SELECT 
  t.group AS "Group",
  t.value_a AS "Descriptive variable",
  t.value_b AS "Value for all subs",
  t.value_c AS "Value for churners",
  t.value_d AS "Relative difference, in percentages",  
  t.order_id, 
  a.value AS "t1", 
  b.value AS "t2"
FROM (
  SELECT DISTINCT 
    aa.mod_job_id as mod_job_id,  
    bb.group_name as group,
    aa.var_name AS value_a,
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    round(((bb.var_value-aa.var_value)/aa.var_value*100.00)::numeric,2) AS value_d,
    aa.order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'CHURNER_I'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'CHURNER_I'
      AND d.key='t2'
    ) b
  ON mod_job_id=b.max_mod_job_id
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.var_name = bb.var_name
  AND aa.var_value>0
  AND (aa.stat_name = 'DESC_VAR_SNA' OR aa.stat_name = 'DESC_VAR_SU1' OR aa.stat_name = 'DESC_VAR_SU2' OR aa.stat_name = 'DESC_VAR_SU3' OR aa.stat_name = 'DESC_VAR_DU' OR aa.stat_name = 'DESC_VAR_TOPUP')
  AND aa.group_name = 'ALL'
  AND bb.group_name = 'CHURNER_I'
  UNION
  SELECT DISTINCT 
    aa.mod_job_id as mod_job_id,  
    bb.group_name as group,
    aa.var_name AS value_a,
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    round(((bb.var_value-aa.var_value)/aa.var_value*100.00)::numeric,2) AS value_d,
    aa.order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'CHURNER_P'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'CHURNER_P'
      AND d.key='t2'
    ) b
  ON mod_job_id=b.max_mod_job_id
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.var_name = bb.var_name
  AND aa.var_value>0
  AND (aa.stat_name = 'DESC_VAR_SNA' OR aa.stat_name = 'DESC_VAR_SU1' OR aa.stat_name = 'DESC_VAR_SU2' OR aa.stat_name = 'DESC_VAR_SU3' OR aa.stat_name = 'DESC_VAR_DU' OR aa.stat_name = 'DESC_VAR_TOPUP')
  AND aa.group_name = 'ALL'
  AND bb.group_name = 'CHURNER_P'
) t
JOIN work.module_job_parameters a
ON a.mod_job_id=t.mod_job_id
JOIN work.module_job_parameters b
ON b.mod_job_id=t.mod_job_id
WHERE b.key = 't2'
AND a.key= 't1'
ORDER BY t.order_id, 4 DESC;
ALTER TABLE charts.view_differentiating_factors_churner OWNER to xsl;

 CREATE OR REPLACE VIEW charts.view_differentiating_factors_data AS
SELECT 
  t.value_a AS "Descriptive variable",
  t.value_b AS "Value for all subs",
  t.value_c AS "Value for subscribers whose subscription is older than 90 days",
  t.value_d AS "Relative difference, in percentages", 
  t.order_id
FROM (
  (SELECT DISTINCT 
    aa.mod_job_id,  
    aa.var_name AS value_a,
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    round(((bb.var_value-aa.var_value)/aa.var_value*100.00)::numeric,2) AS value_d,
    aa.order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.var_name = bb.var_name
  AND aa.var_value>0
  AND aa.stat_name = 'DESC_VAR_DU'
  AND aa.group_name = 'ALL'
  AND bb.group_name = 'MATURE') a
  JOIN (
    SELECT max(mod_job_id) AS max_mod_job_id 
    FROM charts.chart_data 
    WHERE stat_name = 'DESC_VAR_DU' AND group_name = 'ALL') b
  ON a.mod_job_id=b.max_mod_job_id
) t
ORDER BY t.order_id, 4 DESC;
ALTER TABLE charts.view_differentiating_factors_data OWNER to xsl;


CREATE OR REPLACE VIEW charts.view_churnrate_score_list AS
SELECT DISTINCT 
    t.group AS "Group",
    t.value_a AS "Month", 
    t.value_b AS "Churn rate all subs", 
    t.value_c AS "Churn rate new subs",
    t.value_e AS "Churn rate data users",
    t.value_d AS "Churn rate mature subs",
    t.order_id
FROM (
  SELECT DISTINCT 
    aa.mod_job_id as mod_job_id,
    aa.var_name AS value_a, 
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    cc.var_value AS value_d,
    0 as value_e, --change this for data user value
    --dd.var_value AS value_e,
    aa.stat_name AS group,
    aa.data_date AS order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb, charts.chart_data AS cc, charts.chart_data AS dd
  JOIN (
    --take the latest if there are several for one week
    SELECT max(mod_job_id) AS max_mod_job_id
    FROM charts.chart_data 
    WHERE stat_name = 'CHURN_RATE_SCORE_LIST_P' 
    AND group_name = 'ALL'
    GROUP BY data_date
  ) b
  ON mod_job_id=b.max_mod_job_id
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.stat_name = bb.stat_name
  AND aa.var_name = bb.var_name
  AND aa.mod_job_id = cc.mod_job_id
  AND aa.stat_name = cc.stat_name 
  AND aa.var_name = cc.var_name
--  AND aa.mod_job_id = dd.mod_job_id --change these, too
--  AND aa.stat_name = dd.stat_name 
--  AND aa.var_name = dd.var_name
  AND aa.stat_name = 'CHURN_RATE_SCORE_LIST_P'
  AND aa.group_name = 'ALL' 
  AND bb.group_name = 'NEW'
  AND cc.group_name = 'MATURE'
--  AND dd.group_name = 'DATA'
  UNION 
  SELECT DISTINCT 
    aa.mod_job_id as mod_job_id,
    aa.var_name AS value_a, 
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    cc.var_value AS value_d,
    0 as value_e, --change this for data user value
    --dd.var_value AS value_e,
    aa.stat_name AS group,
    aa.data_date AS order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb, charts.chart_data AS cc, charts.chart_data AS dd
  JOIN (
    --take the latest if there are several for one week
    SELECT max(mod_job_id) AS max_mod_job_id
    FROM charts.chart_data 
    WHERE stat_name = 'CHURN_RATE_SCORE_LIST_I' 
    AND group_name = 'ALL'
    GROUP BY data_date
  ) b
  ON mod_job_id=b.max_mod_job_id
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.stat_name = bb.stat_name
  AND aa.var_name = bb.var_name
  AND aa.mod_job_id = cc.mod_job_id
  AND aa.stat_name = cc.stat_name 
  AND aa.var_name = cc.var_name
--  AND aa.mod_job_id = dd.mod_job_id --change these, too
--  AND aa.stat_name = dd.stat_name 
--  AND aa.var_name = dd.var_name
  AND aa.stat_name = 'CHURN_RATE_SCORE_LIST_I'
  AND aa.group_name = 'ALL' 
  AND bb.group_name = 'NEW'
  AND cc.group_name = 'MATURE'
--  AND dd.group_name = 'DATA'
) t
ORDER BY t.order_id ASC;
ALTER TABLE charts.view_churnrate_score_list OWNER to xsl;

CREATE OR REPLACE VIEW charts.view_variable_group_score_drivers AS
SELECT
  cd1.stat_name,
  mm.value       AS use_case,
  mm.model_id,
  cd1.data_date,
  cd1.order_id,
  cd1.var_name   AS driver,
  cd1.var_value
FROM charts.chart_data cd1
INNER JOIN
(
  SELECT MAX(mod_job_id) AS max_mod_job_id
  FROM charts.chart_data
  WHERE stat_name ~ 'VARIABLE_GROUP_SCORE_DRIVERS_'
  GROUP BY data_date
) cd2
ON cd1.mod_job_id = cd2.max_mod_job_id
INNER JOIN work.module_models mm
ON trim(lower(cd1.group_name), 'model_id ') = mm.model_id
WHERE cd1.stat_name ~ 'VARIABLE_GROUP_SCORE_DRIVERS_'
AND mm.key = 'use_case';
ALTER TABLE charts.view_variable_group_score_drivers OWNER to xsl;


CREATE OR REPLACE VIEW charts.view_network_statistics AS
SELECT 
  aa.var_name AS "Variable",
  aa.var_value AS "Value", 
  aa.order_id,
  a.value AS "t1", 
  b.value AS "t2"
FROM 
  (SELECT DISTINCT var_name, var_value, order_id, a.mod_job_id 
  FROM charts.chart_data a
  JOIN (
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'NETWORK_STATS' AND group_name = 'ALL'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'NETWORK_STATS' AND group_name = 'ALL'
      AND d.key='t2'
    ) b 
    ON max_mod_job_id=a.mod_job_id
    WHERE stat_name = 'NETWORK_STATS' AND group_name = 'ALL'
   ) aa
JOIN work.module_job_parameters a
ON a.mod_job_id=aa.mod_job_id
JOIN work.module_job_parameters b
ON b.mod_job_id=aa.mod_job_id
WHERE b.key = 't2'
AND a.key= 't1'
ORDER BY order_id ASC;
ALTER TABLE charts.view_network_statistics OWNER to xsl;

-- DROP VIEW charts.view_differentiating_factors
CREATE OR REPLACE VIEW charts.view_differentiating_factors AS
SELECT 
  t.value_a AS "Descriptive variable",
  t.value_b AS "Value for all subs",
  t.value_c AS "Value for group",
  t.value_d AS "Relative difference, in percentages", 
  t.value_e AS "group_id",
  t.order_id, 
  a.value AS "t1",
  b.value AS "t2"
FROM (
  (SELECT DISTINCT 
    aa.mod_job_id,  
    aa.var_name AS value_a,
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    round(((bb.var_value-aa.var_value)/aa.var_value*100.00)::numeric,2) AS value_d,
    bb.group_name as value_e,
    aa.order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.var_name = bb.var_name
  AND aa.var_value>0
  AND aa.stat_name LIKE 'DESC_VAR_%'
  AND aa.group_name = 'ALL') a
  JOIN ( 
    SELECT max(d.mod_job_id) AS max_mod_job_id
    FROM (
      SELECT max(b.value) AS value
      FROM charts.chart_data a
      JOIN work.module_job_parameters b
      ON a.mod_job_id = b.mod_job_id
      WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'ALL'
      AND b.key='t2'
      ) c
    JOIN work.module_job_parameters d 
    ON d.value=c.value 
    JOIN charts.chart_data e
    ON d.mod_job_id=e.mod_job_id
    WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'ALL'
      AND d.key='t2'
  )b
  ON a.mod_job_id=b.max_mod_job_id
) t
JOIN work.module_job_parameters a
ON a.mod_job_id=t.mod_job_id
JOIN work.module_job_parameters b
ON b.mod_job_id=t.mod_job_id
WHERE b.key = 't2'
AND a.key= 't1'
ORDER BY t.order_id, 4 DESC;
ALTER TABLE charts.view_differentiating_factors OWNER to xsl;


CREATE OR REPLACE VIEW charts.view_product_penetration_weekly AS
SELECT 
  t.var_name, 
  t.var_value, -- percentage of product users
  t.group_name, -- product name
  t.group_id, -- product number
  -1 * t.order_id AS order_id -- order of weeks (from first to last; hence the -1)
FROM (
  SELECT 
    var_name, 
    round(var_value::numeric*100,2)::double precision AS var_value, 
    group_name,
    row_number() over(partition by data_date order by group_name) AS group_id,
    row_number() over(partition by group_name order by data_date desc) AS order_id
  FROM charts.chart_data a
  INNER JOIN (
    --take the latest if there are several for one week
    SELECT max(mod_job_id) AS max_mod_job_id
    FROM charts.chart_data 
    WHERE stat_name = 'PRODUCT_PENETRATION'  
    GROUP BY data_date
  ) b
  ON a.mod_job_id=b.max_mod_job_id
  WHERE a.stat_name = 'PRODUCT_PENETRATION'  
) t
WHERE order_id <= 20; -- 20 Last weeks
ALTER TABLE charts.view_product_penetration_weekly OWNER to xsl;

CREATE OR REPLACE VIEW charts.view_zero_day_segment_distribution AS
SELECT 
  t.var_name, 
  t.var_value, -- percentage of value segment members
  t.group_name, -- value segment name
  t.group_id, -- value segment number
  -1 * t.order_id2 AS order_id -- order of weeks (from first to last; hence the -1)
FROM (
  SELECT 
    var_name, 
    var_value, 
    group_name,
    a.order_id AS group_id, 
    row_number() over(partition by group_name order by data_date desc) AS order_id2
  FROM charts.chart_data a
  INNER JOIN (
    --take the latest if there are several for one week
    SELECT max(mod_job_id) AS max_mod_job_id
    FROM charts.chart_data 
    WHERE stat_name = 'ZERO_DAY_SEGMENT_DISTR'  
    GROUP BY data_date
  ) b
  ON a.mod_job_id=b.max_mod_job_id
  WHERE a.stat_name = 'ZERO_DAY_SEGMENT_DISTR'  
) t
WHERE order_id2 <= 20; -- 20 Last weeks
ALTER TABLE charts.view_zero_day_segment_distribution OWNER to xsl;

-- Note: warpu view has not yet been tested with data
CREATE OR REPLACE VIEW charts.view_zero_day_prediction_warpu_tracking AS 
  SELECT 
    split_part(cd1.group_name, ', ', 1) AS predicted_segment, --Format 'Low value'
    split_part(cd1.group_name, ', ', 2) AS switch_on_month,   --Format 'Joined Mar13'
    cd1.var_value AS warpu,   --warpu
    cd1.var_name  AS topup_month, --format 'Apr13'
    cd1.order_id     --order id, format topup month in form YYYYMM (integer)
  FROM charts.chart_data cd1
  INNER JOIN (
    --take the latest
    SELECT DISTINCT max(cd02.mod_job_id) AS max_mod_job_id 
    FROM (
      SELECT max(data_date) AS max_data_date
      FROM charts.chart_data 
      WHERE stat_name = 'ZERO_DAY_WARPU'
    ) cd01
    INNER JOIN (
      SELECT mod_job_id, data_date 
      FROM charts.chart_data 
      WHERE stat_name = 'ZERO_DAY_WARPU'
    ) cd02
    ON cd01.max_data_date = cd02.data_date
  ) cd2
  ON cd1.mod_job_id=cd2.max_mod_job_id
  WHERE cd1.stat_name = 'ZERO_DAY_WARPU';
ALTER TABLE charts.view_zero_day_prediction_warpu_tracking OWNER to xsl;

-- DROP VIEW charts.view_differentiating_factors_zero_day
CREATE OR REPLACE VIEW charts.view_differentiating_factors_zero_day AS
SELECT 
  t.value_a AS "Descriptive variable",
  t.value_b AS "Value for all subs",
  t.value_c AS "Value for group",
  t.value_d AS "Relative difference, in percentages", 
  t.value_e AS "group_id",
  t.order_id
FROM (
  (SELECT DISTINCT 
    aa.mod_job_id,  
    aa.var_name AS value_a,
    aa.var_value AS value_b, 
    bb.var_value AS value_c,
    round(((bb.var_value-aa.var_value)/aa.var_value*100.00)::numeric,2) AS value_d,
    bb.group_name as value_e,
    aa.order_id
  FROM charts.chart_data AS aa, charts.chart_data AS bb
  WHERE aa.mod_job_id = bb.mod_job_id
  AND aa.var_name = bb.var_name
  AND aa.var_value>0
  AND aa.stat_name LIKE 'DESC_VAR_%'
  AND aa.group_name = 'ALL_ZERO_DAY') a
  JOIN (
    SELECT max(mod_job_id) AS max_mod_job_id 
    FROM charts.chart_data 
    WHERE stat_name = 'DESC_VAR_SNA' AND group_name = 'ALL_ZERO_DAY') b
  ON a.mod_job_id=b.max_mod_job_id
) t
ORDER BY t.order_id, 4 DESC;
ALTER TABLE charts.view_differentiating_factors_zero_day OWNER to xsl;




CREATE OR REPLACE VIEW charts.view_model_quality_cumulative_gain_apply AS 
SELECT f2.use_case, f2."Proportion of subscribers",  f1."Model"as Applied, f2."Random" as Random, f2."Random"::integer as random_y, f2."Model" as Fitted, substr(f1.ti, 0, char_length(f1.ti)-2) as "Applied_time_inserted", substr(f2.ti, 0, char_length(f2.ti)-2)   AS "Fitted_time_inserted",  f2.order_id FROM 
(SELECT 
  bb.use_case, 
  round((aa.order_id::double precision/numb*1000 - 1) / 10.0) AS "Proportion of subscribers", -- The -1 forces chart x-axis labels in the UI to start from 0 instead of 1
  round(aa.model_value::numeric, 2) * 100 as "Model", -- * 100 to make percentages
  round(aa.random_value::numeric, 2) * 100 AS "Random", -- * 100 to make percentages
  round(aa.order_id/numb::double precision*1000) as order_id, 
  date_trunc('minute', aa.dat::timestamp)::text as ti,
  'Applied model' AS chart
  FROM (
  SELECT model_id, value AS use_case
  FROM work.module_models
  WHERE key='use_case'
) bb
INNER JOIN (
  SELECT 
    a.value AS model_id,
    c.value AS submodel_id, 
    a.mod_job_id as mod_job_id
  FROM work.module_job_parameters AS a
  JOIN work.module_job_parameters AS b
  on a.mod_job_id = b.mod_job_id
  LEFT JOIN (
    SELECT * 
    FROM work.module_models 
    WHERE key = 'model_id'
  ) AS c -- submodel ids
  ON a.value = c.model_id
  WHERE a.key ~ 'model_id'
  AND b.key= 'run_type'
  AND b.value ~ 'Fit'
) cc
ON bb.model_id = cc.model_id
OR bb.model_id = cc.submodel_id
INNER JOIN (
  SELECT
    a.model_value,
    b.random_value,
    a.model_id,
    a.order_id,
    b.dat, 
    count(*) over(partition by a.model_id, use_case) as numb, 
    use_case
  FROM (
    (
      SELECT
        id AS order_id,
        l.mod_job_id,
        model_id,
        value AS model_value
        FROM results.module_results_verification l
        WHERE key LIKE '%share_of_positives'
        AND   chart = 'cumulative_gains_apply' 
      ) a
      INNER JOIN (
        SELECT
          id,
          l.mod_job_id,
          be.model_id AS model_id,
          l.value AS random_value, 
          max(kkd.value::timestamp)::text AS dat,
          be.use_case  
        FROM results.module_results_verification l
        JOIN (
          SELECT max(mod_job_id) as mod_job_id, max(model_id) as model_id, use_case
          FROM(
            SELECT t.mod_job_id, model_id AS model_id, trim(substring(k.key from 4 for (char_length(k.key)-8-4))) as use_case
            FROM results.module_results_verification t
            JOIN work.module_job_parameters k 
            ON t.model_id = k.value
            WHERE t.key LIKE '%share_of_positives' 
            AND t.chart = 'cumulative_gains_apply' 
            AND k.key ~ 'model_id') f 
          GROUP BY use_case 
        ) be
        ON be.mod_job_id=l.mod_job_id
        AND be.model_id = l.model_id 
        JOIN work.module_job_parameters kk
        ON be.model_id = kk.value 
        AND be.use_case = trim(substring(kk.key from 4 for (char_length(kk.key)-8-4)))
        JOIN (
          SELECT mod_job_id, value, CASE WHEN char_length(key)>4 THEN trim(substring(key from 4 for (char_length(key)-3-3))) 
            ELSE NULL END as use_case 
            FROM work.module_job_parameters
            WHERE key ~ 't2'
          ) lo
        ON lo.mod_job_id = l.mod_job_id
        AND (be.use_case = lo.use_case OR lo.use_case IS NULL)
        JOIN work.module_job_parameters kki
        ON kki.mod_job_id = kk.mod_job_id
        JOIN work.module_job_parameters se
        ON se.value = lo.value 
        AND se.mod_job_id = kki.mod_job_id
        JOIN work.module_job_parameters kkd
        ON kkd.mod_job_id = kki.mod_job_id
        WHERE se.key = 't2'
        AND l.key LIKE '%share_of_all'
        AND   chart = 'cumulative_gains_apply'
        AND kk.key ~ 'model_id' 
        AND kki.value = 'Predictors + Apply'
        AND kkd.key = 'time_inserted' 
        GROUP BY 1,2,3,4,6
      ) b
      ON a.order_id = b.id
      AND a.mod_job_id = b.mod_job_id
      AND a.model_id = b.model_id 
    )
  ) aa
ON bb.model_id = aa.model_id
AND bb.use_case = aa.use_case
ORDER BY order_id ASC) f1
FULL JOIN
(SELECT 
  use_case, 
  round((dd.order_id::double precision/numb*1000 - 1) / 10.0) AS "Proportion of subscribers", -- The -1 forces chart x-axis labels in the UI to start from 0 instead of 1
  round(dd.model_value::numeric, 2) * 100 as "Model", -- * 100 to make percentages
  round(dd.random_value::numeric, 2) * 100 AS "Random", -- * 100 to make percentages
  dd.order_id, 
  date_trunc('minute', dd.dat::timestamp)::text as ti, 
  'Trained model' AS chart
  FROM (
  SELECT model_id, value AS use_case
  FROM work.module_models
  WHERE key='use_case'
) bb
INNER JOIN (
  SELECT 
    a.value AS model_id,
    c.value AS submodel_id, 
    a.mod_job_id as mod_job_id
  FROM work.module_job_parameters AS a
  JOIN work.module_job_parameters AS b
  on a.mod_job_id = b.mod_job_id
  LEFT JOIN (
    SELECT * 
    FROM work.module_models 
    WHERE key = 'model_id'
  ) AS c -- submodel ids
  ON a.value = c.model_id
  WHERE a.key ~ 'model_id'
  AND b.key= 'run_type'
  AND b.value ~ 'Fit'
) cc
ON bb.model_id = cc.model_id
OR bb.model_id = cc.submodel_id
INNER JOIN (
  SELECT
    a.model_value,
    b.random_value,
    a.model_id,
    a.order_id,
    a.mod_job_id, 
    count(*) over(partition by a.model_id) as numb,
    b.dat
  FROM (
    (
      SELECT
        id AS order_id,
        l.mod_job_id,
        model_id,
        value AS model_value
      FROM results.module_results_verification l
        WHERE key LIKE '%share_of_positives'
      AND   chart = 'cumulative_gains' 
    ) a
    INNER JOIN (
      SELECT
        id,
        l.mod_job_id,
        model_id AS model_id,
        l.value AS random_value,
        kk.value AS dat
      FROM results.module_results_verification l
      JOIN work.module_job_parameters kk
      ON kk.mod_job_id=l.mod_job_id
      WHERE l.key LIKE '%share_of_all'
      AND   chart = 'cumulative_gains'
      AND kk.key = 'time_inserted' 
    ) b
    ON a.order_id = b.id
    AND a.mod_job_id = b.mod_job_id
    AND a.model_id = b.model_id 
    )
  ) dd
  ON dd.model_id = bb.model_id 
  AND cc.mod_job_id = dd.mod_job_id
  JOIN (
          SELECT max(a.model_id) as model_id, a.value
          FROM results.module_results_verification b
          JOIN work.module_models a
          on a.model_id=b.model_id
          WHERE b.key LIKE '%share_of_positives' 
          AND chart = 'cumulative_gains_apply'
          AND a.key = 'use_case'
          GROUP BY  a.value
          ) be
  ON be.model_id=dd.model_id
    
ORDER BY order_id ASC
) f2
ON f1.order_id=f2.order_id 
AND f1.use_case=f2.use_case 
WHERE f1."Model" IS NOT NULL 
AND f2."Model" IS NOT NULL 
ORDER BY f2.order_id; 
ALTER TABLE charts.view_model_quality_cumulative_gain_apply OWNER to xsl;

CREATE OR REPLACE VIEW charts.view_model_quality_lift_apply AS

   SELECT bb.use_case, 
     aa.order_id - 1 AS "Proportion of subscribers", 
     round(((sum(aa.model_value) OVER(PARTITION BY bb.use_case   ORDER BY aa.order_id)) / aa.order_id::double precision)::numeric, 2) AS "Model", 
     aa.random_value AS "Random", 
     round(((sum(dd.model_value) OVER(  PARTITION BY bb.use_case   ORDER BY dd.order_id)) / dd.order_id::double precision)::numeric, 2) AS "Fitted",
     aa.order_id, 
     substr(dd.dat, 0, char_length(dd.dat)-2) as "Fitted_time_inserted", 
     substr(aa.dat, 0, char_length(aa.dat)-2) as "Model_time_inserted"
   FROM ( 
     SELECT module_models.model_id, 
       module_models.value AS use_case
     FROM work.module_models
     WHERE module_models.key = 'use_case'::text
   ) bb
   JOIN ( 
     SELECT a.value AS model_id, c.value AS submodel_id, a.mod_job_id
     FROM work.module_job_parameters a
     JOIN work.module_job_parameters b 
     ON a.mod_job_id = b.mod_job_id
     LEFT JOIN ( 
       SELECT module_models.model_id, 
         module_models.aid,  
         module_models.bid,  
         module_models.output_id, 
         module_models.key, 
         module_models.value
       FROM work.module_models
       WHERE module_models.key = 'model_id'::text
     ) c 
     ON a.value = c.model_id::text
     WHERE a.key ~ 'model_id'
     AND b.key = 'run_type'
     AND b.value ~ 'Fit'
   ) cc 
   ON bb.model_id::text = cc.model_id 
   OR bb.model_id::text = cc.submodel_id
   JOIN ( 
     SELECT l.id AS order_id, 
       l.model_id, 
       l.value AS model_value, 
       1 AS random_value, 
       l.mod_job_id, 
       date_trunc('minute',max(kkd.value::timestamp))::text as dat,
       be.use_case AS use_case  
     FROM results.module_results_verification l
     JOIN ( 
       SELECT max(mod_job_id) as mod_job_id, max(model_id) as model_id, use_case
       FROM(
       SELECT t.mod_job_id, model_id AS model_id, trim(substring(k.key from 4 for (char_length(k.key)-8-4))) as use_case
       FROM results.module_results_verification t
       JOIN work.module_job_parameters k 
       ON t.model_id = k.value
       WHERE t.key LIKE '%group_target_avg_per_global_target_avg' 
       AND t.chart = 'lift_apply' 
       AND k.key ~ 'model_id') f 
       group by use_case       
     ) be 
     ON be.mod_job_id = l.mod_job_id
     AND be.model_id = l.model_id
     JOIN work.module_job_parameters kk
     ON be.model_id = kk.value 
     AND be.use_case = trim(substring(kk.key from 4 for (char_length(kk.key)-8-4)))
     JOIN (
       SELECT mod_job_id, value, CASE WHEN char_length(key)>4 THEN trim(substring(key from 4 for (char_length(key)-3-3))) 
         ELSE NULL END as use_case 
         FROM work.module_job_parameters
         WHERE key ~ 't2'
     ) lo
     ON lo.mod_job_id = l.mod_job_id
     AND (be.use_case = lo.use_case OR lo.use_case IS NULL)
     JOIN work.module_job_parameters kki
     ON kki.mod_job_id = kk.mod_job_id
     JOIN work.module_job_parameters se
     ON se.value = lo.value 
     AND se.mod_job_id = kki.mod_job_id
     JOIN work.module_job_parameters kkd
     ON kkd.mod_job_id = kki.mod_job_id
     WHERE se.key = 't2'
     AND l.key LIKE '%group_target_avg_per_global_target_avg' 
     AND l.chart = 'lift_apply' 
     AND  kk.key ~ 'model_id'
     AND kki.value = 'Predictors + Apply'
     AND kkd.key = 'time_inserted' 
     group by 1,2,3,4, 5, 7
   ) aa 
   ON bb.model_id = aa.model_id
   AND bb.use_case = aa.use_case
   JOIN ( 
     SELECT l.id AS order_id, 
       l.model_id, 
       l.value AS model_value, 
       1 AS random_value, 
       l.mod_job_id, 
       date_trunc('minute', tt.value::timestamp)::text as dat
     FROM results.module_results_verification l
     JOIN work.module_job_parameters tt
     ON tt.mod_job_id=l.mod_job_id
     WHERE l.key LIKE '%group_target_avg_per_global_target_avg' 
     AND l.chart = 'lift'
     AND tt.key = 'time_inserted'
   ) dd 
   ON aa.model_id = dd.model_id 
   AND cc.mod_job_id = dd.mod_job_id 
   AND aa.order_id = dd.order_id
   ORDER BY dd.order_id;

 ALTER TABLE charts.view_model_quality_lift_apply OWNER to xsl;
