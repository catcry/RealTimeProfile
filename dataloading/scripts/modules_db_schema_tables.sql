-- Copyright (c) 2012 Comptel Oy. All rights reserved.  
-- This software is the proprietary information of Comptel Oy. 
-- Use is subject to license terms. --

\set ON_ERROR_STOP 1

CREATE OR REPLACE FUNCTION install_check(schema_name text, table_name text)
  RETURNS void AS
$BODY$
  DECLARE
  table_count integer;
  BEGIN
    select count(*) into table_count
    from pg_tables where schemaname = schema_name and tablename = table_name;
    if table_count > 0 then
      RAISE EXCEPTION 'Full installation of database is not possible; the database has already been installed.';
    end if;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

ALTER FUNCTION install_check(text, text) OWNER TO xsl;

select install_check('core', 'partition_sequence_tables');

DROP FUNCTION install_check(text, text);


-- core and tmp schema should exist


CREATE SCHEMA work;
ALTER SCHEMA work owner to xsl;

CREATE SCHEMA results;
ALTER SCHEMA results OWNER TO xsl;

CREATE SCHEMA charts;
ALTER SCHEMA charts OWNER TO xsl;

--For storing partition configurations for tables partitioned 
--over integer column taken from a sequence.
CREATE TABLE core.partition_sequence_tables (
  table_name text NOT NULL,
  sequence_name text NOT NULL,
  compresslevel int, --NULL for no compression,
  retention_dates int DEFAULT 180,
  cleanup boolean DEFAULT TRUE, --TRUE for the table to be included in automatic cleanup
  PRIMARY KEY (table_name)
) DISTRIBUTED BY (table_name);
ALTER TABLE core.partition_sequence_tables OWNER TO xsl;

--For storing the partition create times
CREATE TABLE core.partition_sequence_create_times (
  table_name text NOT NULL,
  sequence_name text NOT NULL,
  sequence_id integer NOT NULL,
  time_created timestamp NOT NULL,
  cleanup boolean NOT NULL,
  PRIMARY KEY (table_name, sequence_id)
) DISTRIBUTED BY (table_name, sequence_id);
ALTER TABLE core.partition_sequence_create_times OWNER TO xsl;

-- work.network_sequence is used for getting the next job_id for network scoring
CREATE SEQUENCE work.network_sequence
INCREMENT 1
MINVALUE 1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER TABLE work.network_sequence OWNER TO xsl;

-- aliases table for decoding alias_id and finding in_out_network information 
-- (in_out_network = 1 for on-net or subscribers, in_out_network = 0 for off-net)
-- this is a work table with job_id and it doesn't necessary contain all alias_id's
-- which are available in aliases.aliases_updated table
CREATE TABLE work.aliases (
  job_id integer NOT NULL,
  alias_id integer NOT NULL,
  on_black_list smallint NOT NULL DEFAULT 0, 
  in_out_network smallint NOT NULL DEFAULT 0
) WITH (appendonly=TRUE, compresslevel=7)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.aliases OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.aliases', 'work.network_sequence', 7);

-- work.out_network contains the actual network objects
CREATE TABLE work.out_network
(
  alias_a integer NOT NULL,
  alias_b integer NOT NULL,
  job_id integer NOT NULL,
  weight double precision
) 
WITH (appendonly=true, compresslevel=5)
distributed by (alias_a)
PARTITION BY RANGE (job_id) (PARTITION "0" START (0) END (1)); 
ALTER TABLE work.out_network OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.out_network', 'work.network_sequence', 5);

-- work.out_scores contains the results of the base networkscorer runs-- work.out_scores contains the results of the base networkscorer runs
CREATE TABLE work.out_scores
(
  alias_id integer NOT NULL,
  job_id integer NOT NULL,
  alpha double precision,
  k integer,
  community_id integer,
  c double precision,
  wec double precision,
  kshell integer,
  socrev double precision,
  socrevest double precision
)
WITH (appendonly=true, compresslevel=5)
distributed by (alias_id)
PARTITION BY RANGE (job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.out_scores OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.out_scores', 'work.network_sequence', 5);


-- table used for runing the symmetrization in xsl
CREATE TABLE tmp.w_sym_network (
  alias_a integer not null,
  alias_b integer not null,
  job_id integer not null,
  weight double precision
) 
WITH (appendonly=TRUE, compresslevel=5)
DISTRIBUTED BY (alias_a)
PARTITION BY RANGE (job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.w_sym_network OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.w_sym_network', 'work.network_sequence', 5, 10);


-- work.module_targets contains a target and audience column for each use case
-- The target column contains the value of target variable (such as 1, -1, or NULL for churn_inactivity usecase
-- The audience column contains 1 for all the subscribers who are scored in the corresponding usecase. 
CREATE TABLE work.module_targets (
  mod_job_id                           integer NOT NULL,
  alias_id                             integer NOT NULL,
  audience_churn_inactivity            smallint,
  audience_churn_postpaid              smallint,
  audience_zero_day_prediction         smallint,
  target_churn_inactivity              smallint,
  target_churn_postpaid                smallint,
  target_zero_day_prediction           smallint,
  target_product_product_x             smallint,
  audience_product_product_x           smallint,
  target_product_product_y             smallint,
  audience_product_product_y           smallint
) 
WITH (appendonly=true, compresslevel=5)
distributed by (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.module_targets OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.module_targets', 'work.module_sequence', 5);

-- For temporarily storing the target values of different use cases in stack format
CREATE TABLE tmp.module_targets_tmp (
  mod_job_id integer NOT NULL,
  alias_id integer NOT NULL,
  use_case_name text,
  target smallint
) distributed by (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.module_targets_tmp OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.module_targets_tmp', 'work.module_sequence', NULL, 10);


-- table for storing models
CREATE TABLE work.module_models (
  model_id integer NOT NULL,
  aid integer DEFAULT 0,
  bid integer DEFAULT 0,
  output_id integer DEFAULT 0,
  "key" text NOT NULL,
  "value" text
) distributed by (model_id) ;
ALTER TABLE work.module_models OWNER TO xsl;


-- work.module_job_parameters contains module job related information
CREATE TABLE work.module_job_parameters
(
  mod_job_id integer NOT NULL,
  key text NOT NULL,
  value text,
  PRIMARY KEY (mod_job_id, key)
) distributed by (mod_job_id); 
ALTER TABLE work.module_job_parameters OWNER TO xsl;

-- work.scoring_muc_parameters contains scoring related parameters
CREATE TABLE work.scoring_muc_parameters
(
  scoring_paras text NOT NULL,
  para_value text,
  use_case text,
  --insert_date DATE,
  CONSTRAINT scoring_muc_parameters_pkey PRIMARY KEY (use_case, scoring_paras)
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (use_case);
ALTER TABLE work.scoring_muc_parameters
  OWNER TO xsl;

  
INSERT INTO work.scoring_muc_parameters
VALUES
 ( 'source_period_length',           '84',  'common'), -- 7*12 days
 ( 'post_source_period_length',      '56',  'common'),  -- 7*8 days, 8 weeks available for e.g. gap, target and evaluation periods when fitting
 ( 'lag_count',                      '4',   'common'),
 ( 'max_data_age',                   '15',  'common'),  -- CDR and top-up data can not be older
 ( 'crm_data_age',                   '40',  'common'),  -- CRM data can not be older
 ( 'n_handset_topics',               '6',   'common'),
 ( 'n_cell_events_topics',           '6',   'common'),
 ( 'gap_length',                     '7',   'inactivity'), -- 7*1 days
 ( 'target_period_length',           '14',  'inactivity'), -- 7*2 days
 ( 'gap2_length',                    '0',   'inactivity'), -- 7*0 days
 ( 'evaluation_period_length',       '28',  'inactivity'), -- 7*4 days
 ( 'gap_length',                     '7',   'postpaid'), -- 7*1 days
 ( 'target_period_length',           '14',  'postpaid'), -- 7*2 days
 ( 'uc_zero_day_prediction_evaluation_period_length',           '56',  'zero_day'), -- 7*8 days
 ( 'gap_length',                     '7',   'product'), -- 7*0 days
 ( 'target_period_length',           '14',  'product'); -- 7*4 days 
 --( '1', 'override_model_variables_check', 'true');-- Force all predictors to be calculated in work.create_modelling_data1/2/3


-- work.module_model_output contains the output of the ModelApplier
CREATE TABLE work.module_model_output
(
  mod_job_id integer NOT NULL,
  alias_id integer NOT NULL,
  output_id integer,
  "value" double precision,
  model_id integer
)
WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.module_model_output OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.module_model_output', 'work.module_sequence', 5);

-- work.module_model_output_variable_group contains variable group specific output of the ModelApplier if enabled
CREATE TABLE work.module_model_output_variable_group
(
  mod_job_id integer NOT NULL,
  alias_id integer NOT NULL,
  output_id integer,
  vargroup text,
  value real,
  model_id integer
)
WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.module_model_output_variable_group OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.module_model_output_variable_group', 'work.module_sequence', 5);

-- For temporarily storing the vargroup specific scores of different use cases in stack format (score_name-score_value)
CREATE TABLE tmp.module_results_vargroup_tmp (
  mod_job_id integer,
  model_id integer,
  alias_id integer,
  score_base_name text,
  vargroup text,
  score_value double precision,
  vargroup_rank integer
)
WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.module_results_vargroup_tmp OWNER TO xsl; 

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.module_results_vargroup_tmp', 'work.module_sequence', 5, 10);

-- work.variable_group_balance_factors contains balance factors necessary for calculating
-- vargroup specific propensity scores
CREATE TABLE work.variable_group_balance_factors
(
  mod_job_id integer NOT NULL,
  model_id integer,
  output_id integer,
  vargroup text,
  balance_factor real
)
DISTRIBUTED BY (mod_job_id);
ALTER TABLE work.variable_group_balance_factors OWNER TO xsl;

CREATE TABLE results.module_results_variable_group
(
  mod_job_id integer NOT NULL,
  alias_id integer NOT NULL,
  churn_inactivity_driver text,
  churn_inactivity_adjusted_score real,
  churn_postpaid_driver text,
  churn_postpaid_adjusted_score real
)
WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.module_results_variable_group OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('results.module_results_variable_group', 'work.module_sequence', 5);


-- module_modelling_execeptions is used for passing exception information to the Influencer
CREATE TABLE work.module_modelling_exceptions
(
  mod_job_id integer NOT NULL,
  alias_id integer NOT NULL,
  xcept integer
) distributed by (alias_id);
ALTER TABLE work.module_modelling_exceptions OWNER TO xsl;

-- results.module_results is the internal results table
-- One needs to add new columns for the output of new use cases
CREATE TABLE results.module_results (
  mod_job_id integer,
  alias_id integer,
  churn_inactivity_propensity_score double precision,
  churn_postpaid_propensity_score double precision,
  churn_inactivity_expected_revenue_loss double precision,
  campaign_score double precision,
  campaign_total_score double precision,
  product_product_x_propensity_score double precision,
  product_product_y_propensity_score double precision,
  zero_day_prediction_tenure_group integer,
  zero_day_prediction_predicted_segment integer,
  zero_day_prediction_low_value_propensity_score double precision,
  zero_day_prediction_medium_value_propensity_score double precision,
  zero_day_prediction_high_value_propensity_score double precision,
  churn_postpaid_expected_revenue_loss double precision
)
WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.module_results OWNER TO xsl; 


INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, cleanup) 
VALUES ('results.module_results', 'work.module_sequence', 5, FALSE);

-- For temporarily storing the scores of different use cases in stack format (score_name-score_value)
CREATE TABLE tmp.module_results_tmp (
  mod_job_id integer,
  model_id integer,
  alias_id integer,
  score_name text,
  score_value double precision
)
WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.module_results_tmp OWNER TO xsl; 

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.module_results_tmp', 'work.module_sequence', 5, 10);

-- work.module_influences is the internal result table for influences
CREATE TABLE work.module_influences
(
  mod_job_id integer,
  model_id integer,
  alias_id integer,
  influence double precision
)
WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.module_influences OWNER TO xsl; 

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.module_influences', 'work.module_sequence', 5);

-- network roles
CREATE TABLE work.network_roles
(
  job_id integer, 
  alias_id integer, 
  ishub integer, 
  isbridge integer, 
  isoutlier integer
) distributed by (alias_id)
PARTITION BY RANGE (job_id) (PARTITION "0" START (0) END (1)); 
ALTER TABLE work.network_roles owner to xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.network_roles', 'work.network_sequence', NULL, 10);


-- work.model_sequence is used for getting the next model_id
CREATE sequence work.model_sequence
INCREMENT 1
MINVALUE 1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER TABLE work.model_sequence OWNER TO xsl;


-- work.module_sequence is used for getting the next module_job_id
CREATE sequence work.module_sequence
INCREMENT 1
MINVALUE 1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER TABLE work.module_sequence OWNER TO xsl;


/*
 * work.modelling_data_matrix... functions: 
 * 
 * The create modelling data functions insert data into work.modelling_data_matrix_...
 * tables which are joined to work.module_targets and inserted into create work.modelling_data_matrix. 
 */

/*
 * Predictors created in function work.create_modelling_data1 are inserted here.
 * Contains variables calculated from data.in_split_aggregates table.
 */ 
CREATE TABLE work.modelling_data_matrix_1 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  alias_count                                                        double precision  ,
  callsmsratio                                                       double precision  ,
  mr_count_ratio                                                     double precision  ,
  mr_ratio                                                           double precision  ,
  smscount                                                           double precision  ,
  smscountday                                                        double precision  ,
  smscountevening                                                    double precision  ,
  smscountweekday                                                    double precision  ,
  voicecount                                                         double precision  ,
  voicecountday                                                      double precision  ,
  voicecountevening                                                  double precision  ,
  voicecountweekday                                                  double precision  ,
  voicesum                                                           double precision  ,
  voicesumday                                                        double precision  ,
  voicesumevening                                                    double precision  ,
  voicesumweekday                                                    double precision  ,
  week_entropy                                                       double precision  ,
  week_mean                                                          double precision  
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_1 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_1', 'work.module_sequence', 5, 0);


/*
 * Predictors created in function work.create_modelling_data2 are inserted here.
 * Contains variables variables that are aggregates over neighbour data 
 * utilizing data from data.in_split_aggregates and work.out_scores.
 */ 
CREATE TABLE work.modelling_data_matrix_2 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  knn                                                                double precision  ,
  knn_2                                                              double precision  ,
  k_offnet                                                           double precision  ,
  k_target                                                           double precision
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_2 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_2', 'work.module_sequence', 5, 0);


/*
 * Predictors created in function work.create_modelling_data3 are inserted here.
 * Contains variables variables from data.in_crm and work.out_scores.
 */ 
CREATE TABLE work.modelling_data_matrix_3 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  age                                                                double precision  ,
  alpha                                                              double precision  ,
  c                                                                  double precision  ,
  alpha_2                                                            double precision  ,
  c_2                                                                double precision  ,
  churn_score                                                        double precision  ,
  contr_length                                                       double precision  ,
  contr_remain                                                       double precision  ,
  country                                                            text              ,  
  gender                                                             text              ,  
  handset_age                                                        double precision  ,
  handset_model                                                      text              ,
  k                                                                  double precision  ,
  kshell                                                             double precision  ,
  k_2                                                                double precision  ,
  kshell_2                                                           double precision  ,
  language                                                           text              ,
  monthly_arpu                                                       double precision  ,
  no_churn_score                                                     double precision  ,
  payment_type                                                       text              ,
  separate_node                                                      double precision  ,
  separate_node_2                                                    double precision  ,
  socrev                                                             double precision  ,
  socrevest                                                          double precision  ,
  socrev_2                                                           double precision  ,
  socrevest_2                                                        double precision  ,
  subscriber_segment                                                 text              ,
  subscriber_value                                                   double precision  ,
  subscription_type                                                  text              ,
  tariff_plan                                                        text              ,
  wec                                                                double precision  ,
  wec_2                                                              double precision  ,
  zip                                                                text              
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_3 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_3', 'work.module_sequence', 5, 0);


/*
 * Predictors created in function work.create_modelling_data4_made are inserted here.
 * Contains time-lagged aggregates for different made call types from data.call_types_weekly table.
 */ 
CREATE TABLE work.modelling_data_matrix_4_made (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  daily_voice_activity1                                              double precision  ,
  daily_voice_activity2                                              double precision  ,
  daily_voice_activity3                                              double precision  ,
  daily_voice_activity4                                              double precision  ,
  daily_sms_activity1                                                double precision  ,
  daily_sms_activity2                                                double precision  ,
  daily_sms_activity3                                                double precision  ,
  daily_sms_activity4                                                double precision  ,
  daily_data_activity1                                               double precision  ,  
  daily_data_activity2                                               double precision  ,  
  daily_data_activity3                                               double precision  ,  
  daily_data_activity4                                               double precision  ,  
  weekly_data_usage1                                                 double precision  ,
  weekly_data_usage2                                                 double precision  ,
  weekly_data_usage3                                                 double precision  ,
  weekly_data_usage4                                                 double precision  ,
  weekly_voice_neigh_count1                                          double precision  ,  
  weekly_voice_neigh_count2                                          double precision  ,  
  weekly_voice_neigh_count3                                          double precision  ,  
  weekly_voice_neigh_count4                                          double precision  ,  
  weekly_voice_count1                                                double precision  ,  
  weekly_voice_count2                                                double precision  ,  
  weekly_voice_count3                                                double precision  ,  
  weekly_voice_count4                                                double precision  ,  
  weekly_sms_count1                                                  double precision  ,  
  weekly_sms_count2                                                  double precision  ,  
  weekly_sms_count3                                                  double precision  ,  
  weekly_sms_count4                                                  double precision  ,  
  weekly_data_count1                                                 double precision  ,  
  weekly_data_count2                                                 double precision  ,  
  weekly_data_count3                                                 double precision  ,  
  weekly_data_count4                                                 double precision  ,  
  weekly_voice_duration1                                             double precision  ,  
  weekly_voice_duration2                                             double precision  ,  
  weekly_voice_duration3                                             double precision  ,  
  weekly_voice_duration4                                             double precision  ,  
  weekly_voice_cost1                                                 double precision  ,  
  weekly_voice_cost2                                                 double precision  ,  
  weekly_voice_cost3                                                 double precision  ,  
  weekly_voice_cost4                                                 double precision  ,  
  weekly_sms_cost1                                                   double precision  ,  
  weekly_sms_cost2                                                   double precision  ,  
  weekly_sms_cost3                                                   double precision  ,  
  weekly_sms_cost4                                                   double precision  ,  
  weekly_cost1                                                       double precision  ,  
  weekly_cost2                                                       double precision  ,  
  weekly_cost3                                                       double precision  ,  
  weekly_cost4                                                       double precision  ,  
  weekly_cell_id_count1                                              double precision  ,  
  weekly_cell_id_count2                                              double precision  ,  
  weekly_cell_id_count3                                              double precision  ,  
  weekly_cell_id_count4                                              double precision  ,
  inact                                                              double precision
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_4_made OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_4_made', 'work.module_sequence', 5, 0);



/*
 * Predictors created in function work.create_modelling_data4_rec are inserted here.
 * Contains time-lagged aggregates for different received call types from data.call_types_weekly table.
 */ 
CREATE TABLE work.modelling_data_matrix_4_rec (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  daily_voice_activity_rec1                                          double precision  ,  
  daily_voice_activity_rec2                                          double precision  ,  
  daily_voice_activity_rec3                                          double precision  ,  
  daily_voice_activity_rec4                                          double precision  ,  
  daily_sms_activity_rec1                                            double precision  ,  
  daily_sms_activity_rec2                                            double precision  ,  
  daily_sms_activity_rec3                                            double precision  ,  
  daily_sms_activity_rec4                                            double precision  ,  
  weekly_voice_count_rec1                                            double precision  ,  
  weekly_voice_count_rec2                                            double precision  ,  
  weekly_voice_count_rec3                                            double precision  ,  
  weekly_voice_count_rec4                                            double precision  ,  
  weekly_sms_count_rec1                                              double precision  ,  
  weekly_sms_count_rec2                                              double precision  ,  
  weekly_sms_count_rec3                                              double precision  ,  
  weekly_sms_count_rec4                                              double precision  ,   
  weekly_voice_duration_rec1                                         double precision  ,  
  weekly_voice_duration_rec2                                         double precision  ,  
  weekly_voice_duration_rec3                                         double precision  ,  
  weekly_voice_duration_rec4                                         double precision  ,  
  weekly_cell_id_count_rec1                                          double precision  ,  
  weekly_cell_id_count_rec2                                          double precision  ,  
  weekly_cell_id_count_rec3                                          double precision  ,  
  weekly_cell_id_count_rec4                                          double precision  ,
  inact_rec                                                          double precision
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_4_rec OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_4_rec', 'work.module_sequence', 5, 0);



/*
 * Predictors created in function work.create_modelling_data_topup1 are inserted here.
 * Contains time-lagged aggregates for topup data from data.topup table.
 */ 
CREATE TABLE work.modelling_data_matrix_topup1 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  topup_amount_avg1                                                  double precision  ,
  topup_amount_avg2                                                  double precision  ,
  topup_amount_avg3                                                  double precision  ,
  topup_amount_avg4                                                  double precision  ,
  topup_bonus_avg1                                                   double precision  ,
  topup_bonus_avg2                                                   double precision  ,
  topup_bonus_avg3                                                   double precision  ,
  topup_bonus_avg4                                                   double precision  ,
  topup_bonus_per_amount_avg1                                        double precision  ,
  topup_bonus_per_amount_avg2                                        double precision  ,
  topup_bonus_per_amount_avg3                                        double precision  ,
  topup_bonus_per_amount_avg4                                        double precision  ,
  topup_count_weekly1                                                double precision  ,
  topup_count_weekly2                                                double precision  ,
  topup_count_weekly3                                                double precision  ,
  topup_count_weekly4                                                double precision  ,
  topup_free_avg1                                                    double precision  ,  
  topup_free_avg2                                                    double precision  ,  
  topup_free_avg3                                                    double precision  ,  
  topup_free_avg4                                                    double precision  ,  
  topup_free_count_weekly1                                           double precision  ,  
  topup_free_count_weekly2                                           double precision  ,  
  topup_free_count_weekly3                                           double precision  ,  
  topup_free_count_weekly4                                           double precision  
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_topup1 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_topup1', 'work.module_sequence', 5, 0);


/*
 * Predictors created in function work.create_modelling_data_topup2 are inserted here.
 * Contains predictors calculated from data.topup table.
 */ 
CREATE TABLE work.modelling_data_matrix_topup2 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  topup_days_from_last                                               double precision  ,
  topup_days_to_next1                                                double precision  ,
  topup_days_to_next2                                                double precision  ,
  topup_days_interval                                                double precision
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_topup2 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_topup2', 'work.module_sequence', 5, 0);


/*
 * Predictors created in function work.create_modelling_data_topup_channels are inserted here.
 * The function work.create_modelling_data_topup_channels needs to be customized, i.e.,
 * by default nothing is calculated there. Column example_channel_1 is just an example of 
 * a topup channel name. 
 */ 
CREATE TABLE work.modelling_data_matrix_topup_channel (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  example_channel_1                                                  double precision
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_topup_channel OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_topup_channel', 'work.module_sequence', 5, 0);


/*
 * Predictors created in function work.create_modelling_data_topup3 are inserted here.
 * Contains predictors from topup behavior during different times of day.
 */ 
CREATE TABLE work.modelling_data_matrix_topup3 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  topup_count_weekly_daytime1                                        double precision  ,  
  topup_count_weekly_daytime2                                        double precision  ,  
  topup_count_weekly_daytime3                                        double precision  ,  
  topup_count_weekly_daytime4                                        double precision  ,  
  topup_count_weekly_evening1                                        double precision  ,  
  topup_count_weekly_evening2                                        double precision  ,  
  topup_count_weekly_evening3                                        double precision  ,  
  topup_count_weekly_evening4                                        double precision  ,  
  topup_count_weekly_nighttime1                                      double precision  ,  
  topup_count_weekly_nighttime2                                      double precision  ,  
  topup_count_weekly_nighttime3                                      double precision  ,  
  topup_count_weekly_nighttime4                                      double precision  ,  
  topup_count_weekly_weekend1                                        double precision  ,  
  topup_count_weekly_weekend2                                        double precision  ,  
  topup_count_weekly_weekend3                                        double precision  ,  
  topup_count_weekly_weekend4                                        double precision 
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_topup3 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_topup3', 'work.module_sequence', 5, 0);



/*
 * Predictors created in function work.create_modelling_data_topup3 are inserted here.
 * Contains predictors combining topup and CDR data. 
 * (The calculation of these predictors has been currently disabled). 
 */ 
CREATE TABLE work.modelling_data_matrix_topup3a (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  topup_interval_from_last_call_avg1                                 double precision  ,  
  topup_interval_from_last_call_avg2                                 double precision  ,  
  topup_interval_from_last_call_avg3                                 double precision  ,  
  topup_interval_from_last_call_avg4                                 double precision  ,  
  topup_interval_from_last_sms_avg1                                  double precision  ,  
  topup_interval_from_last_sms_avg2                                  double precision  ,  
  topup_interval_from_last_sms_avg3                                  double precision  ,  
  topup_interval_from_last_sms_avg4                                  double precision  ,  
  topup_interval_to_next_call_avg1                                   double precision  ,  
  topup_interval_to_next_call_avg2                                   double precision  ,  
  topup_interval_to_next_call_avg3                                   double precision  ,  
  topup_interval_to_next_call_avg4                                   double precision  ,  
  topup_interval_to_next_sms_avg1                                    double precision  ,  
  topup_interval_to_next_sms_avg2                                    double precision  ,  
  topup_interval_to_next_sms_avg3                                    double precision  ,  
  topup_interval_to_next_sms_avg4                                    double precision  
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_topup3a OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_topup3a', 'work.module_sequence', 5, 0);



/*
 * Predictors created in function work.create_modelling_data_handset_model are inserted here.
 * Contains the LDA coefficient for the handset of the subscriber. 
 */ 
CREATE TABLE work.modelling_data_matrix_handset_topic (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  handset_topic_1                                                    double precision  ,
  handset_topic_2                                                    double precision  ,
  handset_topic_3                                                    double precision  ,
  handset_topic_4                                                    double precision  ,
  handset_topic_5                                                    double precision  
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_handset_topic OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_handset_topic', 'work.module_sequence', 5, 0);



/*
 * Part of the predictors created in function work.uc_zero_day_prediction_create_predictors 
 * are inserted here. Contains predictors calculated from data.cdr.
 */ 
CREATE TABLE work.modelling_data_matrix_zdp1 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  uc_zero_day_prediction_inactive_to_active_days_ratio               double precision  ,
  uc_zero_day_prediction_weekend_to_weekday_voice_count_ratio        double precision  ,
  uc_zero_day_prediction_evening_to_daytime_ratio_voice_made         double precision  ,
  uc_zero_day_prediction_nighttime_to_daytime_ratio_voice_made       double precision  ,
  uc_zero_day_prediction_average_duration_daytime_voice_made         double precision  ,
  uc_zero_day_prediction_average_duration_evening_voice_made         double precision  ,
  uc_zero_day_prediction_average_duration_nighttime_voice_made       double precision  ,
  uc_zero_day_prediction_average_duration_weekend_voice_made         double precision  ,
  uc_zero_day_prediction_average_duration_weekday_voice_made         double precision  ,
  uc_zero_day_prediction_average_daily_count_weekday_voice_made      double precision  ,
  uc_zero_day_prediction_average_daily_count_weekend_voice_made      double precision  ,
  uc_zero_day_prediction_average_daily_count_sms_made                double precision  ,
  uc_zero_day_prediction_sms_to_voice_ratio                          double precision  ,
  uc_zero_day_prediction_average_first_voice_hour                    double precision  ,
  uc_zero_day_prediction_average_daily_voice_hour                    double precision 
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_zdp1 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_zdp1', 'work.module_sequence', 5, 0);



/*
 * Part of the predictors created in function work.uc_zero_day_prediction_create_predictors 
 * are inserted here. Contains predictors calculated from data.topup.
 */ 
CREATE TABLE work.modelling_data_matrix_zdp2 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  uc_zero_day_prediction_average_daily_topup_count                   double precision  ,
  uc_zero_day_prediction_total_topup_count_capped_at_two             integer           ,
  uc_zero_day_prediction_average_topup_cost                          double precision  
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_zdp2 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_zdp2', 'work.module_sequence', 5, 0);


/*
 * Part of the predictors created in function work.uc_zero_day_prediction_create_predictors 
 * are inserted here. Contains predictors calculated from data.in_crm.
 */ 
CREATE TABLE work.modelling_data_matrix_zdp4 (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  uc_zero_day_prediction_tenure                                      double precision  ,
  uc_zero_day_prediction_tenure_group                                integer           ,
  uc_zero_day_prediction_activation_weekday                          text              
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_zdp4 OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_zdp4', 'work.module_sequence', 5, 0);


/*
 * The final modelling data matrix where all the work.modelling_data_matrix_... submatrices are joined. 
 */ 
CREATE TABLE work.modelling_data_matrix (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  target_churn_inactivity                                            double precision  ,
  audience_churn_inactivity                                          double precision  ,
  target_churn_postpaid                                              double precision  ,
  audience_churn_postpaid                                            double precision  ,
  target_zero_day_prediction                                         double precision  ,
  audience_zero_day_prediction                                       double precision  ,
  target_product_product_x                                           double precision  ,
  audience_product_product_x                                         double precision  ,
  target_product_product_y                                           double precision  ,
  audience_product_product_y                                         double precision  ,
  alias_count                                                        double precision  ,
  callsmsratio                                                       double precision  ,
  mr_count_ratio                                                     double precision  ,
  mr_ratio                                                           double precision  ,
  smscount                                                           double precision  ,
  smscountday                                                        double precision  ,
  smscountevening                                                    double precision  ,
  smscountweekday                                                    double precision  ,
  voicecount                                                         double precision  ,
  voicecountday                                                      double precision  ,
  voicecountevening                                                  double precision  ,
  voicecountweekday                                                  double precision  ,
  voicesum                                                           double precision  ,
  voicesumday                                                        double precision  ,
  voicesumevening                                                    double precision  ,
  voicesumweekday                                                    double precision  ,
  week_entropy                                                       double precision  ,
  week_mean                                                          double precision  ,
  knn                                                                double precision  ,
  knn_2                                                              double precision  ,
  k_offnet                                                           double precision  ,
  k_target                                                           double precision  ,
  age                                                                double precision  ,
  alpha                                                              double precision  ,
  c                                                                  double precision  ,
  alpha_2                                                            double precision  ,
  c_2                                                                double precision  ,
  churn_score                                                        double precision  ,
  contr_length                                                       double precision  ,
  contr_remain                                                       double precision  ,
  country                                                            text              ,  
  gender                                                             text              ,  
  handset_age                                                        double precision  ,
  handset_model                                                      text              ,
  k                                                                  double precision  ,
  kshell                                                             double precision  ,
  k_2                                                                double precision  ,
  kshell_2                                                           double precision  ,
  language                                                           text              ,
  monthly_arpu                                                       double precision  ,
  no_churn_score                                                     double precision  ,
  payment_type                                                       text              ,
  separate_node                                                      double precision  ,
  separate_node_2                                                    double precision  ,
  socrev                                                             double precision  ,
  socrevest                                                          double precision  ,
  socrev_2                                                           double precision  ,
  socrevest_2                                                        double precision  ,
  subscriber_segment                                                 text              ,
  subscriber_value                                                   double precision  ,
  subscription_type                                                  text              ,
  tariff_plan                                                        text              ,
  wec                                                                double precision  ,
  wec_2                                                              double precision  ,
  zip                                                                text              ,
  daily_voice_activity1                                              double precision  ,
  daily_voice_activity2                                              double precision  ,
  daily_voice_activity3                                              double precision  ,
  daily_voice_activity4                                              double precision  ,
  daily_sms_activity1                                                double precision  ,
  daily_sms_activity2                                                double precision  ,
  daily_sms_activity3                                                double precision  ,
  daily_sms_activity4                                                double precision  ,
  daily_data_activity1                                               double precision  ,  
  daily_data_activity2                                               double precision  ,  
  daily_data_activity3                                               double precision  ,  
  daily_data_activity4                                               double precision  ,  
  weekly_data_usage1                                                 double precision  ,
  weekly_data_usage2                                                 double precision  ,
  weekly_data_usage3                                                 double precision  ,
  weekly_data_usage4                                                 double precision  ,
  weekly_voice_neigh_count1                                          double precision  ,  
  weekly_voice_neigh_count2                                          double precision  ,  
  weekly_voice_neigh_count3                                          double precision  ,  
  weekly_voice_neigh_count4                                          double precision  ,  
  weekly_voice_count1                                                double precision  ,  
  weekly_voice_count2                                                double precision  ,  
  weekly_voice_count3                                                double precision  ,  
  weekly_voice_count4                                                double precision  ,  
  weekly_sms_count1                                                  double precision  ,  
  weekly_sms_count2                                                  double precision  ,  
  weekly_sms_count3                                                  double precision  ,  
  weekly_sms_count4                                                  double precision  ,  
  weekly_data_count1                                                 double precision  ,  
  weekly_data_count2                                                 double precision  ,  
  weekly_data_count3                                                 double precision  ,  
  weekly_data_count4                                                 double precision  ,  
  weekly_voice_duration1                                             double precision  ,  
  weekly_voice_duration2                                             double precision  ,  
  weekly_voice_duration3                                             double precision  ,  
  weekly_voice_duration4                                             double precision  ,  
  weekly_voice_cost1                                                 double precision  ,  
  weekly_voice_cost2                                                 double precision  ,  
  weekly_voice_cost3                                                 double precision  ,  
  weekly_voice_cost4                                                 double precision  ,  
  weekly_sms_cost1                                                   double precision  ,  
  weekly_sms_cost2                                                   double precision  ,  
  weekly_sms_cost3                                                   double precision  ,  
  weekly_sms_cost4                                                   double precision  ,  
  weekly_cost1                                                       double precision  ,  
  weekly_cost2                                                       double precision  ,  
  weekly_cost3                                                       double precision  ,  
  weekly_cost4                                                       double precision  ,  
  weekly_cell_id_count1                                              double precision  ,  
  weekly_cell_id_count2                                              double precision  ,  
  weekly_cell_id_count3                                              double precision  ,  
  weekly_cell_id_count4                                              double precision  ,
  inact                                                              double precision  ,
  daily_voice_activity_rec1                                          double precision  ,  
  daily_voice_activity_rec2                                          double precision  ,  
  daily_voice_activity_rec3                                          double precision  ,  
  daily_voice_activity_rec4                                          double precision  ,  
  daily_sms_activity_rec1                                            double precision  ,  
  daily_sms_activity_rec2                                            double precision  ,  
  daily_sms_activity_rec3                                            double precision  ,  
  daily_sms_activity_rec4                                            double precision  ,  
  weekly_voice_count_rec1                                            double precision  ,  
  weekly_voice_count_rec2                                            double precision  ,  
  weekly_voice_count_rec3                                            double precision  ,  
  weekly_voice_count_rec4                                            double precision  ,  
  weekly_sms_count_rec1                                              double precision  ,  
  weekly_sms_count_rec2                                              double precision  ,  
  weekly_sms_count_rec3                                              double precision  ,  
  weekly_sms_count_rec4                                              double precision  ,   
  weekly_voice_duration_rec1                                         double precision  ,  
  weekly_voice_duration_rec2                                         double precision  ,  
  weekly_voice_duration_rec3                                         double precision  ,  
  weekly_voice_duration_rec4                                         double precision  ,  
  weekly_cell_id_count_rec1                                          double precision  ,  
  weekly_cell_id_count_rec2                                          double precision  ,  
  weekly_cell_id_count_rec3                                          double precision  ,  
  weekly_cell_id_count_rec4                                          double precision  ,
  inact_rec                                                          double precision  ,
  topup_amount_avg1                                                  double precision  ,
  topup_amount_avg2                                                  double precision  ,
  topup_amount_avg3                                                  double precision  ,
  topup_amount_avg4                                                  double precision  ,
  topup_bonus_avg1                                                   double precision  ,
  topup_bonus_avg2                                                   double precision  ,
  topup_bonus_avg3                                                   double precision  ,
  topup_bonus_avg4                                                   double precision  ,
  topup_bonus_per_amount_avg1                                        double precision  ,
  topup_bonus_per_amount_avg2                                        double precision  ,
  topup_bonus_per_amount_avg3                                        double precision  ,
  topup_bonus_per_amount_avg4                                        double precision  ,
  topup_count_weekly1                                                double precision  ,
  topup_count_weekly2                                                double precision  ,
  topup_count_weekly3                                                double precision  ,
  topup_count_weekly4                                                double precision  ,
  topup_free_avg1                                                    double precision  ,  
  topup_free_avg2                                                    double precision  ,  
  topup_free_avg3                                                    double precision  ,  
  topup_free_avg4                                                    double precision  ,  
  topup_free_count_weekly1                                           double precision  ,  
  topup_free_count_weekly2                                           double precision  ,  
  topup_free_count_weekly3                                           double precision  ,  
  topup_free_count_weekly4                                           double precision  ,
  topup_days_from_last                                               double precision  ,
  topup_days_to_next1                                                double precision  ,
  topup_days_to_next2                                                double precision  ,
  topup_days_interval                                                double precision  ,
  topup_count_weekly_daytime1                                        double precision  ,  
  topup_count_weekly_daytime2                                        double precision  ,  
  topup_count_weekly_daytime3                                        double precision  ,  
  topup_count_weekly_daytime4                                        double precision  ,  
  topup_count_weekly_evening1                                        double precision  ,  
  topup_count_weekly_evening2                                        double precision  ,  
  topup_count_weekly_evening3                                        double precision  ,  
  topup_count_weekly_evening4                                        double precision  ,  
  topup_count_weekly_nighttime1                                      double precision  ,  
  topup_count_weekly_nighttime2                                      double precision  ,  
  topup_count_weekly_nighttime3                                      double precision  ,  
  topup_count_weekly_nighttime4                                      double precision  ,  
  topup_count_weekly_weekend1                                        double precision  ,  
  topup_count_weekly_weekend2                                        double precision  ,  
  topup_count_weekly_weekend3                                        double precision  ,  
  topup_count_weekly_weekend4                                        double precision  ,
  topup_interval_from_last_call_avg1                                 double precision  ,  
  topup_interval_from_last_call_avg2                                 double precision  ,  
  topup_interval_from_last_call_avg3                                 double precision  ,  
  topup_interval_from_last_call_avg4                                 double precision  ,  
  topup_interval_from_last_sms_avg1                                  double precision  ,  
  topup_interval_from_last_sms_avg2                                  double precision  ,  
  topup_interval_from_last_sms_avg3                                  double precision  ,  
  topup_interval_from_last_sms_avg4                                  double precision  ,  
  topup_interval_to_next_call_avg1                                   double precision  ,  
  topup_interval_to_next_call_avg2                                   double precision  ,  
  topup_interval_to_next_call_avg3                                   double precision  ,  
  topup_interval_to_next_call_avg4                                   double precision  ,  
  topup_interval_to_next_sms_avg1                                    double precision  ,  
  topup_interval_to_next_sms_avg2                                    double precision  ,  
  topup_interval_to_next_sms_avg3                                    double precision  ,  
  topup_interval_to_next_sms_avg4                                    double precision  ,
  handset_topic_1                                                    double precision  ,
  handset_topic_2                                                    double precision  ,
  handset_topic_3                                                    double precision  ,
  handset_topic_4                                                    double precision  ,
  handset_topic_5                                                    double precision  ,
  cell_events_topic_1_1                                              double precision  ,
  cell_events_topic_1_2                                              double precision  ,
  cell_events_topic_2_1                                              double precision  ,
  cell_events_topic_2_2                                              double precision  ,
  cell_events_topic_3_1                                              double precision  ,
  cell_events_topic_3_2                                              double precision  ,
  cell_events_topic_4_1                                              double precision  ,
  cell_events_topic_4_2                                              double precision  ,
  cell_events_topic_5_1                                              double precision  ,
  cell_events_topic_5_2                                              double precision  ,
  mobility                                                           double precision  ,
  alltime_long_diversity                                             double precision  ,
  leisure_long_diversity                                             double precision  ,
  business_long_diversity                                            double precision  ,
  alltime_short_diversity                                            double precision  ,
  leisure_short_diversity                                            double precision  ,
  business_short_diversity                                           double precision  ,
  uc_zero_day_prediction_inactive_to_active_days_ratio               double precision  ,
  uc_zero_day_prediction_weekend_to_weekday_voice_count_ratio        double precision  ,
  uc_zero_day_prediction_evening_to_daytime_ratio_voice_made         double precision  ,
  uc_zero_day_prediction_nighttime_to_daytime_ratio_voice_made       double precision  ,
  uc_zero_day_prediction_average_duration_daytime_voice_made         double precision  ,
  uc_zero_day_prediction_average_duration_evening_voice_made         double precision  ,
  uc_zero_day_prediction_average_duration_nighttime_voice_made       double precision  ,
  uc_zero_day_prediction_average_duration_weekend_voice_made         double precision  ,
  uc_zero_day_prediction_average_duration_weekday_voice_made         double precision  ,
  uc_zero_day_prediction_average_daily_count_weekday_voice_made      double precision  ,
  uc_zero_day_prediction_average_daily_count_weekend_voice_made      double precision  ,
  uc_zero_day_prediction_average_daily_count_sms_made                double precision  ,
  uc_zero_day_prediction_sms_to_voice_ratio                          double precision  ,
  uc_zero_day_prediction_average_first_voice_hour                    double precision  ,
  uc_zero_day_prediction_average_daily_voice_hour                    double precision  ,
  uc_zero_day_prediction_average_daily_topup_count                   double precision  ,
  uc_zero_day_prediction_total_topup_count_capped_at_two             integer           ,
  uc_zero_day_prediction_average_topup_cost                          double precision  ,
  uc_zero_day_prediction_tenure                                      double precision  ,
  uc_zero_day_prediction_tenure_group                                integer           ,
  uc_zero_day_prediction_activation_weekday                          text              
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix OWNER TO xsl;


INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.modelling_data_matrix', 'work.module_sequence', 5);


CREATE TABLE work.modelling_data_matrix_cell_events_topic (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  cell_events_topic_1_1                                              double precision  ,
  cell_events_topic_1_2                                              double precision  ,
  cell_events_topic_2_1                                              double precision  ,
  cell_events_topic_2_2                                              double precision  ,
  cell_events_topic_3_1                                              double precision  ,
  cell_events_topic_3_2                                              double precision  ,
  cell_events_topic_4_1                                              double precision  ,
  cell_events_topic_4_2                                              double precision  ,
  cell_events_topic_5_1                                              double precision  ,
  cell_events_topic_5_2                                              double precision  
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_cell_events_topic OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_cell_events_topic', 'work.module_sequence', 5, 0);



-- temporary tables 

create table tmp.nwr_linkcount_tmp(alias_id integer, lc integer) distributed by (alias_id);
create table tmp.nwr_linkcount(alias_id integer, lc integer) distributed by (alias_id);
create table tmp.hublist (alias_id integer) distributed by (alias_id)	;
create table tmp.bridgelist (alias_id integer) distributed by (alias_id)	;
create table tmp.network_roles_fix(job_id integer,alias_id integer , ishub integer, isbridge integer, isoutlier integer) distributed by (alias_id);


alter table tmp.nwr_linkcount_tmp owner to xsl;
alter table tmp.nwr_linkcount owner to xsl;
alter table tmp.hublist owner to xsl;
alter table tmp.bridgelist owner to xsl;
alter table tmp.network_roles_fix owner to xsl;


-- Statistics to evaluate model fitting
-- Contains model_id to differentiate between use cases
CREATE TABLE results.module_results_verification (
  mod_job_id integer,
  model_id integer, 
  chart text,
  id integer,
  key text,
  value double precision,
  PRIMARY KEY (mod_job_id, model_id, chart, id, key)
);
ALTER TABLE results.module_results_verification OWNER TO xsl;


-- Blacklist table used by network scorer to store aliases with connections > blacklist_link_limit
CREATE TABLE work.networkscorer_blacklist (
  job_id integer,
  alias_id integer,
  k integer
) DISTRIBUTED BY (alias_id);
ALTER TABLE work.networkscorer_blacklist OWNER TO xsl;


-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriberlist
CREATE TABLE tmp.descriptive_variables_subscriberlist (
  alias_id integer
) DISTRIBUTED by (alias_id);
ALTER TABLE tmp.descriptive_variables_subscriberlist OWNER TO xsl;


-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: work.descriptive_variables_community
CREATE TABLE work.descriptive_variables_community (
  mod_job_id integer,
  community_id integer,
  var_id integer,
  "value" text
) WITH (OIDS=FALSE, APPENDONLY = TRUE, COMPRESSLEVEL=5) 
DISTRIBUTED BY (community_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.descriptive_variables_community OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.descriptive_variables_community', 'work.module_sequence', 5, 10);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: work.descriptive_variables_list
CREATE TABLE work.descriptive_variables_list (
  var_id serial,
  long_name text,
  var_name text,
  calculate_distributions integer
) DISTRIBUTED BY (var_id);
ALTER TABLE work.descriptive_variables_list OWNER TO xsl;

INSERT into work.descriptive_variables_list (long_name, var_name, calculate_distributions)
VALUES 
  ( 'Community ID', 'community_id', 0),
  ( 'Connectedness of connections', 'connectedness_of_connections', 1),
  ( 'Connections', 'connections', 1),
  ( 'Connections of on-net connections', 'connections_of_onnet_connections', 1),
  ( 'Duration of calls made', 'voice_made_duration', 1),
  ( 'Duration of calls received', 'voice_rec_duration', 1),
  ( 'Voice calls made (nmb) - daytime', 'voice_made_nmb_day', 1),
  ( 'Voice calls made (nmb) - evening', 'voice_made_nmb_eve', 1),
  ( 'Voice calls made (nmb) - nighttime', 'voice_made_nmb_night', 1),
  ( 'Voice calls made (nmb) - weekday', 'voice_made_nmb_weekday', 1),
  ( 'Voice calls made (nmb) - weekend', 'voice_made_nmb_weekend', 1),
  ( 'Voice calls made (vol) - daytime', 'voice_made_duration_day', 1),
  ( 'Voice calls made (vol) - evening', 'voice_made_duration_eve', 1),
  ( 'Voice calls made (vol) - nighttime', 'voice_made_duration_night', 1),
  ( 'Voice calls made (vol) - weekday', 'voice_made_duration_weekday', 1),
  ( 'Voice calls made (vol) - weekend', 'voice_made_duration_weekend', 1),
  ( 'New subscribers within connections', 'new_subs_within_connections', 1),
  ( 'Number of calls made', 'voice_made_nmb', 1),
  ( 'Number of calls received', 'voice_rec_nmb', 1),
  ( 'Number of SMSes sent', 'sms_made_nmb', 1),
  ( 'Number of SMSes received', 'sms_rec_nmb', 1),
  ( 'Off-net connections', 'offnet_connections', 1),
  ( 'Off-net connections of on-net connections', 'offnet_connections_of_onnet_connections', 1),
  ( 'SMSes sent - daytime', 'sms_made_day', 1),
  ( 'SMSes sent - evening', 'sms_made_eve', 1),
  ( 'SMSes sent - nighttime', 'sms_made_night', 1),
  ( 'SMSes sent - weekday', 'sms_made_weekday', 1),
  ( 'SMSes sent - weekend', 'sms_made_weekend', 1),
  ( 'Share of off-net connections', 'share_of_offnet_connections', 1),
  ( 'Share of voice activity', 'share_of_voice', 1),
  ( 'Social connectivity score', 'social_connectivity_score', 1),
  ( 'Social revenue', 'social_revenue', 1),
  ( 'Social role', 'social_role', 0),
  ( 'Subscriber with no connections', 'no_connections', 1),
  ( 'Total number of IDs called', 'neigh_count_made', 1),
  ( 'Total number of IDs who have called', 'neigh_count_rec', 1),
  ( 'Weekly number of IDs called', 'neigh_count_made_weekly', 1),
  ( 'Weekly number of IDs who have called', 'neigh_count_rec_weekly', 1),
  ( 'Share of on-net calls among calls made', 'share_of_onnet_made', 1),
  ( 'Share of on-net calls among calls received', 'share_of_onnet_rec', 1),
  ( 'Number of top-ups', 'topup_count', 1),
  ( 'Average top-up amount', 'topup_amount', 1),
  ( 'Typical top-up amount', 'topup_typical', 1),
  ( 'Share of typical top-up amount', 'topup_shareof_typical', 1),
  ( 'Number of top-ups in the first half of last month', 'topup_count_firsthalf_lastmonth', 1),
  ( 'Number of top-ups in the last half of last month', 'topup_count_lasthalf_lastmonth', 1),
  ( 'Weekly number of top-ups - daytime', 'topup_count_day', 1),
  ( 'Weekly number of top-ups - evening', 'topup_count_eve', 1),
  ( 'Weekly number of top-ups - nighttime', 'topup_count_night', 1),
  ( 'Weekly number of top-ups - weekday', 'topup_count_weekday', 1),
  ( 'Weekly number of top-ups - weekend', 'topup_count_weekend', 1),
  ( 'Number of days from last top-up', 'topup_days_from_last', 1),
  ( 'Weekly data usage (kB) - daytime', 'data_usage_day', 1),
  ( 'Weekly data usage (kB) - evening', 'data_usage_eve', 1),
  ( 'Weekly data usage (kB) - nighttime', 'data_usage_night', 1),
  ( 'Weekly data usage (kB) - weekday', 'data_usage_weekday', 1),  
  ( 'Weekly data usage (kB) - weekend', 'data_usage_weekend', 1),
  ( 'Weekly data usage (kB) used', 'weekly_data_usage', 1),
  ( 'Weekly data usage cost - week', 'weekly_data_usage_cost', 1);
  --( 'Weekly cost - week', 'weekly_cost', 1),
  --( 'Share of data usage activity', 'share_of_data_usage', 1);


-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: work.descriptive_variables_subscriber
CREATE TABLE work.descriptive_variables_subscriber (
  mod_job_id integer,
  alias_id integer,
  var_id integer,
  "value" text
) DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.descriptive_variables_subscriber OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.descriptive_variables_subscriber', 'work.module_sequence', 5, 10);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.out_network_raw
CREATE TABLE tmp.out_network_raw (
  alias_a integer NOT NULL,
  alias_b integer NOT NULL,
  job_id integer NOT NULL,
  weight double precision
) DISTRIBUTED BY (alias_a)
PARTITION BY RANGE (job_id) (PARTITION "0" START (0) END (1)); 
ALTER TABLE tmp.out_network_raw OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.out_network_raw', 'work.network_sequence', NULL, 10);


-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: results.descriptive_variables_community_matrix
CREATE TABLE results.descriptive_variables_community_matrix (
  mod_job_id integer,
  community_id integer,
  "Communication density" real,
  "Intracommunication ratio" real,
  "Linkedness within community" real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (community_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.descriptive_variables_community_matrix OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, cleanup) 
VALUES ('results.descriptive_variables_community_matrix', 'work.module_sequence', 5, FALSE);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_base_1a
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_base_1a (
  mod_job_id integer,
  alias_id integer,
  voice_made_duration real,
  voice_rec_duration real,
  voice_made_nmb_day real,
  voice_made_nmb_eve real,
  voice_made_nmb_night real,
  voice_made_nmb_weekday real,
  voice_made_nmb_weekend real,
  voice_made_duration_day real,
  voice_made_duration_eve real,
  voice_made_duration_night real,
  voice_made_duration_weekday real,
  voice_made_duration_weekend real,
  voice_made_nmb real,
  voice_rec_nmb real,
  sms_rec_nmb real,
  sms_made_nmb real,
  sms_made_day real,
  sms_made_eve real,
  sms_made_night real,
  sms_made_weekday real,
  sms_made_weekend real,
  share_of_voice real,
  neigh_count_made real,
  neigh_count_rec real,
  neigh_count_made_weekly real,
  neigh_count_rec_weekly real,
  data_usage_day real,
  data_usage_weekday real,
  data_usage_eve real,
  data_usage_night real,
  data_usage_weekend real,
  weekly_data_usage real,
  weekly_data_usage_cost real
  --weekly_cost real,
  --share_of_data_usage real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_base_1a OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_base_1a', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_base_1b
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_base_1b (
  mod_job_id integer,
  alias_id integer,
  new_subs_within_connections real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_base_1b OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_base_1b', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_base_1c
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_base_1c (
  mod_job_id integer,
  alias_id integer,
  social_role text
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_base_1c OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_base_1c', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_base_2a
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_base_2a (
  mod_job_id integer,
  alias_id integer,
  community_id integer,
  connectedness_of_connections real,
  connections real,
  social_connectivity_score real,
  offnet_connections_of_onnet_connections real,
  connections_of_onnet_connections real,
  offnet_connections real,
  share_of_offnet_connections real,
  social_revenue real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_base_2a OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_base_2a', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_base_2b
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_base_2b (
  mod_job_id integer,
  alias_id integer,
  no_connections real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_base_2b OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_base_2b', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_base_2c
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_base_2c (
  mod_job_id integer,
  alias_id integer,
  share_of_onnet_made real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_base_2c OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_base_2c', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_base_2d
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_base_2d (
  mod_job_id integer,
  alias_id integer,
  share_of_onnet_rec real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_base_2d OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_base_2d', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: tmp.descriptive_variables_subscriber_matrix_topup
CREATE TABLE tmp.descriptive_variables_subscriber_matrix_topup (
  mod_job_id integer,
  alias_id integer,
  topup_count real,
  topup_amount real,
  topup_typical real,
  topup_shareof_typical real,
  topup_count_firsthalf_lastmonth real,
  topup_count_lasthalf_lastmonth real,
  topup_count_day real,
  topup_count_eve real,
  topup_count_night real,
  topup_count_weekday real,
  topup_count_weekend real,
  topup_days_from_last real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE tmp.descriptive_variables_subscriber_matrix_topup OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('tmp.descriptive_variables_subscriber_matrix_topup', 'work.module_sequence', 5, 0);

-- Used by functions defined in the script descriptivevariables_db_schema_functions.sql
-- Table: results.descriptive_variables_subscriber_matrix
CREATE TABLE results.descriptive_variables_subscriber_matrix (
  mod_job_id integer,
  alias_id integer,
  community_id integer,
  connectedness_of_connections real,
  connections real,
  connections_of_onnet_connections real,
  voice_made_duration real,
  voice_rec_duration real,
  voice_made_nmb_day real,
  voice_made_nmb_eve real,
  voice_made_nmb_night real,
  voice_made_nmb_weekday real,
  voice_made_nmb_weekend real,
  voice_made_duration_day real,
  voice_made_duration_eve real,
  voice_made_duration_night real,
  voice_made_duration_weekday real,
  voice_made_duration_weekend real,
  new_subs_within_connections real,
  voice_made_nmb real,
  voice_rec_nmb real,
  sms_rec_nmb real,
  sms_made_nmb real,
  offnet_connections real,
  offnet_connections_of_onnet_connections real,
  sms_made_day real,
  sms_made_eve real,
  sms_made_night real,
  sms_made_weekday real,
  sms_made_weekend real,
  share_of_offnet_connections real,
  share_of_voice real,
  social_connectivity_score real,
  social_revenue real,
  social_role text,
  no_connections real,
  neigh_count_made real,
  neigh_count_rec real,
  neigh_count_made_weekly real,
  neigh_count_rec_weekly real,
  share_of_onnet_made real,
  share_of_onnet_rec real,
  topup_count real,
  topup_amount real,
  topup_typical real,
  topup_shareof_typical real,
  topup_count_firsthalf_lastmonth real,
  topup_count_lasthalf_lastmonth real,
  topup_count_day real,
  topup_count_eve real,
  topup_count_night real,
  topup_count_weekday real,
  topup_count_weekend real,
  topup_days_from_last real,
  data_usage_day real,
  data_usage_weekday real,
  data_usage_eve real,
  data_usage_night real,
  data_usage_weekend real,
  weekly_data_usage real,
  weekly_data_usage_cost real
  --weekly_cost real,
  --share_of_data_usage real
) WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=5) 
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.descriptive_variables_subscriber_matrix OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, cleanup) 
VALUES ('results.descriptive_variables_subscriber_matrix', 'work.module_sequence', 5, FALSE);

-- Includes from link to link influences
CREATE TABLE work.module_influence_from_a_to_b
(
  mod_job_id integer NOT NULL,
  model_id integer NOT NULL,
  alias_a integer NOT NULL,
  alias_b integer NOT NULL,
  influence double precision
)WITH (OIDS=FALSE, APPENDONLY = true, COMPRESSLEVEL=7) 
DISTRIBUTED BY (alias_a)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.module_influence_from_a_to_b OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.module_influence_from_a_to_b', 'work.module_sequence', 7);

----- LDA schema --------------

CREATE sequence work.lda_input_sequence
INCREMENT 1
MINVALUE 1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER TABLE work.lda_input_sequence OWNER TO xsl;

CREATE sequence work.lda_output_sequence
INCREMENT 1
MINVALUE 1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER TABLE work.lda_output_sequence OWNER TO xsl;

CREATE TABLE work.lda_input_parameters
(
  lda_input_id integer NOT NULL,
  key text NOT NULL,
  value text,
  PRIMARY KEY (lda_input_id, key)
) distributed by (lda_input_id); 
ALTER TABLE work.lda_input_parameters OWNER TO xsl;

CREATE TABLE work.lda_output_parameters
(
  lda_output_id integer NOT NULL,
  key text NOT NULL,
  value text,
  PRIMARY KEY (lda_output_id, key)
) distributed by (lda_output_id); 
ALTER TABLE work.lda_output_parameters OWNER TO xsl;

CREATE TABLE work.lda_input  (
  lda_id integer,
  doc text, 
  term text, -- term can be handset_model, tariffplan etc multicategory variable
  n integer
) WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY(lda_id)
PARTITION BY RANGE (lda_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.lda_input OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.lda_input', 'work.lda_input_sequence', 5);

 -- The table for storing the topic-term probabilities
CREATE TABLE work.lda_output (
  lda_id integer, 
  term text, -- for example, handset model names
  topic text, -- for example, handset_topic1
  value double precision -- value of, for example, handset_topic1
) WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY(lda_id)
PARTITION BY RANGE (lda_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.lda_output OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.lda_output', 'work.lda_output_sequence', 5);

 -- The table for storing the document-topic probabilities
CREATE TABLE work.lda_output_doc_topic (
  lda_id integer, 
  doc text, -- for example, cell ids
  topic text, -- for example, handset_topic1
  value double precision -- value of, for example, handset_topic1
) WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY(lda_id)
PARTITION BY RANGE (lda_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.lda_output_doc_topic OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.lda_output_doc_topic', 'work.lda_output_sequence', 5);
 
-- The table containing the LDA model (unnormalized topic-term distribution)
CREATE TABLE work.lda_models_topic_term (
  lda_id integer, 
  term text, -- for example, handset model names
  topic_name text, -- for example, handset_topic1
  topic_value double precision 
) DISTRIBUTED BY(lda_id)
PARTITION BY RANGE (lda_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.lda_models_topic_term OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.lda_models_topic_term', 'work.lda_output_sequence', 5);

-- The table containing the LDA model (unnormalized doc-topic distribution)
CREATE TABLE work.lda_models_doc_topic (
  lda_id integer, 
  doc text, -- for example, cell_id names
  topic_name text, -- for example, handset_topic1
  topic_value double precision 
) DISTRIBUTED BY(lda_id)
PARTITION BY RANGE (lda_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.lda_models_doc_topic OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.lda_models_doc_topic', 'work.lda_output_sequence', 5);

-- The table for vectors showing the change in document-topic / topic-term distributions between consecutive iterations. 
CREATE TABLE work.lda_distribution_change (
  lda_id integer,
  distribution_name text, 
  iter integer, 
  delta double precision
) WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY(lda_id)
PARTITION BY RANGE (lda_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.lda_distribution_change OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.lda_distribution_change', 'work.lda_output_sequence', 5);

CREATE TABLE tmp.cdr_simpson_index (
  alias_id                      integer,
  monday                        date,
  alltime_long_diversity        double precision,
  leisure_long_diversity        double precision,
  business_long_diversity       double precision,
  alltime_short_diversity       double precision,
  leisure_short_diversity       double precision,
  business_short_diversity      double precision)
WITH (APPENDONLY=TRUE, COMPRESSLEVEL=7)
DISTRIBUTED BY (alias_id);
ALTER TABLE tmp.cdr_simpson_index OWNER TO xsl;
  
CREATE TABLE work.modelling_data_matrix_geolocation (
  mod_job_id                                                         integer NOT NULL  ,
  alias_id                                                           integer NOT NULL  ,
  mobility                                                           double precision  ,
  alltime_long_diversity                                             double precision  ,
  leisure_long_diversity                                             double precision  ,
  business_long_diversity                                            double precision  ,
  alltime_short_diversity                                            double precision  ,
  leisure_short_diversity                                            double precision  ,
  business_short_diversity                                           double precision  
) WITH (appendonly=true, compresslevel=5)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.modelling_data_matrix_geolocation OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel, retention_dates) 
VALUES ('work.modelling_data_matrix_geolocation', 'work.module_sequence', 5, 0);

--automatic preprocessing tables

--DROP TABLE tmp.aid_new_old;

-- work.tmp_model_sequence is used for getting the next template model id
CREATE sequence work.tmp_model_sequence
INCREMENT 1
MINVALUE 1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER TABLE work.tmp_model_sequence OWNER TO xsl;

-- A table for storing template models
CREATE TABLE work.module_template_models
(
  model_id integer NOT NULL,
  aid integer DEFAULT 0,
  bid integer DEFAULT 0,
  output_id integer DEFAULT 0,
  "key" text NOT NULL,
  "value" text
)distributed by (model_id) ;
ALTER TABLE work.module_template_models OWNER TO xsl;


-- Quantiles corresponding to knot positions of splines with a certain number (k) of knots. 
CREATE TABLE work.rcs_quantiles
(
  k integer, -- number of knots
  i integer, -- knot index
  p_quantile double precision -- quantile
) distributed by (k) ;
ALTER TABLE work.rcs_quantiles OWNER TO xsl;

INSERT INTO work.rcs_quantiles
  (k, i, p_quantile)
VALUES
  (3, 1, 25),
  (3, 2, 50),
  (3, 3, 75),
  (4, 1, 20),
  (4, 2, 40),
  (4, 3, 60),
  (4, 4, 80),
  (5, 1, 16.66667),
  (5, 2, 33.33333),
  (5, 3, 50.00000),
  (5, 4, 66.66667),
  (5, 5, 83.33333);

-- For storing quantile values of different distributions: 
CREATE TABLE work.distribution_quantiles
(
  distr_name text,
  quantile double precision,
  value double precision
) distributed by (distr_name) ;
ALTER TABLE work.distribution_quantiles OWNER TO xsl;



INSERT INTO work.distribution_quantiles 
  (distr_name, quantile, value) 
VALUES
  ('normal', 1, -1.00000000),
  ('normal', 2, -0.88282107), 
  ('normal', 3, -0.80847479), 
  ('normal', 4, -0.75254698), 
  ('normal', 5, -0.70705402), 
  ('normal', 6, -0.66833237), 
  ('normal', 7, -0.63438106), 
  ('normal', 8, -0.60398171), 
  ('normal', 9, -0.57633471), 
  ('normal', 10, -0.55088561), 
  ('normal', 11, -0.52723332), 
  ('normal', 12, -0.50507785), 
  ('normal', 13, -0.48418860), 
  ('normal', 14, -0.46438426), 
  ('normal', 15, -0.44551952), 
  ('normal', 16, -0.42747600), 
  ('normal', 17, -0.41015588), 
  ('normal', 18, -0.39347730), 
  ('normal', 19, -0.37737103), 
  ('normal', 20, -0.36177789), 
  ('normal', 21, -0.34664689), 
  ('normal', 22, -0.33193368), 
  ('normal', 23, -0.31759947), 
  ('normal', 24, -0.30361004), 
  ('normal', 25, -0.28993503), 
  ('normal', 26, -0.27654738), 
  ('normal', 27, -0.26342277), 
  ('normal', 28, -0.25053927), 
  ('normal', 29, -0.23787703), 
  ('normal', 30, -0.22541793), 
  ('normal', 31, -0.21314540), 
  ('normal', 32, -0.20104422), 
  ('normal', 33, -0.18910034), 
  ('normal', 34, -0.17730071), 
  ('normal', 35, -0.16563321), 
  ('normal', 36, -0.15408650), 
  ('normal', 37, -0.14264992), 
  ('normal', 38, -0.13131346), 
  ('normal', 39, -0.12006761), 
  ('normal', 40, -0.10890336), 
  ('normal', 41, -0.09781210), 
  ('normal', 42, -0.08678559), 
  ('normal', 43, -0.07581590), 
  ('normal', 44, -0.06489537), 
  ('normal', 45, -0.05401658), 
  ('normal', 46, -0.04317227), 
  ('normal', 47, -0.03235538), 
  ('normal', 48, -0.02155894), 
  ('normal', 49, -0.01077608), 
  ('normal', 50, 0.00000000), 
  ('normal', 51, 0.01077608), 
  ('normal', 52, 0.02155894), 
  ('normal', 53, 0.03235538), 
  ('normal', 54, 0.04317227), 
  ('normal', 55, 0.05401658), 
  ('normal', 56, 0.06489537), 
  ('normal', 57, 0.07581590), 
  ('normal', 58, 0.08678559), 
  ('normal', 59, 0.09781210), 
  ('normal', 60, 0.10890336), 
  ('normal', 61, 0.12006761), 
  ('normal', 62, 0.13131346), 
  ('normal', 63, 0.14264992), 
  ('normal', 64, 0.15408650), 
  ('normal', 65, 0.16563321), 
  ('normal', 66, 0.17730071), 
  ('normal', 67, 0.18910034), 
  ('normal', 68, 0.20104422), 
  ('normal', 69, 0.21314540), 
  ('normal', 70, 0.22541793), 
  ('normal', 71, 0.23787703), 
  ('normal', 72, 0.25053927), 
  ('normal', 73, 0.26342277), 
  ('normal', 74, 0.27654738), 
  ('normal', 75, 0.28993503), 
  ('normal', 76, 0.30361004), 
  ('normal', 77, 0.31759947), 
  ('normal', 78, 0.33193368), 
  ('normal', 79, 0.34664689), 
  ('normal', 80, 0.36177789), 
  ('normal', 81, 0.37737103), 
  ('normal', 82, 0.39347730), 
  ('normal', 83, 0.41015588), 
  ('normal', 84, 0.42747600), 
  ('normal', 85, 0.44551952), 
  ('normal', 86, 0.46438426), 
  ('normal', 87, 0.48418860), 
  ('normal', 88, 0.50507785), 
  ('normal', 89, 0.52723332), 
  ('normal', 90, 0.55088561), 
  ('normal', 91, 0.57633471), 
  ('normal', 92, 0.60398171), 
  ('normal', 93, 0.63438106), 
  ('normal', 94, 0.66833237), 
  ('normal', 95, 0.70705402), 
  ('normal', 96, 0.75254698), 
  ('normal', 97, 0.80847479), 
  ('normal', 98, 0.88282107),  
  ('normal', 99, 1.00000000);


-- Charts

-- data for the charts is calculated and stored in:
CREATE TABLE charts.chart_data (
	mod_job_id integer,
	stat_name text,			-- short name for the calculated statistics
	group_name text,		-- short name for group for which calcutation is done
	data_date date,			-- chart data date 
	var_name text,			-- field for variable name 
	var_value double precision,	-- field for variable value
	order_id integer) 		-- field for order values are shown in chart
DISTRIBUTED RANDOMLY;
ALTER TABLE charts.chart_data OWNER TO xsl;

-- chart colors
CREATE TABLE charts.chart_color
(
  rank_id integer,
  rank_color text
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (rank_id);
ALTER TABLE charts.chart_color OWNER TO xsl;

INSERT INTO charts.chart_color values 
(1, '00A4E4'), 
(2, '0069AA'), 
(3, '0069AA'), 
(4, '0069AA'), 
(5, '0069AA'), 
(6, '0069AA');


-- data for the targeting charts is calculated and stored in:
CREATE TABLE charts.chart_targeting_data (
  target_id integer,          -- ID of the target list
  group_id integer,           -- ID of the group, running number 1, 2, 3,...
  group_name text,            -- Name of the group, e.g., 'Product X', 'High value segment', etc.
  var_name text,              -- x-axis value
  var_value double precision, -- y-axis value
  order_id integer)           -- field for the order in which the values are shown in the chart
DISTRIBUTED RANDOMLY;
ALTER TABLE charts.chart_targeting_data OWNER TO xsl;

-- This table contains the descriptive variables that are plotted in the differentiating factors charts
CREATE TABLE work.differentiating_factors (
  var_id serial,
  long_name text,
  var_name text,
  stat_name text,
  order_id integer
) DISTRIBUTED BY (var_id);
ALTER TABLE work.differentiating_factors OWNER TO xsl;

INSERT into work.differentiating_factors (long_name, var_name, stat_name, order_id)
VALUES 
  ( 'Total number of IDs called', 'neigh_count_made', 'DESC_VAR_SNA',1),
  ( 'Total number of IDs who have called', 'neigh_count_rec', 'DESC_VAR_SNA',1),
  ( 'Weekly number of IDs called', 'neigh_count_made_weekly', 'DESC_VAR_SNA',1),
  ( 'Weekly number of IDs who have called', 'neigh_count_rec_weekly', 'DESC_VAR_SNA',1),
  ( 'Share of off-net connections', 'share_of_offnet_connections', 'DESC_VAR_SNA',1),
  ( 'Connectedness of connections', 'connectedness_of_connections', 'DESC_VAR_SNA',1),
  ( 'Connections', 'connections', 'DESC_VAR_SNA',1),
  ( 'Connections of on-net connections', 'connections_of_onnet_connections', 'DESC_VAR_SNA',1),
  ( 'Social revenue', 'social_revenue', 'DESC_VAR_SNA',1),
  ( 'Share of on-net calls among calls received', 'share_of_onnet_rec', 'DESC_VAR_SNA',1),
  ( 'Share of on-net calls among calls made', 'share_of_onnet_made', 'DESC_VAR_SNA',1),
  ( 'New subscribers within connections', 'new_subs_within_connections', 'DESC_VAR_SNA',1),
  ( 'Off-net connections', 'offnet_connections', 'DESC_VAR_SNA',1),
  ( 'Off-net connections of on-net connections', 'offnet_connections_of_onnet_connections', 'DESC_VAR_SNA',1),
  ( 'Social connectivity score', 'social_connectivity_score', 'DESC_VAR_SNA',1),

  ( 'Number of SMSes sent', 'sms_made_nmb', 'DESC_VAR_SU1',2),
  ( 'Duration of calls received', 'voice_rec_duration', 'DESC_VAR_SU1',2),
  ( 'Duration of calls made', 'voice_made_duration', 'DESC_VAR_SU1',2),
  ( 'Share of voice activity', 'share_of_voice', 'DESC_VAR_SU1',2),
  ( 'Number of calls made', 'voice_made_nmb', 'DESC_VAR_SU1',2),
  ( 'Number of calls received', 'voice_rec_nmb', 'DESC_VAR_SU1',2),
  ( 'Number of SMSes received', 'sms_rec_nmb', 'DESC_VAR_SU1',2),
  ( 'Weekly data usage(kB) used', 'weekly_data_usage','DESC_VAR_SU1',2),
  
  ( 'Voice calls made (nmb) - daytime', 'voice_made_nmb_day', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (nmb) - evening', 'voice_made_nmb_eve', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (nmb) - nighttime', 'voice_made_nmb_night', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (nmb) - weekday', 'voice_made_nmb_weekday', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (nmb) - weekend', 'voice_made_nmb_weekend', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (vol) - daytime', 'voice_made_duration_day', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (vol) - evening', 'voice_made_duration_eve', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (vol) - nighttime', 'voice_made_duration_night', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (vol) - weekday', 'voice_made_duration_weekday', 'DESC_VAR_SU2', 3),
  ( 'Voice calls made (vol) - weekend', 'voice_made_duration_weekend', 'DESC_VAR_SU2', 3),

  ( 'SMSes sent - daytime', 'sms_made_day', 'DESC_VAR_SU3', 4),
  ( 'SMSes sent - evening', 'sms_made_eve', 'DESC_VAR_SU3', 4),
  ( 'SMSes sent - nighttime', 'sms_made_night', 'DESC_VAR_SU3', 4),
  ( 'SMSes sent - weekday', 'sms_made_weekday', 'DESC_VAR_SU3', 4),
  ( 'SMSes sent - weekend', 'sms_made_weekend', 'DESC_VAR_SU3', 4),

  ( 'Number of top-ups', 'topup_count', 'DESC_VAR_TOPUP', 5),
  ( 'Average top-up amount', 'topup_amount', 'DESC_VAR_TOPUP', 5),
  ( 'Typical top-up amount', 'topup_typical', 'DESC_VAR_TOPUP', 5),
  ( 'Share of typical top-up amount', 'topup_shareof_typical', 'DESC_VAR_TOPUP', 5),
  ( 'Number of top-ups in the first half of last month', 'topup_count_firsthalf_lastmonth', 'DESC_VAR_TOPUP', 5),
  ( 'Number of top-ups in the last half of last month', 'topup_count_lasthalf_lastmonth', 'DESC_VAR_TOPUP', 5),
  ( 'Weekly number of top-ups - daytime', 'topup_count_day', 'DESC_VAR_TOPUP', 5),
  ( 'Weekly number of top-ups - evening', 'topup_count_eve', 'DESC_VAR_TOPUP', 5),
  ( 'Weekly number of top-ups - nighttime', 'topup_count_night', 'DESC_VAR_TOPUP', 5),
  ( 'Weekly number of top-ups - weekday', 'topup_count_weekday', 'DESC_VAR_TOPUP', 5),
  ( 'Weekly number of top-ups - weekend', 'topup_count_weekend', 'DESC_VAR_TOPUP', 5),
  ( 'Number of days from last top-up', 'topup_days_from_last', 'DESC_VAR_TOPUP', 5),
  
  ( 'Weekly data usage (kB) - daytime', 'data_usage_day', 'DESC_VAR_DU',6),
  ( 'Weekly data usage (kB) - evening', 'data_usage_eve', 'DESC_VAR_DU',6),
  ( 'Weekly data usage (kB) - night', 'data_usage_night', 'DESC_VAR_DU',6),
  ( 'Weekly data usage (kB) - weekday', 'data_usage_weekday','DESC_VAR_DU',6),  
  ( 'Weekly data usage (kB) - weekend', 'data_usage_weekend','DESC_VAR_DU',6),
  --( 'Weekly data usage (kB) - week', 'weekly_data_usage','DESC_VAR_DU',6),
  ( 'Weekly data usage cost - week', 'weekly_data_usage_cost','DESC_VAR_DU',6);
  --( 'Weekly cost for - week', 'weekly_cost','DESC_VAR_DU',6),
  --( 'Share of data usage activity', 'share_of_data_usage','DESC_VAR_DU',6);


------------ Target list output tables ------------

-- Table for saving the targeting outputs for the churn_inactivity usecase
CREATE TABLE results.module_export_churn_inactivity (
  mod_job_id integer NOT NULL,
  target_id integer NOT NULL,
  msisdn text NOT NULL,
  churn_inactivity_propensity_score double precision,
  churn_inactivity_expected_revenue_loss double precision,
  target text,
  group_id integer,
  delivery_date date,
  order_id integer
) 
WITH (appendonly=TRUE, compresslevel=5)
DISTRIBUTED BY (target_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.module_export_churn_inactivity OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('results.module_export_churn_inactivity', 'work.module_sequence', 5);


-- Table for saving the targeting outputs for the churn_postpaid usecase
CREATE TABLE results.module_export_churn_postpaid (
  mod_job_id integer NOT NULL,
  target_id integer NOT NULL,
  msisdn text NOT NULL,
  churn_postpaid_propensity_score double precision,
  target text,
  group_id integer,
  delivery_date date,
  order_id integer,
  churn_postpaid_expected_revenue_loss double precision
) 
WITH (appendonly=TRUE, compresslevel=5)
DISTRIBUTED BY (target_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.module_export_churn_postpaid OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('results.module_export_churn_postpaid', 'work.module_sequence', 5);


-- Table for saving the targeting outputs for the best next product usecase
CREATE TABLE results.module_export_product (
  mod_job_id integer NOT NULL,
  target_id integer NOT NULL,
  msisdn text NOT NULL,
  highest_propensity double precision,
  highest_propensity_product text,
  product_x_propensity double precision, 
  product_y_propensity double precision,
  target text,
  group_id integer,
  delivery_date date,
  order_id integer
) 
WITH (appendonly=TRUE, compresslevel=5)
DISTRIBUTED BY (target_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.module_export_product OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('results.module_export_product', 'work.module_sequence', 5);


-- Table for saving the targeting outputs for the social network insight usecase
CREATE TABLE results.module_export_sni (
  mod_job_id integer NOT NULL,
  target_id integer NOT NULL,
  msisdn text NOT NULL,
  connectedness_of_connections real,
  connections real,
  connections_of_onnet_connections real,
  voice_made_duration real,
  voice_rec_duration real,
  voice_made_nmb_day real,
  voice_made_nmb_eve real,
  voice_made_nmb_night real,
  voice_made_nmb_weekday real,
  voice_made_nmb_weekend real,
  voice_made_duration_day real,
  voice_made_duration_eve real,
  voice_made_duration_night real,
  voice_made_duration_weekday real,
  voice_made_duration_weekend real,
  new_subs_within_connections real,
  voice_made_nmb real,
  voice_rec_nmb real,
  sms_rec_nmb real,
  sms_made_nmb real,
  offnet_connections real,
  offnet_connections_of_onnet_connections real,
  sms_made_day real,
  sms_made_eve real,
  sms_made_night real,
  sms_made_weekday real,
  sms_made_weekend real,
  share_of_offnet_connections real,
  share_of_voice real,
  social_connectivity_score real,
  social_revenue real,
  social_role text,
  no_connections real,
  neigh_count_made real,
  neigh_count_rec real,
  neigh_count_made_weekly real,
  neigh_count_rec_weekly real,
  share_of_onnet_made real,
  share_of_onnet_rec real,
  topup_count real,
  topup_amount real,
  topup_typical real,
  topup_shareof_typical real,
  topup_count_firsthalf_lastmonth real,
  topup_count_lasthalf_lastmonth real,
  topup_count_day real,
  topup_count_eve real,
  topup_count_night real,
  topup_count_weekday real,
  topup_count_weekend real,
  topup_days_from_last real,
  data_usage_day real,
  data_usage_weekday real,
  data_usage_eve real,
  data_usage_night real,
  data_usage_weekend real,
  weekly_data_usage real,
  weekly_data_usage_cost real,
  --weekly_cost real,
  --share_of_data_usage real, 
  target text,
  group_id integer,
  delivery_date date,
  order_id integer
) 
WITH (appendonly=TRUE, compresslevel=5)
DISTRIBUTED BY (target_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.module_export_sni OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('results.module_export_sni', 'work.module_sequence', 5);

-- Table for saving the targeting outputs for the zero_day_prediction usecase
CREATE TABLE results.module_export_zero_day_prediction (
  mod_job_id integer NOT NULL,
  target_id integer NOT NULL,
  msisdn text NOT NULL,
  tenure_group integer, --Currently not used in targeting output
  highest_propensity double precision,
  highest_propensity_segment text,
  low_value_segment_propensity double precision,
  medium_value_segment_propensity double precision,
  high_value_segment_propensity double precision,
  target text,
  group_id integer,
  delivery_date date,
  order_id integer
) 
WITH (appendonly=TRUE, compresslevel=5)
DISTRIBUTED BY (target_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE results.module_export_zero_day_prediction OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('results.module_export_zero_day_prediction', 'work.module_sequence', 5);


-- Table for saving the IDs of approved target lists
CREATE TABLE results.approved_target_lists (
  target_id integer,
  time_approved timestamp without time zone
) 
DISTRIBUTED BY (target_id);
ALTER TABLE results.approved_target_lists OWNER TO xsl;


CREATE TABLE data.product_information (
  product_id text,
  subproduct_id text, 
  product_name text,
  product_cost double precision,
  PRIMARY KEY (product_id, subproduct_id)
);
ALTER TABLE data.product_information OWNER TO xsl;

-- Table: work.monthly_arpu

-- DROP TABLE work.monthly_arpu;

CREATE TABLE work.monthly_arpu
(
  mod_job_id integer NOT NULL,
  alias_id integer NOT NULL,
  monthly_arpu double precision,
  segmentation text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, 
  OIDS=FALSE
)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE(mod_job_id) 
          (
          PARTITION "0" START (0) END (1) WITH (appendonly=true, compresslevel=5)
          )
;
ALTER TABLE work.monthly_arpu
  OWNER TO xsl;
INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.monthly_arpu', 'work.module_sequence', 5);

CREATE TABLE work.tenure_days (
  mod_job_id integer NOT NULL,
  alias_id integer NOT NULL,
  tenure_days integer,
  segmentation text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, 
  OIDS=FALSE
)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE(mod_job_id) 
          (
          PARTITION "0" START (0) END (1) WITH (appendonly=true, compresslevel=5)
          )
;
ALTER TABLE work.tenure_days
  OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.tenure_days', 'work.module_sequence', 5);  
  
CREATE TABLE work.target_control_groups
(
  mod_job_id integer NOT NULL,
  target_id integer NOT NULL,
  alias_id integer NOT NULL,
  target text,
  use_case text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, 
  OIDS=FALSE
)
DISTRIBUTED BY (target_id)
PARTITION BY RANGE (mod_job_id) (PARTITION "0" START (0) END (1));
ALTER TABLE work.target_control_groups
  OWNER TO xsl;

INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) 
VALUES ('work.target_control_groups', 'work.module_sequence', 5);


-- Table for saving the IDs of approved score lists
CREATE TABLE work.approved_score_lists (
  workflow_id text,
  time_approved timestamp without time zone
) 
DISTRIBUTED BY (workflow_id);
ALTER TABLE work.approved_score_lists OWNER TO xsl;
