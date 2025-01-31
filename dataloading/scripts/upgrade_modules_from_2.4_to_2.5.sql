/*
This file contains the schema upgrade sql from SL 2.4 to SL 2.5.
Functions are updated automatically outside this sql by the shell
script. Here are included all the changes needed to modify the 
module tables while preserving the data. Also if functions are dropped 
from SL 2.4 or their arguments are changed, they are dropped here.

The changes are here in chronological order, earlier changes in the 
beginning of the file. Before each change, is the date and the
initials of the responsible person.
*/

\set ON_ERROR_STOP 1

CREATE OR REPLACE FUNCTION upgrade_check_to_25(schema_name text, table_name text)
  RETURNS void AS
$BODY$
  DECLARE
  table_count integer;
  BEGIN
    select count(*) into table_count
    from pg_tables where schemaname = schema_name and tablename = table_name;
    if table_count > 0 then
      RAISE EXCEPTION 'No upgrade to version 2.5 possible; this is already version 2.5.';
    end if;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION upgrade_check_to_25(text, text) OWNER TO xsl;

select upgrade_check_to_25('work', 'tenure_days');

DROP FUNCTION upgrade_check_to_25(text, text);


--Help function to add partitions for tables that are added after partitions based on sequences have been created

CREATE OR REPLACE FUNCTION core.upgrade_24_to_25_add_missing_partitions(in_table_name text)
  RETURNS VOID AS
$BODY$
/* SUMMARY
 * Adds missing partitions for new tables created based on the current sequence value that is related to table. 
 *
 * VERSION
 * 10.06.2014 HMa SL 2.5 upgrade version
 * 14.08.2013 KL
 */
DECLARE

  sequence_name_t text;
  sequence_value_max integer;
  seq_value integer;
  table_compresslevel integer;
  cleanup_t boolean;

BEGIN

  SELECT sequence_name INTO sequence_name_t
  FROM core.partition_sequence_tables
  WHERE table_name = in_table_name;

  SELECT compresslevel INTO table_compresslevel
  FROM core.partition_sequence_tables
  WHERE table_name = in_table_name;


  SELECT cleanup INTO cleanup_t
  FROM core.partition_sequence_tables
  WHERE table_name = in_table_name;


  SELECT max(sequence_id) INTO sequence_value_max
  FROM core.partition_sequence_create_times 
  WHERE sequence_name = sequence_name_t;

  IF sequence_value_max IS NOT NULL THEN
    FOR seq_value IN 1..sequence_value_max LOOP
  
      EXECUTE 'ALTER TABLE ' || in_table_name || ' '
           || 'ADD PARTITION "' || seq_value || '" '
           || 'START (' || seq_value || ') END (' || (seq_value + 1) || ') '
           || COALESCE('WITH (appendonly=true, compresslevel=' || table_compresslevel || ')', '');
  
      INSERT INTO core.partition_sequence_create_times (table_name, sequence_name, sequence_id, time_created, cleanup) 
         VALUES (in_table_name , sequence_name_t ,seq_value ,now() , cleanup_t );
    END LOOP;
  END IF;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION core.upgrade_24_to_25_add_missing_partitions(text) OWNER TO xsl;




-- 2014-06-10 HMa: Drop old modelling data functions:
DROP FUNCTION work.create_modelling_data4(in_mod_job_id integer, in_lag_length integer, in_lag_count integer);
DROP FUNCTION work.create_modelling_data5(in_mod_job_id integer, in_lag_length integer, in_lag_count integer);
DROP FUNCTION work.create_modelling_data6(in_mod_job_id integer, in_lag_length integer, in_lag_count integer);
DROP FUNCTION work.create_modelling_data_targets(in_mod_job_id integer);

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_1');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_2');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_3');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_4_made');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_4_rec');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_topup1');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_topup2');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_topup_channel');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_topup3');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_topup3a');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_handset_topic');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_zdp1');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_zdp2');

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix_zdp4');


-- 2014-06-10 HMa: Replace old work.modelling_data_matrix with new one: 
ALTER TABLE work.modelling_data_matrix RENAME TO modelling_data_matrix_sl24;
DELETE FROM core.partition_sequence_create_times WHERE table_name = 'work.modelling_data_matrix';

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

SELECT * FROM core.upgrade_24_to_25_add_missing_partitions('work.modelling_data_matrix');

INSERT INTO	work.modelling_data_matrix (
  mod_job_id,
  alias_id,
  target_churn_inactivity,
  audience_churn_inactivity,
  target_churn_postpaid,
  audience_churn_postpaid,
  target_zero_day_prediction,
  audience_zero_day_prediction,
  target_product_product_x,
  audience_product_product_x,
  target_product_product_y,
  audience_product_product_y,
  alias_count,
  callsmsratio,
  mr_count_ratio,
  mr_ratio,
  smscount,
  smscountday,
  smscountevening,
  smscountweekday,
  voicecount,
  voicecountday,
  voicecountevening,
  voicecountweekday,
  voicesum,
  voicesumday,
  voicesumevening,
  voicesumweekday,
  week_entropy,
  week_mean,
  knn,
  knn_2,
  k_offnet,
  k_target,
  age,
  alpha,
  c,
  alpha_2,
  c_2,
  churn_score,
  contr_length,
  contr_remain,
  country,
  gender,
  handset_age,
  handset_model,
  k,
  kshell,
  k_2,
  kshell_2,
  language,
  monthly_arpu,
  no_churn_score,
  payment_type,
  separate_node,
  separate_node_2,
  socrev,
  socrevest,
  socrev_2,
  socrevest_2,
  subscriber_segment,
  subscriber_value,
  subscription_type,
  tariff_plan,
  wec,
  wec_2,
  zip,
  daily_voice_activity1,
  daily_voice_activity2,
  daily_voice_activity3,
  daily_voice_activity4,
  daily_sms_activity1,
  daily_sms_activity2,
  daily_sms_activity3,
  daily_sms_activity4,
  daily_data_activity1,
  daily_data_activity2,
  daily_data_activity3,
  daily_data_activity4,
  weekly_data_usage1,
  weekly_data_usage2,
  weekly_data_usage3,
  weekly_data_usage4,
  weekly_voice_neigh_count1,
  weekly_voice_neigh_count2,
  weekly_voice_neigh_count3,
  weekly_voice_neigh_count4,
  weekly_voice_count1,
  weekly_voice_count2,
  weekly_voice_count3,
  weekly_voice_count4,
  weekly_sms_count1,
  weekly_sms_count2,
  weekly_sms_count3,
  weekly_sms_count4,
  weekly_data_count1,
  weekly_data_count2,
  weekly_data_count3,
  weekly_data_count4,
  weekly_voice_duration1,
  weekly_voice_duration2,
  weekly_voice_duration3,
  weekly_voice_duration4,
  weekly_voice_cost1,
  weekly_voice_cost2,
  weekly_voice_cost3,
  weekly_voice_cost4,
  weekly_sms_cost1,
  weekly_sms_cost2,
  weekly_sms_cost3,
  weekly_sms_cost4,
  weekly_cost1,
  weekly_cost2,
  weekly_cost3,
  weekly_cost4,
  weekly_cell_id_count1,
  weekly_cell_id_count2,
  weekly_cell_id_count3,
  weekly_cell_id_count4,
  inact,
  daily_voice_activity_rec1,
  daily_voice_activity_rec2,
  daily_voice_activity_rec3,
  daily_voice_activity_rec4,
  daily_sms_activity_rec1,
  daily_sms_activity_rec2,
  daily_sms_activity_rec3,
  daily_sms_activity_rec4,
  weekly_voice_count_rec1,
  weekly_voice_count_rec2,
  weekly_voice_count_rec3,
  weekly_voice_count_rec4,
  weekly_sms_count_rec1,
  weekly_sms_count_rec2,
  weekly_sms_count_rec3,
  weekly_sms_count_rec4,
  weekly_voice_duration_rec1,
  weekly_voice_duration_rec2,
  weekly_voice_duration_rec3,
  weekly_voice_duration_rec4,
  weekly_cell_id_count_rec1,
  weekly_cell_id_count_rec2,
  weekly_cell_id_count_rec3,
  weekly_cell_id_count_rec4,
  inact_rec,
  topup_amount_avg1,
  topup_amount_avg2,
  topup_amount_avg3,
  topup_amount_avg4,
  topup_bonus_avg1,
  topup_bonus_avg2,
  topup_bonus_avg3,
  topup_bonus_avg4,
  topup_bonus_per_amount_avg1,
  topup_bonus_per_amount_avg2,
  topup_bonus_per_amount_avg3,
  topup_bonus_per_amount_avg4,
  topup_count_weekly1,
  topup_count_weekly2,
  topup_count_weekly3,
  topup_count_weekly4,
  topup_free_avg1,
  topup_free_avg2,
  topup_free_avg3,
  topup_free_avg4,
  topup_free_count_weekly1,
  topup_free_count_weekly2,
  topup_free_count_weekly3,
  topup_free_count_weekly4,
  topup_days_from_last,
  topup_days_to_next1,
  topup_days_to_next2,
  topup_days_interval,
  topup_count_weekly_daytime1,
  topup_count_weekly_daytime2,
  topup_count_weekly_daytime3,
  topup_count_weekly_daytime4,
  topup_count_weekly_evening1,
  topup_count_weekly_evening2,
  topup_count_weekly_evening3,
  topup_count_weekly_evening4,
  topup_count_weekly_nighttime1,
  topup_count_weekly_nighttime2,
  topup_count_weekly_nighttime3,
  topup_count_weekly_nighttime4,
  topup_count_weekly_weekend1,
  topup_count_weekly_weekend2,
  topup_count_weekly_weekend3,
  topup_count_weekly_weekend4,
  topup_interval_from_last_call_avg1,
  topup_interval_from_last_call_avg2,
  topup_interval_from_last_call_avg3,
  topup_interval_from_last_call_avg4,
  topup_interval_from_last_sms_avg1,
  topup_interval_from_last_sms_avg2,
  topup_interval_from_last_sms_avg3,
  topup_interval_from_last_sms_avg4,
  topup_interval_to_next_call_avg1,
  topup_interval_to_next_call_avg2,
  topup_interval_to_next_call_avg3,
  topup_interval_to_next_call_avg4,
  topup_interval_to_next_sms_avg1,
  topup_interval_to_next_sms_avg2,
  topup_interval_to_next_sms_avg3,
  topup_interval_to_next_sms_avg4,
  handset_topic_1,
  handset_topic_2,
  handset_topic_3,
  handset_topic_4,
  handset_topic_5,
  uc_zero_day_prediction_inactive_to_active_days_ratio,
  uc_zero_day_prediction_weekend_to_weekday_voice_count_ratio,
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
  uc_zero_day_prediction_average_daily_voice_hour,
  uc_zero_day_prediction_average_daily_topup_count,
  uc_zero_day_prediction_total_topup_count_capped_at_two,
  uc_zero_day_prediction_average_topup_cost,
  uc_zero_day_prediction_tenure,
  uc_zero_day_prediction_tenure_group,
  uc_zero_day_prediction_activation_weekday
)
SELECT
  mod_job_id,
  alias_id,
  target_churn_inactivity,
  audience_churn_inactivity,
  target_churn_postpaid,
  audience_churn_postpaid,
  target_zero_day_prediction,
  audience_zero_day_prediction,
  target_product_product_x,
  audience_product_product_x,
  target_product_product_y,
  audience_product_product_y,
  alias_count,
  callsmsratio,
  mr_count_ratio,
  mr_ratio,
  smscount,
  smscountday,
  smscountevening,
  smscountweekday,
  voicecount,
  voicecountday,
  voicecountevening,
  voicecountweekday,
  voicesum,
  voicesumday,
  voicesumevening,
  voicesumweekday,
  week_entropy,
  week_mean,
  knn,
  knn_2,
  k_offnet,
  k_target,
  age,
  alpha,
  c,
  alpha_2,
  c_2,
  churn_score,
  contr_length,
  contr_remain,
  country,
  gender,
  handset_age,
  handset_model,
  k,
  kshell,
  k_2,
  kshell_2,
  language,
  monthly_arpu,
  no_churn_score,
  payment_type,
  separate_node,
  separate_node_2,
  socrev,
  socrevest,
  socrev_2,
  socrevest_2,
  subscriber_segment,
  subscriber_value,
  subscription_type,
  tariff_plan,
  wec,
  wec_2,
  zip,
  daily_voice_activity1,
  daily_voice_activity2,
  daily_voice_activity3,
  daily_voice_activity4,
  daily_sms_activity1,
  daily_sms_activity2,
  daily_sms_activity3,
  daily_sms_activity4,
  daily_data_activity1,
  daily_data_activity2,
  daily_data_activity3,
  daily_data_activity4,
  weekly_data_usage1,
  weekly_data_usage2,
  weekly_data_usage3,
  weekly_data_usage4,
  weekly_voice_neigh_count1,
  weekly_voice_neigh_count2,
  weekly_voice_neigh_count3,
  weekly_voice_neigh_count4,
  weekly_voice_count1,
  weekly_voice_count2,
  weekly_voice_count3,
  weekly_voice_count4,
  weekly_sms_count1,
  weekly_sms_count2,
  weekly_sms_count3,
  weekly_sms_count4,
  weekly_data_count1,
  weekly_data_count2,
  weekly_data_count3,
  weekly_data_count4,
  weekly_voice_duration1,
  weekly_voice_duration2,
  weekly_voice_duration3,
  weekly_voice_duration4,
  weekly_voice_cost1,
  weekly_voice_cost2,
  weekly_voice_cost3,
  weekly_voice_cost4,
  weekly_sms_cost1,
  weekly_sms_cost2,
  weekly_sms_cost3,
  weekly_sms_cost4,
  weekly_cost1,
  weekly_cost2,
  weekly_cost3,
  weekly_cost4,
  weekly_cell_id_count1,
  weekly_cell_id_count2,
  weekly_cell_id_count3,
  weekly_cell_id_count4,
  inact,
  daily_voice_activity_rec1,
  daily_voice_activity_rec2,
  daily_voice_activity_rec3,
  daily_voice_activity_rec4,
  daily_sms_activity_rec1,
  daily_sms_activity_rec2,
  daily_sms_activity_rec3,
  daily_sms_activity_rec4,
  weekly_voice_count_rec1,
  weekly_voice_count_rec2,
  weekly_voice_count_rec3,
  weekly_voice_count_rec4,
  weekly_sms_count_rec1,
  weekly_sms_count_rec2,
  weekly_sms_count_rec3,
  weekly_sms_count_rec4,
  weekly_voice_duration_rec1,
  weekly_voice_duration_rec2,
  weekly_voice_duration_rec3,
  weekly_voice_duration_rec4,
  weekly_cell_id_count_rec1,
  weekly_cell_id_count_rec2,
  weekly_cell_id_count_rec3,
  weekly_cell_id_count_rec4,
  inact_rec,
  topup_amount_avg1,
  topup_amount_avg2,
  topup_amount_avg3,
  topup_amount_avg4,
  topup_bonus_avg1,
  topup_bonus_avg2,
  topup_bonus_avg3,
  topup_bonus_avg4,
  topup_bonus_per_amount_avg1,
  topup_bonus_per_amount_avg2,
  topup_bonus_per_amount_avg3,
  topup_bonus_per_amount_avg4,
  topup_count_weekly1,
  topup_count_weekly2,
  topup_count_weekly3,
  topup_count_weekly4,
  topup_free_avg1,
  topup_free_avg2,
  topup_free_avg3,
  topup_free_avg4,
  topup_free_count_weekly1,
  topup_free_count_weekly2,
  topup_free_count_weekly3,
  topup_free_count_weekly4,
  topup_days_from_last,
  topup_days_to_next1,
  topup_days_to_next2,
  topup_days_interval,
  topup_count_weekly_daytime1,
  topup_count_weekly_daytime2,
  topup_count_weekly_daytime3,
  topup_count_weekly_daytime4,
  topup_count_weekly_evening1,
  topup_count_weekly_evening2,
  topup_count_weekly_evening3,
  topup_count_weekly_evening4,
  topup_count_weekly_nighttime1,
  topup_count_weekly_nighttime2,
  topup_count_weekly_nighttime3,
  topup_count_weekly_nighttime4,
  topup_count_weekly_weekend1,
  topup_count_weekly_weekend2,
  topup_count_weekly_weekend3,
  topup_count_weekly_weekend4,
  topup_interval_from_last_call_avg1,
  topup_interval_from_last_call_avg2,
  topup_interval_from_last_call_avg3,
  topup_interval_from_last_call_avg4,
  topup_interval_from_last_sms_avg1,
  topup_interval_from_last_sms_avg2,
  topup_interval_from_last_sms_avg3,
  topup_interval_from_last_sms_avg4,
  topup_interval_to_next_call_avg1,
  topup_interval_to_next_call_avg2,
  topup_interval_to_next_call_avg3,
  topup_interval_to_next_call_avg4,
  topup_interval_to_next_sms_avg1,
  topup_interval_to_next_sms_avg2,
  topup_interval_to_next_sms_avg3,
  topup_interval_to_next_sms_avg4,
  handset_topic_1,
  handset_topic_2,
  handset_topic_3,
  handset_topic_4,
  handset_topic_5,
  uc_zero_day_prediction_inactive_to_active_days_ratio,
  uc_zero_day_prediction_weekend_to_weekday_voice_count_ratio,
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
  uc_zero_day_prediction_average_daily_voice_hour,
  uc_zero_day_prediction_average_daily_topup_count,
  uc_zero_day_prediction_total_topup_count_capped_at_two,
  uc_zero_day_prediction_average_topup_cost,
  uc_zero_day_prediction_tenure,
  uc_zero_day_prediction_tenure_group,
  uc_zero_day_prediction_activation_weekday
FROM work.modelling_data_matrix_sl24;

DROP TABLE work.modelling_data_matrix_sl24;

-- 2014-06-10 HMa: Drop old modelling data tables:
DROP TABLE work.modelling_data;
DELETE FROM core.partition_sequence_tables WHERE table_name = 'work.modelling_data';
DELETE FROM core.partition_sequence_create_times WHERE table_name = 'work.modelling_data';
DROP TABLE work.modelling_variables;
DELETE FROM core.partition_sequence_tables WHERE table_name = 'work.modelling_variables';
DELETE FROM core.partition_sequence_create_times WHERE table_name = 'work.modelling_variables';

--2014-07-22 JVi: Fixing ICIF-216
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


CREATE OR REPLACE FUNCTION work.churn_model_quality_verification(in_mod_job_id integer)
  RETURNS void AS
$BODY$
/*
 * SUMMARY
 * 
 * Adds information for quality charts for applied models, when churners are known.
 * 
 * VERSION
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

--DROP FUNCTION work.combine_modelling_data(integer);

-- Drop functions only used in this file, these should be the last rows:
DROP FUNCTION IF EXISTS core.upgrade_24_to_25_add_missing_partitions(text);

----------------Geolocation predictors--------------------------------------
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
		  AND bb.b_cell_id IS NOT NULL
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


  
CREATE OR REPLACE FUNCTION work.insert_churn_inactivity_template_model()
RETURNS void AS

$BODY$ 
    
DECLARE
template_model_id integer;
  
BEGIN
    
SELECT core.nextval_with_partitions('work.tmp_model_sequence') INTO template_model_id;
                        
INSERT INTO work.module_template_models
VALUES
(template_model_id,0,0,0,'model_type','logistic'),
(template_model_id,0,0,0,'use_case','churn_inactivity'),
(template_model_id,0,0,0,'target_name','target_churn_inactivity'),
(template_model_id,0,0,0,'is_parent','true'),
(template_model_id,0,0,0,'model_name','churn_inactivity_template_model'),
(template_model_id,0,0,0,'tempmodel_date','2014-10-29 11:09:20'),
(template_model_id,1,0,0,'varname','alias_count'),
(template_model_id,2,0,0,'varname','callsmsratio'),
(template_model_id,3,0,0,'varname','mr_count_ratio'),
(template_model_id,4,0,0,'varname','mr_ratio'),
(template_model_id,5,0,0,'varname','smscount'),
(template_model_id,6,0,0,'varname','smscountday'),
(template_model_id,7,0,0,'varname','smscountevening'),
(template_model_id,8,0,0,'varname','smscountweekday'),
(template_model_id,9,0,0,'varname','voicecount'),
(template_model_id,10,0,0,'varname','voicecountday'),
(template_model_id,11,0,0,'varname','voicecountevening'),
(template_model_id,12,0,0,'varname','voicecountweekday'),
(template_model_id,13,0,0,'varname','voicesum'),
(template_model_id,14,0,0,'varname','voicesumday'),
(template_model_id,15,0,0,'varname','voicesumevening'),
(template_model_id,16,0,0,'varname','voicesumweekday'),
(template_model_id,17,0,0,'varname','week_entropy'),
(template_model_id,18,0,0,'varname','week_mean'),
(template_model_id,19,0,0,'varname','weekly_cell_id_count1'),
(template_model_id,20,0,0,'varname','weekly_cell_id_count2'),
(template_model_id,21,0,0,'varname','weekly_cell_id_count3'),
(template_model_id,22,0,0,'varname','weekly_cell_id_count4'),
(template_model_id,23,0,0,'varname','weekly_cost1'),
(template_model_id,24,0,0,'varname','weekly_cost2'),
(template_model_id,25,0,0,'varname','weekly_cost3'),
(template_model_id,26,0,0,'varname','weekly_cost4'),
(template_model_id,27,0,0,'varname','daily_voice_activity1'),
(template_model_id,28,0,0,'varname','daily_voice_activity2'),
(template_model_id,29,0,0,'varname','daily_voice_activity3'),
(template_model_id,30,0,0,'varname','daily_voice_activity4'),
(template_model_id,31,0,0,'varname','weekly_voice_cost1'),
(template_model_id,32,0,0,'varname','weekly_voice_cost2'),
(template_model_id,33,0,0,'varname','weekly_voice_cost3'),
(template_model_id,34,0,0,'varname','weekly_voice_cost4'),
(template_model_id,35,0,0,'varname','weekly_voice_count1'),
(template_model_id,36,0,0,'varname','weekly_voice_count2'),
(template_model_id,37,0,0,'varname','weekly_voice_count3'),
(template_model_id,38,0,0,'varname','weekly_voice_count4'),
(template_model_id,39,0,0,'varname','weekly_voice_duration1'),
(template_model_id,40,0,0,'varname','weekly_voice_duration2'),
(template_model_id,41,0,0,'varname','weekly_voice_duration3'),
(template_model_id,42,0,0,'varname','weekly_voice_duration4'),
(template_model_id,43,0,0,'varname','weekly_voice_neigh_count1'),
(template_model_id,44,0,0,'varname','weekly_voice_neigh_count2'),
(template_model_id,45,0,0,'varname','weekly_voice_neigh_count3'),
(template_model_id,46,0,0,'varname','weekly_voice_neigh_count4'),
(template_model_id,47,0,0,'varname','daily_sms_activity1'),
(template_model_id,48,0,0,'varname','daily_sms_activity2'),
(template_model_id,49,0,0,'varname','daily_sms_activity3'),
(template_model_id,50,0,0,'varname','daily_sms_activity4'),
(template_model_id,51,0,0,'varname','weekly_sms_cost1'),
(template_model_id,52,0,0,'varname','weekly_sms_cost2'),
(template_model_id,53,0,0,'varname','weekly_sms_cost3'),
(template_model_id,54,0,0,'varname','weekly_sms_cost4'),
(template_model_id,55,0,0,'varname','weekly_sms_count1'),
(template_model_id,56,0,0,'varname','weekly_sms_count2'),
(template_model_id,57,0,0,'varname','weekly_sms_count3'),
(template_model_id,58,0,0,'varname','weekly_sms_count4'),
(template_model_id,59,0,0,'varname','daily_data_activity1'),
(template_model_id,60,0,0,'varname','daily_data_activity2'),
(template_model_id,61,0,0,'varname','daily_data_activity3'),
(template_model_id,62,0,0,'varname','daily_data_activity4'),
(template_model_id,63,0,0,'varname','weekly_data_count1'),
(template_model_id,64,0,0,'varname','weekly_data_count2'),
(template_model_id,65,0,0,'varname','weekly_data_count3'),
(template_model_id,66,0,0,'varname','weekly_data_count4'),
(template_model_id,67,0,0,'varname','weekly_data_usage1'),
(template_model_id,68,0,0,'varname','weekly_data_usage2'),
(template_model_id,69,0,0,'varname','weekly_data_usage3'),
(template_model_id,70,0,0,'varname','weekly_data_usage4'),
(template_model_id,71,0,0,'varname','inact'),
(template_model_id,72,0,0,'varname','weekly_cell_id_count_rec1'),
(template_model_id,73,0,0,'varname','weekly_cell_id_count_rec2'),
(template_model_id,74,0,0,'varname','weekly_cell_id_count_rec3'),
(template_model_id,75,0,0,'varname','weekly_cell_id_count_rec4'),
(template_model_id,76,0,0,'varname','daily_voice_activity_rec1'),
(template_model_id,77,0,0,'varname','daily_voice_activity_rec2'),
(template_model_id,78,0,0,'varname','daily_voice_activity_rec3'),
(template_model_id,79,0,0,'varname','daily_voice_activity_rec4'),
(template_model_id,80,0,0,'varname','weekly_voice_count_rec1'),
(template_model_id,81,0,0,'varname','weekly_voice_count_rec2'),
(template_model_id,82,0,0,'varname','weekly_voice_count_rec3'),
(template_model_id,83,0,0,'varname','weekly_voice_count_rec4'),
(template_model_id,84,0,0,'varname','weekly_voice_duration_rec1'),
(template_model_id,85,0,0,'varname','weekly_voice_duration_rec2'),
(template_model_id,86,0,0,'varname','weekly_voice_duration_rec3'),
(template_model_id,87,0,0,'varname','weekly_voice_duration_rec4'),
(template_model_id,88,0,0,'varname','daily_sms_activity_rec1'),
(template_model_id,89,0,0,'varname','daily_sms_activity_rec2'),
(template_model_id,90,0,0,'varname','daily_sms_activity_rec3'),
(template_model_id,91,0,0,'varname','daily_sms_activity_rec4'),
(template_model_id,92,0,0,'varname','weekly_sms_count_rec1'),
(template_model_id,93,0,0,'varname','weekly_sms_count_rec2'),
(template_model_id,94,0,0,'varname','weekly_sms_count_rec3'),
(template_model_id,95,0,0,'varname','weekly_sms_count_rec4'),
(template_model_id,96,0,0,'varname','inact_rec'),
(template_model_id,97,0,0,'varname','topup_amount_avg1'),
(template_model_id,98,0,0,'varname','topup_amount_avg2'),
(template_model_id,99,0,0,'varname','topup_amount_avg3'),
(template_model_id,100,0,0,'varname','topup_amount_avg4'),
(template_model_id,101,0,0,'varname','topup_bonus_avg1'),
(template_model_id,102,0,0,'varname','topup_bonus_avg2'),
(template_model_id,103,0,0,'varname','topup_bonus_avg3'),
(template_model_id,104,0,0,'varname','topup_bonus_avg4'),
(template_model_id,105,0,0,'varname','topup_bonus_per_amount_avg1'),
(template_model_id,106,0,0,'varname','topup_bonus_per_amount_avg2'),
(template_model_id,107,0,0,'varname','topup_bonus_per_amount_avg3'),
(template_model_id,108,0,0,'varname','topup_bonus_per_amount_avg4'),
(template_model_id,109,0,0,'varname','topup_count_weekly1'),
(template_model_id,110,0,0,'varname','topup_count_weekly2'),
(template_model_id,111,0,0,'varname','topup_count_weekly3'),
(template_model_id,112,0,0,'varname','topup_count_weekly4'),
(template_model_id,113,0,0,'varname','topup_days_from_last'),
(template_model_id,114,0,0,'varname','topup_days_to_next1'),
(template_model_id,115,0,0,'varname','topup_days_to_next2'),
(template_model_id,116,0,0,'varname','topup_days_interval'),
(template_model_id,117,0,0,'varname','topup_free_avg1'),
(template_model_id,118,0,0,'varname','topup_free_avg2'),
(template_model_id,119,0,0,'varname','topup_free_avg3'),
(template_model_id,120,0,0,'varname','topup_free_avg4'),
(template_model_id,121,0,0,'varname','topup_free_count_weekly1'),
(template_model_id,122,0,0,'varname','topup_free_count_weekly2'),
(template_model_id,123,0,0,'varname','topup_free_count_weekly3'),
(template_model_id,124,0,0,'varname','topup_free_count_weekly4'),
(template_model_id,125,0,0,'varname','topup_interval_from_last_call_avg1'),
(template_model_id,126,0,0,'varname','topup_interval_from_last_call_avg2'),
(template_model_id,127,0,0,'varname','topup_interval_from_last_call_avg3'),
(template_model_id,128,0,0,'varname','topup_interval_from_last_call_avg4'),
(template_model_id,129,0,0,'varname','topup_interval_from_last_sms_avg1'),
(template_model_id,130,0,0,'varname','topup_interval_from_last_sms_avg2'),
(template_model_id,131,0,0,'varname','topup_interval_from_last_sms_avg3'),
(template_model_id,132,0,0,'varname','topup_interval_from_last_sms_avg4'),
(template_model_id,133,0,0,'varname','topup_interval_to_next_call_avg1'),
(template_model_id,134,0,0,'varname','topup_interval_to_next_call_avg2'),
(template_model_id,135,0,0,'varname','topup_interval_to_next_call_avg3'),
(template_model_id,136,0,0,'varname','topup_interval_to_next_call_avg4'),
(template_model_id,137,0,0,'varname','topup_interval_to_next_sms_avg1'),
(template_model_id,138,0,0,'varname','topup_interval_to_next_sms_avg2'),
(template_model_id,139,0,0,'varname','topup_interval_to_next_sms_avg3'),
(template_model_id,140,0,0,'varname','topup_interval_to_next_sms_avg4'),
(template_model_id,141,0,0,'varname','topup_count_weekly_daytime1'),
(template_model_id,142,0,0,'varname','topup_count_weekly_daytime2'),
(template_model_id,143,0,0,'varname','topup_count_weekly_daytime3'),
(template_model_id,144,0,0,'varname','topup_count_weekly_daytime4'),
(template_model_id,145,0,0,'varname','topup_count_weekly_evening1'),
(template_model_id,146,0,0,'varname','topup_count_weekly_evening2'),
(template_model_id,147,0,0,'varname','topup_count_weekly_evening3'),
(template_model_id,148,0,0,'varname','topup_count_weekly_evening4'),
(template_model_id,149,0,0,'varname','topup_count_weekly_nighttime1'),
(template_model_id,150,0,0,'varname','topup_count_weekly_nighttime2'),
(template_model_id,151,0,0,'varname','topup_count_weekly_nighttime3'),
(template_model_id,152,0,0,'varname','topup_count_weekly_nighttime4'),
(template_model_id,153,0,0,'varname','topup_count_weekly_weekend1'),
(template_model_id,154,0,0,'varname','topup_count_weekly_weekend2'),
(template_model_id,155,0,0,'varname','topup_count_weekly_weekend3'),
(template_model_id,156,0,0,'varname','topup_count_weekly_weekend4'),
(template_model_id,157,0,0,'varname','knn'),
(template_model_id,158,0,0,'varname','knn_2'),
(template_model_id,159,0,0,'varname','k_offnet'),
(template_model_id,160,0,0,'varname','k_target'),
(template_model_id,161,0,0,'varname','age'),
(template_model_id,162,0,0,'varname','alpha'),
(template_model_id,163,0,0,'varname','c'),
(template_model_id,164,0,0,'varname','alpha_2'),
(template_model_id,165,0,0,'varname','c_2'),
(template_model_id,166,0,0,'varname','churn_score'),
(template_model_id,167,0,0,'varname','contr_length'),
(template_model_id,168,0,0,'varname','contr_remain'),
(template_model_id,169,0,0,'varname','handset_age'),
(template_model_id,170,0,0,'varname','k'),
(template_model_id,171,0,0,'varname','kshell'),
(template_model_id,172,0,0,'varname','socrev'),
(template_model_id,173,0,0,'varname','socrevest'),
(template_model_id,174,0,0,'varname','k_2'),
(template_model_id,175,0,0,'varname','kshell_2'),
(template_model_id,176,0,0,'varname','socrev_2'),
(template_model_id,177,0,0,'varname','socrevest_2'),
(template_model_id,178,0,0,'varname','monthly_arpu'),
(template_model_id,179,0,0,'varname','subscriber_value'),
(template_model_id,180,0,0,'varname','wec'),
(template_model_id,181,0,0,'varname','handset_topic_1'),
(template_model_id,182,0,0,'varname','handset_topic_2'),
(template_model_id,183,0,0,'varname','handset_topic_3'),
(template_model_id,184,0,0,'varname','handset_topic_4'),
(template_model_id,185,0,0,'varname','handset_topic_5'),
(template_model_id,186,0,0,'varname','cell_events_topic_1_1'),
(template_model_id,187,0,0,'varname','cell_events_topic_2_1'),
(template_model_id,188,0,0,'varname','cell_events_topic_3_1'),
(template_model_id,189,0,0,'varname','cell_events_topic_4_1'),
(template_model_id,190,0,0,'varname','cell_events_topic_5_1'),
(template_model_id,191,0,0,'varname','cell_events_topic_1_2'),
(template_model_id,192,0,0,'varname','cell_events_topic_2_2'),
(template_model_id,193,0,0,'varname','cell_events_topic_3_2'),
(template_model_id,194,0,0,'varname','cell_events_topic_4_2'),
(template_model_id,195,0,0,'varname','cell_events_topic_5_2'),
(template_model_id,196,0,0,'varname','mobility'),
(template_model_id,197,0,0,'varname','alltime_long_diversity'),
(template_model_id,198,0,0,'varname','leisure_long_diversity'),
(template_model_id,199,0,0,'varname','business_long_diversity'),
(template_model_id,200,0,0,'varname','alltime_short_diversity'),
(template_model_id,201,0,0,'varname','leisure_short_diversity'),
(template_model_id,202,0,0,'varname','business_short_diversity'),
(template_model_id,1,0,0,'vartype','numeric'),
(template_model_id,2,0,0,'vartype','numeric'),
(template_model_id,3,0,0,'vartype','numeric'),
(template_model_id,4,0,0,'vartype','numeric'),
(template_model_id,5,0,0,'vartype','numeric'),
(template_model_id,6,0,0,'vartype','numeric'),
(template_model_id,7,0,0,'vartype','numeric'),
(template_model_id,8,0,0,'vartype','numeric'),
(template_model_id,9,0,0,'vartype','numeric'),
(template_model_id,10,0,0,'vartype','numeric'),
(template_model_id,11,0,0,'vartype','numeric'),
(template_model_id,12,0,0,'vartype','numeric'),
(template_model_id,13,0,0,'vartype','numeric'),
(template_model_id,14,0,0,'vartype','numeric'),
(template_model_id,15,0,0,'vartype','numeric'),
(template_model_id,16,0,0,'vartype','numeric'),
(template_model_id,17,0,0,'vartype','numeric'),
(template_model_id,18,0,0,'vartype','numeric'),
(template_model_id,19,0,0,'vartype','numeric'),
(template_model_id,20,0,0,'vartype','numeric'),
(template_model_id,21,0,0,'vartype','numeric'),
(template_model_id,22,0,0,'vartype','numeric'),
(template_model_id,23,0,0,'vartype','numeric'),
(template_model_id,24,0,0,'vartype','numeric'),
(template_model_id,25,0,0,'vartype','numeric'),
(template_model_id,26,0,0,'vartype','numeric'),
(template_model_id,27,0,0,'vartype','numeric'),
(template_model_id,28,0,0,'vartype','numeric'),
(template_model_id,29,0,0,'vartype','numeric'),
(template_model_id,30,0,0,'vartype','numeric'),
(template_model_id,31,0,0,'vartype','numeric'),
(template_model_id,32,0,0,'vartype','numeric'),
(template_model_id,33,0,0,'vartype','numeric'),
(template_model_id,34,0,0,'vartype','numeric'),
(template_model_id,35,0,0,'vartype','numeric'),
(template_model_id,36,0,0,'vartype','numeric'),
(template_model_id,37,0,0,'vartype','numeric'),
(template_model_id,38,0,0,'vartype','numeric'),
(template_model_id,39,0,0,'vartype','numeric'),
(template_model_id,40,0,0,'vartype','numeric'),
(template_model_id,41,0,0,'vartype','numeric'),
(template_model_id,42,0,0,'vartype','numeric'),
(template_model_id,43,0,0,'vartype','numeric'),
(template_model_id,44,0,0,'vartype','numeric'),
(template_model_id,45,0,0,'vartype','numeric'),
(template_model_id,46,0,0,'vartype','numeric'),
(template_model_id,47,0,0,'vartype','numeric'),
(template_model_id,48,0,0,'vartype','numeric'),
(template_model_id,49,0,0,'vartype','numeric'),
(template_model_id,50,0,0,'vartype','numeric'),
(template_model_id,51,0,0,'vartype','numeric'),
(template_model_id,52,0,0,'vartype','numeric'),
(template_model_id,53,0,0,'vartype','numeric'),
(template_model_id,54,0,0,'vartype','numeric'),
(template_model_id,55,0,0,'vartype','numeric'),
(template_model_id,56,0,0,'vartype','numeric'),
(template_model_id,57,0,0,'vartype','numeric'),
(template_model_id,58,0,0,'vartype','numeric'),
(template_model_id,59,0,0,'vartype','numeric'),
(template_model_id,60,0,0,'vartype','numeric'),
(template_model_id,61,0,0,'vartype','numeric'),
(template_model_id,62,0,0,'vartype','numeric'),
(template_model_id,63,0,0,'vartype','numeric'),
(template_model_id,64,0,0,'vartype','numeric'),
(template_model_id,65,0,0,'vartype','numeric'),
(template_model_id,66,0,0,'vartype','numeric'),
(template_model_id,67,0,0,'vartype','numeric'),
(template_model_id,68,0,0,'vartype','numeric'),
(template_model_id,69,0,0,'vartype','numeric'),
(template_model_id,70,0,0,'vartype','numeric'),
(template_model_id,71,0,0,'vartype','numeric'),
(template_model_id,72,0,0,'vartype','numeric'),
(template_model_id,73,0,0,'vartype','numeric'),
(template_model_id,74,0,0,'vartype','numeric'),
(template_model_id,75,0,0,'vartype','numeric'),
(template_model_id,76,0,0,'vartype','numeric'),
(template_model_id,77,0,0,'vartype','numeric'),
(template_model_id,78,0,0,'vartype','numeric'),
(template_model_id,79,0,0,'vartype','numeric'),
(template_model_id,80,0,0,'vartype','numeric'),
(template_model_id,81,0,0,'vartype','numeric'),
(template_model_id,82,0,0,'vartype','numeric'),
(template_model_id,83,0,0,'vartype','numeric'),
(template_model_id,84,0,0,'vartype','numeric'),
(template_model_id,85,0,0,'vartype','numeric'),
(template_model_id,86,0,0,'vartype','numeric'),
(template_model_id,87,0,0,'vartype','numeric'),
(template_model_id,88,0,0,'vartype','numeric'),
(template_model_id,89,0,0,'vartype','numeric'),
(template_model_id,90,0,0,'vartype','numeric'),
(template_model_id,91,0,0,'vartype','numeric'),
(template_model_id,92,0,0,'vartype','numeric'),
(template_model_id,93,0,0,'vartype','numeric'),
(template_model_id,94,0,0,'vartype','numeric'),
(template_model_id,95,0,0,'vartype','numeric'),
(template_model_id,96,0,0,'vartype','numeric'),
(template_model_id,97,0,0,'vartype','numeric'),
(template_model_id,98,0,0,'vartype','numeric'),
(template_model_id,99,0,0,'vartype','numeric'),
(template_model_id,100,0,0,'vartype','numeric'),
(template_model_id,101,0,0,'vartype','numeric'),
(template_model_id,102,0,0,'vartype','numeric'),
(template_model_id,103,0,0,'vartype','numeric'),
(template_model_id,104,0,0,'vartype','numeric'),
(template_model_id,105,0,0,'vartype','numeric'),
(template_model_id,106,0,0,'vartype','numeric'),
(template_model_id,107,0,0,'vartype','numeric'),
(template_model_id,108,0,0,'vartype','numeric'),
(template_model_id,109,0,0,'vartype','numeric'),
(template_model_id,110,0,0,'vartype','numeric'),
(template_model_id,111,0,0,'vartype','numeric'),
(template_model_id,112,0,0,'vartype','numeric'),
(template_model_id,113,0,0,'vartype','numeric'),
(template_model_id,114,0,0,'vartype','numeric'),
(template_model_id,115,0,0,'vartype','numeric'),
(template_model_id,116,0,0,'vartype','numeric'),
(template_model_id,117,0,0,'vartype','numeric'),
(template_model_id,118,0,0,'vartype','numeric'),
(template_model_id,119,0,0,'vartype','numeric'),
(template_model_id,120,0,0,'vartype','numeric'),
(template_model_id,121,0,0,'vartype','numeric'),
(template_model_id,122,0,0,'vartype','numeric'),
(template_model_id,123,0,0,'vartype','numeric'),
(template_model_id,124,0,0,'vartype','numeric'),
(template_model_id,125,0,0,'vartype','numeric'),
(template_model_id,126,0,0,'vartype','numeric'),
(template_model_id,127,0,0,'vartype','numeric'),
(template_model_id,128,0,0,'vartype','numeric'),
(template_model_id,129,0,0,'vartype','numeric'),
(template_model_id,130,0,0,'vartype','numeric'),
(template_model_id,131,0,0,'vartype','numeric'),
(template_model_id,132,0,0,'vartype','numeric'),
(template_model_id,133,0,0,'vartype','numeric'),
(template_model_id,134,0,0,'vartype','numeric'),
(template_model_id,135,0,0,'vartype','numeric'),
(template_model_id,136,0,0,'vartype','numeric'),
(template_model_id,137,0,0,'vartype','numeric'),
(template_model_id,138,0,0,'vartype','numeric'),
(template_model_id,139,0,0,'vartype','numeric'),
(template_model_id,140,0,0,'vartype','numeric'),
(template_model_id,141,0,0,'vartype','numeric'),
(template_model_id,142,0,0,'vartype','numeric'),
(template_model_id,143,0,0,'vartype','numeric'),
(template_model_id,144,0,0,'vartype','numeric'),
(template_model_id,145,0,0,'vartype','numeric'),
(template_model_id,146,0,0,'vartype','numeric'),
(template_model_id,147,0,0,'vartype','numeric'),
(template_model_id,148,0,0,'vartype','numeric'),
(template_model_id,149,0,0,'vartype','numeric'),
(template_model_id,150,0,0,'vartype','numeric'),
(template_model_id,151,0,0,'vartype','numeric'),
(template_model_id,152,0,0,'vartype','numeric'),
(template_model_id,153,0,0,'vartype','numeric'),
(template_model_id,154,0,0,'vartype','numeric'),
(template_model_id,155,0,0,'vartype','numeric'),
(template_model_id,156,0,0,'vartype','numeric'),
(template_model_id,157,0,0,'vartype','numeric'),
(template_model_id,158,0,0,'vartype','numeric'),
(template_model_id,159,0,0,'vartype','numeric'),
(template_model_id,160,0,0,'vartype','numeric'),
(template_model_id,161,0,0,'vartype','numeric'),
(template_model_id,162,0,0,'vartype','numeric'),
(template_model_id,163,0,0,'vartype','numeric'),
(template_model_id,164,0,0,'vartype','numeric'),
(template_model_id,165,0,0,'vartype','numeric'),
(template_model_id,166,0,0,'vartype','numeric'),
(template_model_id,167,0,0,'vartype','numeric'),
(template_model_id,168,0,0,'vartype','numeric'),
(template_model_id,169,0,0,'vartype','numeric'),
(template_model_id,170,0,0,'vartype','numeric'),
(template_model_id,171,0,0,'vartype','numeric'),
(template_model_id,172,0,0,'vartype','numeric'),
(template_model_id,173,0,0,'vartype','numeric'),
(template_model_id,174,0,0,'vartype','numeric'),
(template_model_id,175,0,0,'vartype','numeric'),
(template_model_id,176,0,0,'vartype','numeric'),
(template_model_id,177,0,0,'vartype','numeric'),
(template_model_id,178,0,0,'vartype','numeric'),
(template_model_id,179,0,0,'vartype','numeric'),
(template_model_id,180,0,0,'vartype','numeric'),
(template_model_id,181,0,0,'vartype','numeric'),
(template_model_id,182,0,0,'vartype','numeric'),
(template_model_id,183,0,0,'vartype','numeric'),
(template_model_id,184,0,0,'vartype','numeric'),
(template_model_id,185,0,0,'vartype','numeric'),
(template_model_id,186,0,0,'vartype','numeric'),
(template_model_id,187,0,0,'vartype','numeric'),
(template_model_id,188,0,0,'vartype','numeric'),
(template_model_id,189,0,0,'vartype','numeric'),
(template_model_id,190,0,0,'vartype','numeric'),
(template_model_id,191,0,0,'vartype','numeric'),
(template_model_id,192,0,0,'vartype','numeric'),
(template_model_id,193,0,0,'vartype','numeric'),
(template_model_id,194,0,0,'vartype','numeric'),
(template_model_id,195,0,0,'vartype','numeric'),
(template_model_id,196,0,0,'vartype','numeric'),
(template_model_id,197,0,0,'vartype','numeric'),
(template_model_id,198,0,0,'vartype','numeric'),
(template_model_id,199,0,0,'vartype','numeric'),
(template_model_id,200,0,0,'vartype','numeric'),
(template_model_id,201,0,0,'vartype','numeric'),
(template_model_id,202,0,0,'vartype','numeric'),
(template_model_id,203,0,0,'varname','country'),
(template_model_id,204,0,0,'varname','gender'),
(template_model_id,205,0,0,'varname','handset_model'),
(template_model_id,206,0,0,'varname','language'),
(template_model_id,207,0,0,'varname','no_churn_score'),
(template_model_id,208,0,0,'varname','payment_type'),
(template_model_id,209,0,0,'varname','separate_node'),
(template_model_id,210,0,0,'varname','separate_node_2'),
(template_model_id,211,0,0,'varname','subscriber_segment'),
(template_model_id,212,0,0,'varname','subscription_type'),
(template_model_id,213,0,0,'varname','tariff_plan'),
(template_model_id,214,0,0,'varname','zip'),
(template_model_id,203,0,0,'vartype','categorical'),
(template_model_id,204,0,0,'vartype','categorical'),
(template_model_id,205,0,0,'vartype','categorical'),
(template_model_id,206,0,0,'vartype','categorical'),
(template_model_id,207,0,0,'vartype','categorical'),
(template_model_id,208,0,0,'vartype','categorical'),
(template_model_id,209,0,0,'vartype','categorical'),
(template_model_id,210,0,0,'vartype','categorical'),
(template_model_id,211,0,0,'vartype','categorical'),
(template_model_id,212,0,0,'vartype','categorical'),
(template_model_id,213,0,0,'vartype','categorical'),
(template_model_id,214,0,0,'vartype','categorical'),
(template_model_id,1,0,0,'preprocessfun','ecdf'),
(template_model_id,2,0,0,'preprocessfun','ecdf'),
(template_model_id,3,0,0,'preprocessfun','ecdf'),
(template_model_id,4,0,0,'preprocessfun','ecdf'),
(template_model_id,5,0,0,'preprocessfun','ecdf'),
(template_model_id,6,0,0,'preprocessfun','ecdf'),
(template_model_id,7,0,0,'preprocessfun','ecdf'),
(template_model_id,8,0,0,'preprocessfun','ecdf'),
(template_model_id,9,0,0,'preprocessfun','ecdf'),
(template_model_id,10,0,0,'preprocessfun','ecdf'),
(template_model_id,11,0,0,'preprocessfun','ecdf'),
(template_model_id,12,0,0,'preprocessfun','ecdf'),
(template_model_id,13,0,0,'preprocessfun','ecdf'),
(template_model_id,14,0,0,'preprocessfun','ecdf'),
(template_model_id,15,0,0,'preprocessfun','ecdf'),
(template_model_id,16,0,0,'preprocessfun','ecdf'),
(template_model_id,17,0,0,'preprocessfun','ecdf'),
(template_model_id,18,0,0,'preprocessfun','ecdf'),
(template_model_id,19,0,0,'preprocessfun','ecdf'),
(template_model_id,20,0,0,'preprocessfun','ecdf'),
(template_model_id,21,0,0,'preprocessfun','ecdf'),
(template_model_id,22,0,0,'preprocessfun','ecdf'),
(template_model_id,23,0,0,'preprocessfun','ecdf'),
(template_model_id,24,0,0,'preprocessfun','ecdf'),
(template_model_id,25,0,0,'preprocessfun','ecdf'),
(template_model_id,26,0,0,'preprocessfun','ecdf'),
(template_model_id,27,0,0,'preprocessfun','ecdf'),
(template_model_id,28,0,0,'preprocessfun','ecdf'),
(template_model_id,29,0,0,'preprocessfun','ecdf'),
(template_model_id,30,0,0,'preprocessfun','ecdf'),
(template_model_id,31,0,0,'preprocessfun','ecdf'),
(template_model_id,32,0,0,'preprocessfun','ecdf'),
(template_model_id,33,0,0,'preprocessfun','ecdf'),
(template_model_id,34,0,0,'preprocessfun','ecdf'),
(template_model_id,35,0,0,'preprocessfun','ecdf'),
(template_model_id,36,0,0,'preprocessfun','ecdf'),
(template_model_id,37,0,0,'preprocessfun','ecdf'),
(template_model_id,38,0,0,'preprocessfun','ecdf'),
(template_model_id,39,0,0,'preprocessfun','ecdf'),
(template_model_id,40,0,0,'preprocessfun','ecdf'),
(template_model_id,41,0,0,'preprocessfun','ecdf'),
(template_model_id,42,0,0,'preprocessfun','ecdf'),
(template_model_id,43,0,0,'preprocessfun','ecdf'),
(template_model_id,44,0,0,'preprocessfun','ecdf'),
(template_model_id,45,0,0,'preprocessfun','ecdf'),
(template_model_id,46,0,0,'preprocessfun','ecdf'),
(template_model_id,47,0,0,'preprocessfun','ecdf'),
(template_model_id,48,0,0,'preprocessfun','ecdf'),
(template_model_id,49,0,0,'preprocessfun','ecdf'),
(template_model_id,50,0,0,'preprocessfun','ecdf'),
(template_model_id,51,0,0,'preprocessfun','ecdf'),
(template_model_id,52,0,0,'preprocessfun','ecdf'),
(template_model_id,53,0,0,'preprocessfun','ecdf'),
(template_model_id,54,0,0,'preprocessfun','ecdf'),
(template_model_id,55,0,0,'preprocessfun','ecdf'),
(template_model_id,56,0,0,'preprocessfun','ecdf'),
(template_model_id,57,0,0,'preprocessfun','ecdf'),
(template_model_id,58,0,0,'preprocessfun','ecdf'),
(template_model_id,59,0,0,'preprocessfun','ecdf'),
(template_model_id,60,0,0,'preprocessfun','ecdf'),
(template_model_id,61,0,0,'preprocessfun','ecdf'),
(template_model_id,62,0,0,'preprocessfun','ecdf'),
(template_model_id,63,0,0,'preprocessfun','ecdf'),
(template_model_id,64,0,0,'preprocessfun','ecdf'),
(template_model_id,65,0,0,'preprocessfun','ecdf'),
(template_model_id,66,0,0,'preprocessfun','ecdf'),
(template_model_id,67,0,0,'preprocessfun','ecdf'),
(template_model_id,68,0,0,'preprocessfun','ecdf'),
(template_model_id,69,0,0,'preprocessfun','ecdf'),
(template_model_id,70,0,0,'preprocessfun','ecdf'),
(template_model_id,71,0,0,'preprocessfun','ecdf'),
(template_model_id,72,0,0,'preprocessfun','ecdf'),
(template_model_id,73,0,0,'preprocessfun','ecdf'),
(template_model_id,74,0,0,'preprocessfun','ecdf'),
(template_model_id,75,0,0,'preprocessfun','ecdf'),
(template_model_id,76,0,0,'preprocessfun','ecdf'),
(template_model_id,77,0,0,'preprocessfun','ecdf'),
(template_model_id,78,0,0,'preprocessfun','ecdf'),
(template_model_id,79,0,0,'preprocessfun','ecdf'),
(template_model_id,80,0,0,'preprocessfun','ecdf'),
(template_model_id,81,0,0,'preprocessfun','ecdf'),
(template_model_id,82,0,0,'preprocessfun','ecdf'),
(template_model_id,83,0,0,'preprocessfun','ecdf'),
(template_model_id,84,0,0,'preprocessfun','ecdf'),
(template_model_id,85,0,0,'preprocessfun','ecdf'),
(template_model_id,86,0,0,'preprocessfun','ecdf'),
(template_model_id,87,0,0,'preprocessfun','ecdf'),
(template_model_id,88,0,0,'preprocessfun','ecdf'),
(template_model_id,89,0,0,'preprocessfun','ecdf'),
(template_model_id,90,0,0,'preprocessfun','ecdf'),
(template_model_id,91,0,0,'preprocessfun','ecdf'),
(template_model_id,92,0,0,'preprocessfun','ecdf'),
(template_model_id,93,0,0,'preprocessfun','ecdf'),
(template_model_id,94,0,0,'preprocessfun','ecdf'),
(template_model_id,95,0,0,'preprocessfun','ecdf'),
(template_model_id,96,0,0,'preprocessfun','ecdf'),
(template_model_id,97,0,0,'preprocessfun','ecdf'),
(template_model_id,98,0,0,'preprocessfun','ecdf'),
(template_model_id,99,0,0,'preprocessfun','ecdf'),
(template_model_id,100,0,0,'preprocessfun','ecdf'),
(template_model_id,101,0,0,'preprocessfun','ecdf'),
(template_model_id,102,0,0,'preprocessfun','ecdf'),
(template_model_id,103,0,0,'preprocessfun','ecdf'),
(template_model_id,104,0,0,'preprocessfun','ecdf'),
(template_model_id,105,0,0,'preprocessfun','ecdf'),
(template_model_id,106,0,0,'preprocessfun','ecdf'),
(template_model_id,107,0,0,'preprocessfun','ecdf'),
(template_model_id,108,0,0,'preprocessfun','ecdf'),
(template_model_id,109,0,0,'preprocessfun','ecdf'),
(template_model_id,110,0,0,'preprocessfun','ecdf'),
(template_model_id,111,0,0,'preprocessfun','ecdf'),
(template_model_id,112,0,0,'preprocessfun','ecdf'),
(template_model_id,113,0,0,'preprocessfun','ecdf'),
(template_model_id,114,0,0,'preprocessfun','ecdf'),
(template_model_id,115,0,0,'preprocessfun','ecdf'),
(template_model_id,116,0,0,'preprocessfun','ecdf'),
(template_model_id,117,0,0,'preprocessfun','ecdf'),
(template_model_id,118,0,0,'preprocessfun','ecdf'),
(template_model_id,119,0,0,'preprocessfun','ecdf'),
(template_model_id,120,0,0,'preprocessfun','ecdf'),
(template_model_id,121,0,0,'preprocessfun','ecdf'),
(template_model_id,122,0,0,'preprocessfun','ecdf'),
(template_model_id,123,0,0,'preprocessfun','ecdf'),
(template_model_id,124,0,0,'preprocessfun','ecdf'),
(template_model_id,125,0,0,'preprocessfun','ecdf'),
(template_model_id,126,0,0,'preprocessfun','ecdf'),
(template_model_id,127,0,0,'preprocessfun','ecdf'),
(template_model_id,128,0,0,'preprocessfun','ecdf'),
(template_model_id,129,0,0,'preprocessfun','ecdf'),
(template_model_id,130,0,0,'preprocessfun','ecdf'),
(template_model_id,131,0,0,'preprocessfun','ecdf'),
(template_model_id,132,0,0,'preprocessfun','ecdf'),
(template_model_id,133,0,0,'preprocessfun','ecdf'),
(template_model_id,134,0,0,'preprocessfun','ecdf'),
(template_model_id,135,0,0,'preprocessfun','ecdf'),
(template_model_id,136,0,0,'preprocessfun','ecdf'),
(template_model_id,137,0,0,'preprocessfun','ecdf'),
(template_model_id,138,0,0,'preprocessfun','ecdf'),
(template_model_id,139,0,0,'preprocessfun','ecdf'),
(template_model_id,140,0,0,'preprocessfun','ecdf'),
(template_model_id,141,0,0,'preprocessfun','ecdf'),
(template_model_id,142,0,0,'preprocessfun','ecdf'),
(template_model_id,143,0,0,'preprocessfun','ecdf'),
(template_model_id,144,0,0,'preprocessfun','ecdf'),
(template_model_id,145,0,0,'preprocessfun','ecdf'),
(template_model_id,146,0,0,'preprocessfun','ecdf'),
(template_model_id,147,0,0,'preprocessfun','ecdf'),
(template_model_id,148,0,0,'preprocessfun','ecdf'),
(template_model_id,149,0,0,'preprocessfun','ecdf'),
(template_model_id,150,0,0,'preprocessfun','ecdf'),
(template_model_id,151,0,0,'preprocessfun','ecdf'),
(template_model_id,152,0,0,'preprocessfun','ecdf'),
(template_model_id,153,0,0,'preprocessfun','ecdf'),
(template_model_id,154,0,0,'preprocessfun','ecdf'),
(template_model_id,155,0,0,'preprocessfun','ecdf'),
(template_model_id,156,0,0,'preprocessfun','ecdf'),
(template_model_id,157,0,0,'preprocessfun','ecdf'),
(template_model_id,158,0,0,'preprocessfun','ecdf'),
(template_model_id,159,0,0,'preprocessfun','ecdf'),
(template_model_id,160,0,0,'preprocessfun','ecdf'),
(template_model_id,161,0,0,'preprocessfun','ecdf'),
(template_model_id,162,0,0,'preprocessfun','ecdf'),
(template_model_id,163,0,0,'preprocessfun','ecdf'),
(template_model_id,164,0,0,'preprocessfun','ecdf'),
(template_model_id,165,0,0,'preprocessfun','ecdf'),
(template_model_id,166,0,0,'preprocessfun','ecdf'),
(template_model_id,167,0,0,'preprocessfun','ecdf'),
(template_model_id,168,0,0,'preprocessfun','ecdf'),
(template_model_id,169,0,0,'preprocessfun','ecdf'),
(template_model_id,170,0,0,'preprocessfun','ecdf'),
(template_model_id,171,0,0,'preprocessfun','ecdf'),
(template_model_id,172,0,0,'preprocessfun','ecdf'),
(template_model_id,173,0,0,'preprocessfun','ecdf'),
(template_model_id,174,0,0,'preprocessfun','ecdf'),
(template_model_id,175,0,0,'preprocessfun','ecdf'),
(template_model_id,176,0,0,'preprocessfun','ecdf'),
(template_model_id,177,0,0,'preprocessfun','ecdf'),
(template_model_id,178,0,0,'preprocessfun','ecdf'),
(template_model_id,179,0,0,'preprocessfun','ecdf'),
(template_model_id,180,0,0,'preprocessfun','ecdf'),
(template_model_id,181,0,0,'preprocessfun','ecdf'),
(template_model_id,182,0,0,'preprocessfun','ecdf'),
(template_model_id,183,0,0,'preprocessfun','ecdf'),
(template_model_id,184,0,0,'preprocessfun','ecdf'),
(template_model_id,185,0,0,'preprocessfun','ecdf'),
(template_model_id,186,0,0,'preprocessfun','ecdf'),
(template_model_id,187,0,0,'preprocessfun','ecdf'),
(template_model_id,188,0,0,'preprocessfun','ecdf'),
(template_model_id,189,0,0,'preprocessfun','ecdf'),
(template_model_id,190,0,0,'preprocessfun','ecdf'),
(template_model_id,191,0,0,'preprocessfun','ecdf'),
(template_model_id,192,0,0,'preprocessfun','ecdf'),
(template_model_id,193,0,0,'preprocessfun','ecdf'),
(template_model_id,194,0,0,'preprocessfun','ecdf'),
(template_model_id,195,0,0,'preprocessfun','ecdf'),
(template_model_id,196,0,0,'preprocessfun','ecdf'),
(template_model_id,197,0,0,'preprocessfun','ecdf'),
(template_model_id,198,0,0,'preprocessfun','ecdf'),
(template_model_id,199,0,0,'preprocessfun','ecdf'),
(template_model_id,200,0,0,'preprocessfun','ecdf'),
(template_model_id,201,0,0,'preprocessfun','ecdf'),
(template_model_id,202,0,0,'preprocessfun','ecdf'),
(template_model_id,1,1,0,'rcs','3'),
(template_model_id,2,1,0,'rcs','3'),
(template_model_id,3,1,0,'rcs','3'),
(template_model_id,4,1,0,'rcs','3'),
(template_model_id,5,1,0,'rcs','3'),
(template_model_id,6,1,0,'rcs','3'),
(template_model_id,7,1,0,'rcs','3'),
(template_model_id,8,1,0,'rcs','3'),
(template_model_id,9,1,0,'rcs','3'),
(template_model_id,10,1,0,'rcs','3'),
(template_model_id,11,1,0,'rcs','3'),
(template_model_id,12,1,0,'rcs','3'),
(template_model_id,13,1,0,'rcs','3'),
(template_model_id,14,1,0,'rcs','3'),
(template_model_id,15,1,0,'rcs','3'),
(template_model_id,16,1,0,'rcs','3'),
(template_model_id,17,1,0,'rcs','3'),
(template_model_id,18,1,0,'rcs','3'),
(template_model_id,19,1,0,'rcs','3'),
(template_model_id,20,1,0,'rcs','3'),
(template_model_id,21,1,0,'rcs','3'),
(template_model_id,22,1,0,'rcs','3'),
(template_model_id,23,1,0,'rcs','3'),
(template_model_id,24,1,0,'rcs','3'),
(template_model_id,25,1,0,'rcs','3'),
(template_model_id,26,1,0,'rcs','3'),
(template_model_id,27,1,0,'rcs','3'),
(template_model_id,28,1,0,'rcs','3'),
(template_model_id,29,1,0,'rcs','3'),
(template_model_id,30,1,0,'rcs','3'),
(template_model_id,31,1,0,'rcs','3'),
(template_model_id,32,1,0,'rcs','3'),
(template_model_id,33,1,0,'rcs','3'),
(template_model_id,34,1,0,'rcs','3'),
(template_model_id,35,1,0,'rcs','3'),
(template_model_id,36,1,0,'rcs','3'),
(template_model_id,37,1,0,'rcs','3'),
(template_model_id,38,1,0,'rcs','3'),
(template_model_id,39,1,0,'rcs','3'),
(template_model_id,40,1,0,'rcs','3'),
(template_model_id,41,1,0,'rcs','3'),
(template_model_id,42,1,0,'rcs','3'),
(template_model_id,43,1,0,'rcs','3'),
(template_model_id,44,1,0,'rcs','3'),
(template_model_id,45,1,0,'rcs','3'),
(template_model_id,46,1,0,'rcs','3'),
(template_model_id,47,1,0,'rcs','3'),
(template_model_id,48,1,0,'rcs','3'),
(template_model_id,49,1,0,'rcs','3'),
(template_model_id,50,1,0,'rcs','3'),
(template_model_id,51,1,0,'rcs','3'),
(template_model_id,52,1,0,'rcs','3'),
(template_model_id,53,1,0,'rcs','3'),
(template_model_id,54,1,0,'rcs','3'),
(template_model_id,55,1,0,'rcs','3'),
(template_model_id,56,1,0,'rcs','3'),
(template_model_id,57,1,0,'rcs','3'),
(template_model_id,58,1,0,'rcs','3'),
(template_model_id,59,1,0,'rcs','3'),
(template_model_id,60,1,0,'rcs','3'),
(template_model_id,61,1,0,'rcs','3'),
(template_model_id,62,1,0,'rcs','3'),
(template_model_id,63,1,0,'rcs','3'),
(template_model_id,64,1,0,'rcs','3'),
(template_model_id,65,1,0,'rcs','3'),
(template_model_id,66,1,0,'rcs','3'),
(template_model_id,67,1,0,'rcs','3'),
(template_model_id,68,1,0,'rcs','3'),
(template_model_id,69,1,0,'rcs','3'),
(template_model_id,70,1,0,'rcs','3'),
(template_model_id,71,1,0,'rcs','5'),
(template_model_id,72,1,0,'rcs','3'),
(template_model_id,73,1,0,'rcs','3'),
(template_model_id,74,1,0,'rcs','3'),
(template_model_id,75,1,0,'rcs','3'),
(template_model_id,76,1,0,'rcs','3'),
(template_model_id,77,1,0,'rcs','3'),
(template_model_id,78,1,0,'rcs','3'),
(template_model_id,79,1,0,'rcs','3'),
(template_model_id,80,1,0,'rcs','3'),
(template_model_id,81,1,0,'rcs','3'),
(template_model_id,82,1,0,'rcs','3'),
(template_model_id,83,1,0,'rcs','3'),
(template_model_id,84,1,0,'rcs','3'),
(template_model_id,85,1,0,'rcs','3'),
(template_model_id,86,1,0,'rcs','3'),
(template_model_id,87,1,0,'rcs','3'),
(template_model_id,88,1,0,'rcs','3'),
(template_model_id,89,1,0,'rcs','3'),
(template_model_id,90,1,0,'rcs','3'),
(template_model_id,91,1,0,'rcs','3'),
(template_model_id,92,1,0,'rcs','3'),
(template_model_id,93,1,0,'rcs','3'),
(template_model_id,94,1,0,'rcs','3'),
(template_model_id,95,1,0,'rcs','3'),
(template_model_id,96,1,0,'rcs','5'),
(template_model_id,97,1,0,'rcs','3'),
(template_model_id,98,1,0,'rcs','3'),
(template_model_id,99,1,0,'rcs','3'),
(template_model_id,100,1,0,'rcs','3'),
(template_model_id,101,1,0,'rcs','3'),
(template_model_id,102,1,0,'rcs','3'),
(template_model_id,103,1,0,'rcs','3'),
(template_model_id,104,1,0,'rcs','3'),
(template_model_id,105,1,0,'rcs','3'),
(template_model_id,106,1,0,'rcs','3'),
(template_model_id,107,1,0,'rcs','3'),
(template_model_id,108,1,0,'rcs','3'),
(template_model_id,109,1,0,'rcs','3'),
(template_model_id,110,1,0,'rcs','3'),
(template_model_id,111,1,0,'rcs','3'),
(template_model_id,112,1,0,'rcs','3'),
(template_model_id,113,1,0,'rcs','3'),
(template_model_id,114,1,0,'rcs','3'),
(template_model_id,115,1,0,'rcs','3'),
(template_model_id,116,1,0,'rcs','3'),
(template_model_id,117,1,0,'rcs','3'),
(template_model_id,118,1,0,'rcs','3'),
(template_model_id,119,1,0,'rcs','3'),
(template_model_id,120,1,0,'rcs','3'),
(template_model_id,121,1,0,'rcs','3'),
(template_model_id,122,1,0,'rcs','3'),
(template_model_id,123,1,0,'rcs','3'),
(template_model_id,124,1,0,'rcs','3'),
(template_model_id,125,1,0,'rcs','3'),
(template_model_id,126,1,0,'rcs','3'),
(template_model_id,127,1,0,'rcs','3'),
(template_model_id,128,1,0,'rcs','3'),
(template_model_id,129,1,0,'rcs','3'),
(template_model_id,130,1,0,'rcs','3'),
(template_model_id,131,1,0,'rcs','3'),
(template_model_id,132,1,0,'rcs','3'),
(template_model_id,133,1,0,'rcs','3'),
(template_model_id,134,1,0,'rcs','3'),
(template_model_id,135,1,0,'rcs','3'),
(template_model_id,136,1,0,'rcs','3'),
(template_model_id,137,1,0,'rcs','3'),
(template_model_id,138,1,0,'rcs','3'),
(template_model_id,139,1,0,'rcs','3'),
(template_model_id,140,1,0,'rcs','3'),
(template_model_id,141,1,0,'rcs','3'),
(template_model_id,142,1,0,'rcs','3'),
(template_model_id,143,1,0,'rcs','3'),
(template_model_id,144,1,0,'rcs','3'),
(template_model_id,145,1,0,'rcs','3'),
(template_model_id,146,1,0,'rcs','3'),
(template_model_id,147,1,0,'rcs','3'),
(template_model_id,148,1,0,'rcs','3'),
(template_model_id,149,1,0,'rcs','3'),
(template_model_id,150,1,0,'rcs','3'),
(template_model_id,151,1,0,'rcs','3'),
(template_model_id,152,1,0,'rcs','3'),
(template_model_id,153,1,0,'rcs','3'),
(template_model_id,154,1,0,'rcs','3'),
(template_model_id,155,1,0,'rcs','3'),
(template_model_id,156,1,0,'rcs','3'),
(template_model_id,157,1,0,'rcs','3'),
(template_model_id,158,1,0,'rcs','3'),
(template_model_id,159,1,0,'rcs','3'),
(template_model_id,160,1,0,'rcs','3'),
(template_model_id,161,1,0,'rcs','3'),
(template_model_id,162,1,0,'rcs','3'),
(template_model_id,163,1,0,'rcs','3'),
(template_model_id,164,1,0,'rcs','3'),
(template_model_id,165,1,0,'rcs','3'),
(template_model_id,167,1,0,'rcs','5'),
(template_model_id,169,1,0,'rcs','3'),
(template_model_id,170,1,0,'rcs','3'),
(template_model_id,171,1,0,'rcs','3'),
(template_model_id,172,1,0,'rcs','3'),
(template_model_id,173,1,0,'rcs','3'),
(template_model_id,174,1,0,'rcs','3'),
(template_model_id,175,1,0,'rcs','3'),
(template_model_id,176,1,0,'rcs','3'),
(template_model_id,177,1,0,'rcs','3'),
(template_model_id,178,1,0,'rcs','3'),
(template_model_id,179,1,0,'rcs','3'),
(template_model_id,180,1,0,'rcs','3'),
(template_model_id,1,1,0,'freqlim','0.3'),
(template_model_id,2,1,0,'freqlim','0.1'),
(template_model_id,3,1,0,'freqlim','0.1'),
(template_model_id,4,1,0,'freqlim','0.1'),
(template_model_id,5,1,0,'freqlim','0.3'),
(template_model_id,6,1,0,'freqlim','0.3'),
(template_model_id,7,1,0,'freqlim','0.3'),
(template_model_id,8,1,0,'freqlim','0.3'),
(template_model_id,9,1,0,'freqlim','0.3'),
(template_model_id,10,1,0,'freqlim','0.3'),
(template_model_id,11,1,0,'freqlim','0.3'),
(template_model_id,12,1,0,'freqlim','0.3'),
(template_model_id,13,1,0,'freqlim','0.1'),
(template_model_id,14,1,0,'freqlim','0.1'),
(template_model_id,15,1,0,'freqlim','0.1'),
(template_model_id,16,1,0,'freqlim','0.1'),
(template_model_id,17,1,0,'freqlim','0.1'),
(template_model_id,18,1,0,'freqlim','0.1'),
(template_model_id,19,1,0,'freqlim','0.1'),
(template_model_id,20,1,0,'freqlim','0.1'),
(template_model_id,21,1,0,'freqlim','0.1'),
(template_model_id,22,1,0,'freqlim','0.1'),
(template_model_id,23,1,0,'freqlim','0.1'),
(template_model_id,24,1,0,'freqlim','0.1'),
(template_model_id,25,1,0,'freqlim','0.1'),
(template_model_id,26,1,0,'freqlim','0.1'),
(template_model_id,27,1,0,'freqlim','0.3'),
(template_model_id,28,1,0,'freqlim','0.3'),
(template_model_id,29,1,0,'freqlim','0.3'),
(template_model_id,30,1,0,'freqlim','0.3'),
(template_model_id,31,1,0,'freqlim','0.1'),
(template_model_id,32,1,0,'freqlim','0.1'),
(template_model_id,33,1,0,'freqlim','0.1'),
(template_model_id,34,1,0,'freqlim','0.1'),
(template_model_id,35,1,0,'freqlim','0.1'),
(template_model_id,36,1,0,'freqlim','0.1'),
(template_model_id,37,1,0,'freqlim','0.1'),
(template_model_id,38,1,0,'freqlim','0.1'),
(template_model_id,39,1,0,'freqlim','0.1'),
(template_model_id,40,1,0,'freqlim','0.1'),
(template_model_id,41,1,0,'freqlim','0.1'),
(template_model_id,42,1,0,'freqlim','0.1'),
(template_model_id,43,1,0,'freqlim','0.1'),
(template_model_id,44,1,0,'freqlim','0.1'),
(template_model_id,45,1,0,'freqlim','0.1'),
(template_model_id,46,1,0,'freqlim','0.1'),
(template_model_id,47,1,0,'freqlim','0.3'),
(template_model_id,48,1,0,'freqlim','0.3'),
(template_model_id,49,1,0,'freqlim','0.3'),
(template_model_id,50,1,0,'freqlim','0.3'),
(template_model_id,51,1,0,'freqlim','0.1'),
(template_model_id,52,1,0,'freqlim','0.1'),
(template_model_id,53,1,0,'freqlim','0.1'),
(template_model_id,54,1,0,'freqlim','0.1'),
(template_model_id,55,1,0,'freqlim','0.1'),
(template_model_id,56,1,0,'freqlim','0.1'),
(template_model_id,57,1,0,'freqlim','0.1'),
(template_model_id,58,1,0,'freqlim','0.1'),
(template_model_id,59,1,0,'freqlim','0.3'),
(template_model_id,60,1,0,'freqlim','0.3'),
(template_model_id,61,1,0,'freqlim','0.3'),
(template_model_id,62,1,0,'freqlim','0.3'),
(template_model_id,63,1,0,'freqlim','0.1'),
(template_model_id,64,1,0,'freqlim','0.1'),
(template_model_id,65,1,0,'freqlim','0.1'),
(template_model_id,66,1,0,'freqlim','0.1'),
(template_model_id,67,1,0,'freqlim','0.1'),
(template_model_id,68,1,0,'freqlim','0.1'),
(template_model_id,69,1,0,'freqlim','0.1'),
(template_model_id,70,1,0,'freqlim','0.1'),
(template_model_id,71,1,0,'freqlim','0.1'),
(template_model_id,72,1,0,'freqlim','0.1'),
(template_model_id,73,1,0,'freqlim','0.1'),
(template_model_id,74,1,0,'freqlim','0.1'),
(template_model_id,75,1,0,'freqlim','0.1'),
(template_model_id,76,1,0,'freqlim','0.3'),
(template_model_id,77,1,0,'freqlim','0.3'),
(template_model_id,78,1,0,'freqlim','0.3'),
(template_model_id,79,1,0,'freqlim','0.3'),
(template_model_id,80,1,0,'freqlim','0.1'),
(template_model_id,81,1,0,'freqlim','0.1'),
(template_model_id,82,1,0,'freqlim','0.1'),
(template_model_id,83,1,0,'freqlim','0.1'),
(template_model_id,84,1,0,'freqlim','0.1'),
(template_model_id,85,1,0,'freqlim','0.1'),
(template_model_id,86,1,0,'freqlim','0.1'),
(template_model_id,87,1,0,'freqlim','0.1'),
(template_model_id,88,1,0,'freqlim','0.3'),
(template_model_id,89,1,0,'freqlim','0.3'),
(template_model_id,90,1,0,'freqlim','0.3'),
(template_model_id,91,1,0,'freqlim','0.3'),
(template_model_id,92,1,0,'freqlim','0.1'),
(template_model_id,93,1,0,'freqlim','0.1'),
(template_model_id,94,1,0,'freqlim','0.1'),
(template_model_id,95,1,0,'freqlim','0.1'),
(template_model_id,96,1,0,'freqlim','0.1'),
(template_model_id,97,1,0,'freqlim','0.1'),
(template_model_id,98,1,0,'freqlim','0.1'),
(template_model_id,99,1,0,'freqlim','0.1'),
(template_model_id,100,1,0,'freqlim','0.1'),
(template_model_id,101,1,0,'freqlim','0.1'),
(template_model_id,102,1,0,'freqlim','0.1'),
(template_model_id,103,1,0,'freqlim','0.1'),
(template_model_id,104,1,0,'freqlim','0.1'),
(template_model_id,105,1,0,'freqlim','0.1'),
(template_model_id,106,1,0,'freqlim','0.1'),
(template_model_id,107,1,0,'freqlim','0.1'),
(template_model_id,108,1,0,'freqlim','0.1'),
(template_model_id,109,1,0,'freqlim','0.3'),
(template_model_id,110,1,0,'freqlim','0.3'),
(template_model_id,111,1,0,'freqlim','0.3'),
(template_model_id,112,1,0,'freqlim','0.3'),
(template_model_id,113,1,0,'freqlim','0.3'),
(template_model_id,114,1,0,'freqlim','0.3'),
(template_model_id,115,1,0,'freqlim','0.3'),
(template_model_id,116,1,0,'freqlim','0.1'),
(template_model_id,117,1,0,'freqlim','0.1'),
(template_model_id,118,1,0,'freqlim','0.1'),
(template_model_id,119,1,0,'freqlim','0.1'),
(template_model_id,120,1,0,'freqlim','0.1'),
(template_model_id,121,1,0,'freqlim','0.1'),
(template_model_id,122,1,0,'freqlim','0.1'),
(template_model_id,123,1,0,'freqlim','0.1'),
(template_model_id,124,1,0,'freqlim','0.1'),
(template_model_id,125,1,0,'freqlim','0.1'),
(template_model_id,126,1,0,'freqlim','0.1'),
(template_model_id,127,1,0,'freqlim','0.1'),
(template_model_id,128,1,0,'freqlim','0.1'),
(template_model_id,129,1,0,'freqlim','0.1'),
(template_model_id,130,1,0,'freqlim','0.1'),
(template_model_id,131,1,0,'freqlim','0.1'),
(template_model_id,132,1,0,'freqlim','0.1'),
(template_model_id,133,1,0,'freqlim','0.1'),
(template_model_id,134,1,0,'freqlim','0.1'),
(template_model_id,135,1,0,'freqlim','0.1'),
(template_model_id,136,1,0,'freqlim','0.1'),
(template_model_id,137,1,0,'freqlim','0.1'),
(template_model_id,138,1,0,'freqlim','0.1'),
(template_model_id,139,1,0,'freqlim','0.1'),
(template_model_id,140,1,0,'freqlim','0.1'),
(template_model_id,141,1,0,'freqlim','0.3'),
(template_model_id,142,1,0,'freqlim','0.3'),
(template_model_id,143,1,0,'freqlim','0.3'),
(template_model_id,144,1,0,'freqlim','0.3'),
(template_model_id,145,1,0,'freqlim','0.3'),
(template_model_id,146,1,0,'freqlim','0.3'),
(template_model_id,147,1,0,'freqlim','0.3'),
(template_model_id,148,1,0,'freqlim','0.3'),
(template_model_id,149,1,0,'freqlim','0.3'),
(template_model_id,150,1,0,'freqlim','0.3'),
(template_model_id,151,1,0,'freqlim','0.3'),
(template_model_id,152,1,0,'freqlim','0.3'),
(template_model_id,153,1,0,'freqlim','0.3'),
(template_model_id,154,1,0,'freqlim','0.3'),
(template_model_id,155,1,0,'freqlim','0.3'),
(template_model_id,156,1,0,'freqlim','0.3'),
(template_model_id,157,1,0,'freqlim','0.3'),
(template_model_id,158,1,0,'freqlim','0.3'),
(template_model_id,159,1,0,'freqlim','0.3'),
(template_model_id,160,1,0,'freqlim','0.1'),
(template_model_id,161,1,0,'freqlim','0.1'),
(template_model_id,162,1,0,'freqlim','0.1'),
(template_model_id,163,1,0,'freqlim','0.1'),
(template_model_id,164,1,0,'freqlim','0.1'),
(template_model_id,165,1,0,'freqlim','0.1'),
(template_model_id,166,1,0,'freqlim','0.1'),
(template_model_id,167,1,0,'freqlim','0.1'),
(template_model_id,168,1,0,'freqlim','0.1'),
(template_model_id,169,1,0,'freqlim','0.1'),
(template_model_id,170,1,0,'freqlim','0.3'),
(template_model_id,171,1,0,'freqlim','0.3'),
(template_model_id,172,1,0,'freqlim','0.1'),
(template_model_id,173,1,0,'freqlim','0.1'),
(template_model_id,174,1,0,'freqlim','0.3'),
(template_model_id,175,1,0,'freqlim','0.3'),
(template_model_id,176,1,0,'freqlim','0.1'),
(template_model_id,177,1,0,'freqlim','0.1'),
(template_model_id,178,1,0,'freqlim','0.1'),
(template_model_id,179,1,0,'freqlim','0.1'),
(template_model_id,180,1,0,'freqlim','0.1'),
(template_model_id,181,1,0,'freqlim','0.1'),
(template_model_id,182,1,0,'freqlim','0.1'),
(template_model_id,183,1,0,'freqlim','0.1'),
(template_model_id,184,1,0,'freqlim','0.1'),
(template_model_id,185,1,0,'freqlim','0.1'),
(template_model_id,186,1,0,'freqlim','0.1'),
(template_model_id,187,1,0,'freqlim','0.1'),
(template_model_id,188,1,0,'freqlim','0.1'),
(template_model_id,189,1,0,'freqlim','0.1'),
(template_model_id,190,1,0,'freqlim','0.1'),
(template_model_id,191,1,0,'freqlim','0.1'),
(template_model_id,192,1,0,'freqlim','0.1'),
(template_model_id,193,1,0,'freqlim','0.1'),
(template_model_id,194,1,0,'freqlim','0.1'),
(template_model_id,195,1,0,'freqlim','0.1'),
(template_model_id,196,1,0,'freqlim','0.1'),
(template_model_id,197,1,0,'freqlim','0.1'),
(template_model_id,198,1,0,'freqlim','0.1'),
(template_model_id,199,1,0,'freqlim','0.1'),
(template_model_id,200,1,0,'freqlim','0.1'),
(template_model_id,201,1,0,'freqlim','0.1'),
(template_model_id,202,1,0,'freqlim','0.1'),
(template_model_id,1,0,0,'vargroup','Social network'),
(template_model_id,2,0,0,'vargroup','Voice'),
(template_model_id,3,0,0,'vargroup','Social network'),
(template_model_id,4,0,0,'vargroup','Social network'),
(template_model_id,5,0,0,'vargroup','SMS'),
(template_model_id,6,0,0,'vargroup','SMS'),
(template_model_id,7,0,0,'vargroup','SMS'),
(template_model_id,8,0,0,'vargroup','SMS'),
(template_model_id,9,0,0,'vargroup','Voice'),
(template_model_id,10,0,0,'vargroup','Voice'),
(template_model_id,11,0,0,'vargroup','Voice'),
(template_model_id,12,0,0,'vargroup','Voice'),
(template_model_id,13,0,0,'vargroup','Voice'),
(template_model_id,14,0,0,'vargroup','Voice'),
(template_model_id,15,0,0,'vargroup','Voice'),
(template_model_id,16,0,0,'vargroup','Voice'),
(template_model_id,17,0,0,'vargroup','Voice'),
(template_model_id,18,0,0,'vargroup','Voice'),
(template_model_id,19,0,0,'vargroup','Mobility'),
(template_model_id,20,0,0,'vargroup','Mobility'),
(template_model_id,21,0,0,'vargroup','Mobility'),
(template_model_id,22,0,0,'vargroup','Mobility'),
(template_model_id,23,0,0,'vargroup','Other call data'),
(template_model_id,24,0,0,'vargroup','Other call data'),
(template_model_id,25,0,0,'vargroup','Other call data'),
(template_model_id,26,0,0,'vargroup','Other call data'),
(template_model_id,27,0,0,'vargroup','Voice'),
(template_model_id,28,0,0,'vargroup','Voice'),
(template_model_id,29,0,0,'vargroup','Voice'),
(template_model_id,30,0,0,'vargroup','Voice'),
(template_model_id,31,0,0,'vargroup','Voice'),
(template_model_id,32,0,0,'vargroup','Voice'),
(template_model_id,33,0,0,'vargroup','Voice'),
(template_model_id,34,0,0,'vargroup','Voice'),
(template_model_id,35,0,0,'vargroup','Voice'),
(template_model_id,36,0,0,'vargroup','Voice'),
(template_model_id,37,0,0,'vargroup','Voice'),
(template_model_id,38,0,0,'vargroup','Voice'),
(template_model_id,39,0,0,'vargroup','Voice'),
(template_model_id,40,0,0,'vargroup','Voice'),
(template_model_id,41,0,0,'vargroup','Voice'),
(template_model_id,42,0,0,'vargroup','Voice'),
(template_model_id,43,0,0,'vargroup','Voice'),
(template_model_id,44,0,0,'vargroup','Voice'),
(template_model_id,45,0,0,'vargroup','Voice'),
(template_model_id,46,0,0,'vargroup','Voice'),
(template_model_id,47,0,0,'vargroup','SMS'),
(template_model_id,48,0,0,'vargroup','SMS'),
(template_model_id,49,0,0,'vargroup','SMS'),
(template_model_id,50,0,0,'vargroup','SMS'),
(template_model_id,51,0,0,'vargroup','SMS'),
(template_model_id,52,0,0,'vargroup','SMS'),
(template_model_id,53,0,0,'vargroup','SMS'),
(template_model_id,54,0,0,'vargroup','SMS'),
(template_model_id,55,0,0,'vargroup','SMS'),
(template_model_id,56,0,0,'vargroup','SMS'),
(template_model_id,57,0,0,'vargroup','SMS'),
(template_model_id,58,0,0,'vargroup','SMS'),
(template_model_id,59,0,0,'vargroup','Data'),
(template_model_id,60,0,0,'vargroup','Data'),
(template_model_id,61,0,0,'vargroup','Data'),
(template_model_id,62,0,0,'vargroup','Data'),
(template_model_id,63,0,0,'vargroup','Data'),
(template_model_id,64,0,0,'vargroup','Data'),
(template_model_id,65,0,0,'vargroup','Data'),
(template_model_id,66,0,0,'vargroup','Data'),
(template_model_id,67,0,0,'vargroup','Data'),
(template_model_id,68,0,0,'vargroup','Data'),
(template_model_id,69,0,0,'vargroup','Data'),
(template_model_id,70,0,0,'vargroup','Data'),
(template_model_id,71,0,0,'vargroup','Other call data'),
(template_model_id,72,0,0,'vargroup','Mobility'),
(template_model_id,73,0,0,'vargroup','Mobility'),
(template_model_id,74,0,0,'vargroup','Mobility'),
(template_model_id,75,0,0,'vargroup','Mobility'),
(template_model_id,76,0,0,'vargroup','Voice'),
(template_model_id,77,0,0,'vargroup','Voice'),
(template_model_id,78,0,0,'vargroup','Voice'),
(template_model_id,79,0,0,'vargroup','Voice'),
(template_model_id,80,0,0,'vargroup','Voice'),
(template_model_id,81,0,0,'vargroup','Voice'),
(template_model_id,82,0,0,'vargroup','Voice'),
(template_model_id,83,0,0,'vargroup','Voice'),
(template_model_id,84,0,0,'vargroup','Voice'),
(template_model_id,85,0,0,'vargroup','Voice'),
(template_model_id,86,0,0,'vargroup','Voice'),
(template_model_id,87,0,0,'vargroup','Voice'),
(template_model_id,88,0,0,'vargroup','SMS'),
(template_model_id,89,0,0,'vargroup','SMS'),
(template_model_id,90,0,0,'vargroup','SMS'),
(template_model_id,91,0,0,'vargroup','SMS'),
(template_model_id,92,0,0,'vargroup','SMS'),
(template_model_id,93,0,0,'vargroup','SMS'),
(template_model_id,94,0,0,'vargroup','SMS'),
(template_model_id,95,0,0,'vargroup','SMS'),
(template_model_id,96,0,0,'vargroup','Other call data'),
(template_model_id,97,0,0,'vargroup','Topup'),
(template_model_id,98,0,0,'vargroup','Topup'),
(template_model_id,99,0,0,'vargroup','Topup'),
(template_model_id,100,0,0,'vargroup','Topup'),
(template_model_id,101,0,0,'vargroup','Topup'),
(template_model_id,102,0,0,'vargroup','Topup'),
(template_model_id,103,0,0,'vargroup','Topup'),
(template_model_id,104,0,0,'vargroup','Topup'),
(template_model_id,105,0,0,'vargroup','Topup'),
(template_model_id,106,0,0,'vargroup','Topup'),
(template_model_id,107,0,0,'vargroup','Topup'),
(template_model_id,108,0,0,'vargroup','Topup'),
(template_model_id,109,0,0,'vargroup','Topup'),
(template_model_id,110,0,0,'vargroup','Topup'),
(template_model_id,111,0,0,'vargroup','Topup'),
(template_model_id,112,0,0,'vargroup','Topup'),
(template_model_id,113,0,0,'vargroup','Topup'),
(template_model_id,114,0,0,'vargroup','Topup'),
(template_model_id,115,0,0,'vargroup','Topup'),
(template_model_id,116,0,0,'vargroup','Topup'),
(template_model_id,117,0,0,'vargroup','Topup'),
(template_model_id,118,0,0,'vargroup','Topup'),
(template_model_id,119,0,0,'vargroup','Topup'),
(template_model_id,120,0,0,'vargroup','Topup'),
(template_model_id,121,0,0,'vargroup','Topup'),
(template_model_id,122,0,0,'vargroup','Topup'),
(template_model_id,123,0,0,'vargroup','Topup'),
(template_model_id,124,0,0,'vargroup','Topup'),
(template_model_id,125,0,0,'vargroup','Topup'),
(template_model_id,126,0,0,'vargroup','Topup'),
(template_model_id,127,0,0,'vargroup','Topup'),
(template_model_id,128,0,0,'vargroup','Topup'),
(template_model_id,129,0,0,'vargroup','Topup'),
(template_model_id,130,0,0,'vargroup','Topup'),
(template_model_id,131,0,0,'vargroup','Topup'),
(template_model_id,132,0,0,'vargroup','Topup'),
(template_model_id,133,0,0,'vargroup','Topup'),
(template_model_id,134,0,0,'vargroup','Topup'),
(template_model_id,135,0,0,'vargroup','Topup'),
(template_model_id,136,0,0,'vargroup','Topup'),
(template_model_id,137,0,0,'vargroup','Topup'),
(template_model_id,138,0,0,'vargroup','Topup'),
(template_model_id,139,0,0,'vargroup','Topup'),
(template_model_id,140,0,0,'vargroup','Topup'),
(template_model_id,141,0,0,'vargroup','Topup'),
(template_model_id,142,0,0,'vargroup','Topup'),
(template_model_id,143,0,0,'vargroup','Topup'),
(template_model_id,144,0,0,'vargroup','Topup'),
(template_model_id,145,0,0,'vargroup','Topup'),
(template_model_id,146,0,0,'vargroup','Topup'),
(template_model_id,147,0,0,'vargroup','Topup'),
(template_model_id,148,0,0,'vargroup','Topup'),
(template_model_id,149,0,0,'vargroup','Topup'),
(template_model_id,150,0,0,'vargroup','Topup'),
(template_model_id,151,0,0,'vargroup','Topup'),
(template_model_id,152,0,0,'vargroup','Topup'),
(template_model_id,153,0,0,'vargroup','Topup'),
(template_model_id,154,0,0,'vargroup','Topup'),
(template_model_id,155,0,0,'vargroup','Topup'),
(template_model_id,156,0,0,'vargroup','Topup'),
(template_model_id,157,0,0,'vargroup','Social network'),
(template_model_id,158,0,0,'vargroup','Social network'),
(template_model_id,159,0,0,'vargroup','Social network'),
(template_model_id,160,0,0,'vargroup','Social network'),
(template_model_id,161,0,0,'vargroup','Demographics'),
(template_model_id,162,0,0,'vargroup','Social network'),
(template_model_id,163,0,0,'vargroup','Social network'),
(template_model_id,164,0,0,'vargroup','Social network'),
(template_model_id,165,0,0,'vargroup','Social network'),
(template_model_id,166,0,0,'vargroup','Other'),
(template_model_id,167,0,0,'vargroup','Subscription details'),
(template_model_id,168,0,0,'vargroup','Subscription details'),
(template_model_id,169,0,0,'vargroup','Handset'),
(template_model_id,170,0,0,'vargroup','Social network'),
(template_model_id,171,0,0,'vargroup','Social network'),
(template_model_id,172,0,0,'vargroup','Social network'),
(template_model_id,173,0,0,'vargroup','Social network'),
(template_model_id,174,0,0,'vargroup','Social network'),
(template_model_id,175,0,0,'vargroup','Social network'),
(template_model_id,176,0,0,'vargroup','Social network'),
(template_model_id,177,0,0,'vargroup','Social network'),
(template_model_id,178,0,0,'vargroup','Subscription details'),
(template_model_id,179,0,0,'vargroup','Other'),
(template_model_id,180,0,0,'vargroup','Social network'),
(template_model_id,181,0,0,'vargroup','Handset'),
(template_model_id,182,0,0,'vargroup','Handset'),
(template_model_id,183,0,0,'vargroup','Handset'),
(template_model_id,184,0,0,'vargroup','Handset'),
(template_model_id,185,0,0,'vargroup','Handset'),
(template_model_id,186,0,0,'vargroup','cell_events'),
(template_model_id,187,0,0,'vargroup','cell_events'),
(template_model_id,188,0,0,'vargroup','cell_events'),
(template_model_id,189,0,0,'vargroup','cell_events'),
(template_model_id,190,0,0,'vargroup','cell_events'),
(template_model_id,191,0,0,'vargroup','cell_events'),
(template_model_id,192,0,0,'vargroup','cell_events'),
(template_model_id,193,0,0,'vargroup','cell_events'),
(template_model_id,194,0,0,'vargroup','cell_events'),
(template_model_id,195,0,0,'vargroup','cell_events'),
(template_model_id,196,0,0,'vargroup','geolocation'),
(template_model_id,197,0,0,'vargroup','geolocation'),
(template_model_id,198,0,0,'vargroup','geolocation'),
(template_model_id,199,0,0,'vargroup','geolocation'),
(template_model_id,200,0,0,'vargroup','geolocation'),
(template_model_id,201,0,0,'vargroup','geolocation'),
(template_model_id,202,0,0,'vargroup','geolocation'),
(template_model_id,1,1,0,'indname','isna'),
(template_model_id,1,1,0,'indcond','NULL'),
(template_model_id,1,2,0,'indname','iszero'),
(template_model_id,1,2,0,'indcond','0'),
(template_model_id,1,3,0,'indname','isneg'),
(template_model_id,1,3,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,2,1,0,'indname','isna'),
(template_model_id,2,1,0,'indcond','NULL'),
(template_model_id,2,2,0,'indname','isneg'),
(template_model_id,2,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,3,1,0,'indname','isna'),
(template_model_id,3,1,0,'indcond','NULL'),
(template_model_id,3,2,0,'indname','isneg'),
(template_model_id,3,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,4,1,0,'indname','isna'),
(template_model_id,4,1,0,'indcond','NULL'),
(template_model_id,4,2,0,'indname','isneg'),
(template_model_id,4,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,5,1,0,'indname','isna'),
(template_model_id,5,1,0,'indcond','NULL'),
(template_model_id,5,2,0,'indname','isneg'),
(template_model_id,5,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,6,1,0,'indname','isna'),
(template_model_id,6,1,0,'indcond','NULL'),
(template_model_id,6,2,0,'indname','isneg'),
(template_model_id,6,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,7,1,0,'indname','isna'),
(template_model_id,7,1,0,'indcond','NULL'),
(template_model_id,7,2,0,'indname','isneg'),
(template_model_id,7,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,8,1,0,'indname','isna'),
(template_model_id,8,1,0,'indcond','NULL'),
(template_model_id,8,2,0,'indname','isneg'),
(template_model_id,8,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,9,1,0,'indname','isna'),
(template_model_id,9,1,0,'indcond','NULL'),
(template_model_id,9,2,0,'indname','isneg'),
(template_model_id,9,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,10,1,0,'indname','isna'),
(template_model_id,10,1,0,'indcond','NULL'),
(template_model_id,10,2,0,'indname','isneg'),
(template_model_id,10,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,11,1,0,'indname','isna'),
(template_model_id,11,1,0,'indcond','NULL'),
(template_model_id,11,2,0,'indname','isneg'),
(template_model_id,11,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,12,1,0,'indname','isna'),
(template_model_id,12,1,0,'indcond','NULL'),
(template_model_id,12,2,0,'indname','isneg'),
(template_model_id,12,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,13,1,0,'indname','isna'),
(template_model_id,13,1,0,'indcond','NULL'),
(template_model_id,13,2,0,'indname','isneg'),
(template_model_id,13,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,14,1,0,'indname','isna'),
(template_model_id,14,1,0,'indcond','NULL'),
(template_model_id,14,2,0,'indname','isneg'),
(template_model_id,14,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,15,1,0,'indname','isna'),
(template_model_id,15,1,0,'indcond','NULL'),
(template_model_id,15,2,0,'indname','isneg'),
(template_model_id,15,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,16,1,0,'indname','isna'),
(template_model_id,16,1,0,'indcond','NULL'),
(template_model_id,16,2,0,'indname','isneg'),
(template_model_id,16,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,17,1,0,'indname','isna'),
(template_model_id,17,1,0,'indcond','NULL'),
(template_model_id,17,2,0,'indname','isneg'),
(template_model_id,17,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,18,1,0,'indname','isna'),
(template_model_id,18,1,0,'indcond','NULL'),
(template_model_id,18,2,0,'indname','isneg'),
(template_model_id,18,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,19,1,0,'indname','isna'),
(template_model_id,19,1,0,'indcond','NULL'),
(template_model_id,19,2,0,'indname','isneg'),
(template_model_id,19,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,20,1,0,'indname','isna'),
(template_model_id,20,1,0,'indcond','NULL'),
(template_model_id,20,2,0,'indname','isneg'),
(template_model_id,20,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,21,1,0,'indname','isna'),
(template_model_id,21,1,0,'indcond','NULL'),
(template_model_id,21,2,0,'indname','isneg'),
(template_model_id,21,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,22,1,0,'indname','isna'),
(template_model_id,22,1,0,'indcond','NULL'),
(template_model_id,22,2,0,'indname','isneg'),
(template_model_id,22,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,23,1,0,'indname','isna'),
(template_model_id,23,1,0,'indcond','NULL'),
(template_model_id,23,2,0,'indname','isneg'),
(template_model_id,23,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,24,1,0,'indname','isna'),
(template_model_id,24,1,0,'indcond','NULL'),
(template_model_id,24,2,0,'indname','isneg'),
(template_model_id,24,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,25,1,0,'indname','isna'),
(template_model_id,25,1,0,'indcond','NULL'),
(template_model_id,25,2,0,'indname','isneg'),
(template_model_id,25,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,26,1,0,'indname','isna'),
(template_model_id,26,1,0,'indcond','NULL'),
(template_model_id,26,2,0,'indname','isneg'),
(template_model_id,26,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,27,1,0,'indname','isna'),
(template_model_id,27,1,0,'indcond','NULL'),
(template_model_id,27,2,0,'indname','isneg'),
(template_model_id,27,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,28,1,0,'indname','isna'),
(template_model_id,28,1,0,'indcond','NULL'),
(template_model_id,28,2,0,'indname','isneg'),
(template_model_id,28,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,29,1,0,'indname','isna'),
(template_model_id,29,1,0,'indcond','NULL'),
(template_model_id,29,2,0,'indname','isneg'),
(template_model_id,29,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,30,1,0,'indname','isna'),
(template_model_id,30,1,0,'indcond','NULL'),
(template_model_id,30,2,0,'indname','isneg'),
(template_model_id,30,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,31,1,0,'indname','isna'),
(template_model_id,31,1,0,'indcond','NULL'),
(template_model_id,31,2,0,'indname','isneg'),
(template_model_id,31,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,32,1,0,'indname','isna'),
(template_model_id,32,1,0,'indcond','NULL'),
(template_model_id,32,2,0,'indname','isneg'),
(template_model_id,32,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,33,1,0,'indname','isna'),
(template_model_id,33,1,0,'indcond','NULL'),
(template_model_id,33,2,0,'indname','isneg'),
(template_model_id,33,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,34,1,0,'indname','isna'),
(template_model_id,34,1,0,'indcond','NULL'),
(template_model_id,34,2,0,'indname','isneg'),
(template_model_id,34,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,35,1,0,'indname','isna'),
(template_model_id,35,1,0,'indcond','NULL'),
(template_model_id,35,2,0,'indname','isneg'),
(template_model_id,35,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,36,1,0,'indname','isna'),
(template_model_id,36,1,0,'indcond','NULL'),
(template_model_id,36,2,0,'indname','isneg'),
(template_model_id,36,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,37,1,0,'indname','isna'),
(template_model_id,37,1,0,'indcond','NULL'),
(template_model_id,37,2,0,'indname','isneg'),
(template_model_id,37,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,38,1,0,'indname','isna'),
(template_model_id,38,1,0,'indcond','NULL'),
(template_model_id,38,2,0,'indname','isneg'),
(template_model_id,38,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,39,1,0,'indname','isna'),
(template_model_id,39,1,0,'indcond','NULL'),
(template_model_id,39,2,0,'indname','isneg'),
(template_model_id,39,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,40,1,0,'indname','isna'),
(template_model_id,40,1,0,'indcond','NULL'),
(template_model_id,40,2,0,'indname','isneg'),
(template_model_id,40,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,41,1,0,'indname','isna'),
(template_model_id,41,1,0,'indcond','NULL'),
(template_model_id,41,2,0,'indname','isneg'),
(template_model_id,41,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,42,1,0,'indname','isna'),
(template_model_id,42,1,0,'indcond','NULL'),
(template_model_id,42,2,0,'indname','isneg'),
(template_model_id,42,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,43,1,0,'indname','isna'),
(template_model_id,43,1,0,'indcond','NULL'),
(template_model_id,43,2,0,'indname','isneg'),
(template_model_id,43,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,44,1,0,'indname','isna'),
(template_model_id,44,1,0,'indcond','NULL'),
(template_model_id,44,2,0,'indname','isneg'),
(template_model_id,44,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,45,1,0,'indname','isna'),
(template_model_id,45,1,0,'indcond','NULL'),
(template_model_id,45,2,0,'indname','isneg'),
(template_model_id,45,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,46,1,0,'indname','isna'),
(template_model_id,46,1,0,'indcond','NULL'),
(template_model_id,46,2,0,'indname','isneg'),
(template_model_id,46,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,47,1,0,'indname','isna'),
(template_model_id,47,1,0,'indcond','NULL'),
(template_model_id,47,2,0,'indname','isneg'),
(template_model_id,47,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,48,1,0,'indname','isna'),
(template_model_id,48,1,0,'indcond','NULL'),
(template_model_id,48,2,0,'indname','isneg'),
(template_model_id,48,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,49,1,0,'indname','isna'),
(template_model_id,49,1,0,'indcond','NULL'),
(template_model_id,49,2,0,'indname','isneg'),
(template_model_id,49,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,50,1,0,'indname','isna'),
(template_model_id,50,1,0,'indcond','NULL'),
(template_model_id,50,2,0,'indname','isneg'),
(template_model_id,50,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,51,1,0,'indname','isna'),
(template_model_id,51,1,0,'indcond','NULL'),
(template_model_id,51,2,0,'indname','isneg'),
(template_model_id,51,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,52,1,0,'indname','isna'),
(template_model_id,52,1,0,'indcond','NULL'),
(template_model_id,52,2,0,'indname','isneg'),
(template_model_id,52,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,53,1,0,'indname','isna'),
(template_model_id,53,1,0,'indcond','NULL'),
(template_model_id,53,2,0,'indname','isneg'),
(template_model_id,53,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,54,1,0,'indname','isna'),
(template_model_id,54,1,0,'indcond','NULL'),
(template_model_id,54,2,0,'indname','isneg'),
(template_model_id,54,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,55,1,0,'indname','isna'),
(template_model_id,55,1,0,'indcond','NULL'),
(template_model_id,55,2,0,'indname','isneg'),
(template_model_id,55,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,56,1,0,'indname','isna'),
(template_model_id,56,1,0,'indcond','NULL'),
(template_model_id,56,2,0,'indname','isneg'),
(template_model_id,56,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,57,1,0,'indname','isna'),
(template_model_id,57,1,0,'indcond','NULL'),
(template_model_id,57,2,0,'indname','isneg'),
(template_model_id,57,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,58,1,0,'indname','isna'),
(template_model_id,58,1,0,'indcond','NULL'),
(template_model_id,58,2,0,'indname','isneg'),
(template_model_id,58,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,59,1,0,'indname','isna'),
(template_model_id,59,1,0,'indcond','NULL'),
(template_model_id,59,2,0,'indname','isneg'),
(template_model_id,59,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,60,1,0,'indname','isna'),
(template_model_id,60,1,0,'indcond','NULL'),
(template_model_id,60,2,0,'indname','isneg'),
(template_model_id,60,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,61,1,0,'indname','isna'),
(template_model_id,61,1,0,'indcond','NULL'),
(template_model_id,61,2,0,'indname','isneg'),
(template_model_id,61,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,62,1,0,'indname','isna'),
(template_model_id,62,1,0,'indcond','NULL'),
(template_model_id,62,2,0,'indname','isneg'),
(template_model_id,62,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,63,1,0,'indname','isna'),
(template_model_id,63,1,0,'indcond','NULL'),
(template_model_id,63,2,0,'indname','isneg'),
(template_model_id,63,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,64,1,0,'indname','isna'),
(template_model_id,64,1,0,'indcond','NULL'),
(template_model_id,64,2,0,'indname','isneg'),
(template_model_id,64,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,65,1,0,'indname','isna'),
(template_model_id,65,1,0,'indcond','NULL'),
(template_model_id,65,2,0,'indname','isneg'),
(template_model_id,65,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,66,1,0,'indname','isna'),
(template_model_id,66,1,0,'indcond','NULL'),
(template_model_id,66,2,0,'indname','isneg'),
(template_model_id,66,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,67,1,0,'indname','isna'),
(template_model_id,67,1,0,'indcond','NULL'),
(template_model_id,67,2,0,'indname','isneg'),
(template_model_id,67,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,68,1,0,'indname','isna'),
(template_model_id,68,1,0,'indcond','NULL'),
(template_model_id,68,2,0,'indname','isneg'),
(template_model_id,68,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,69,1,0,'indname','isna'),
(template_model_id,69,1,0,'indcond','NULL'),
(template_model_id,69,2,0,'indname','isneg'),
(template_model_id,69,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,70,1,0,'indname','isna'),
(template_model_id,70,1,0,'indcond','NULL'),
(template_model_id,70,2,0,'indname','isneg'),
(template_model_id,70,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,71,1,0,'indname','isna'),
(template_model_id,71,1,0,'indcond','NULL'),
(template_model_id,71,2,0,'indname','isneg'),
(template_model_id,71,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,72,1,0,'indname','isna'),
(template_model_id,72,1,0,'indcond','NULL'),
(template_model_id,72,2,0,'indname','isneg'),
(template_model_id,72,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,73,1,0,'indname','isna'),
(template_model_id,73,1,0,'indcond','NULL'),
(template_model_id,73,2,0,'indname','isneg'),
(template_model_id,73,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,74,1,0,'indname','isna'),
(template_model_id,74,1,0,'indcond','NULL'),
(template_model_id,74,2,0,'indname','isneg'),
(template_model_id,74,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,75,1,0,'indname','isna'),
(template_model_id,75,1,0,'indcond','NULL'),
(template_model_id,75,2,0,'indname','isneg'),
(template_model_id,75,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,76,1,0,'indname','isna'),
(template_model_id,76,1,0,'indcond','NULL'),
(template_model_id,76,2,0,'indname','isneg'),
(template_model_id,76,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,77,1,0,'indname','isna'),
(template_model_id,77,1,0,'indcond','NULL'),
(template_model_id,77,2,0,'indname','isneg'),
(template_model_id,77,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,78,1,0,'indname','isna'),
(template_model_id,78,1,0,'indcond','NULL'),
(template_model_id,78,2,0,'indname','isneg'),
(template_model_id,78,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,79,1,0,'indname','isna'),
(template_model_id,79,1,0,'indcond','NULL'),
(template_model_id,79,2,0,'indname','isneg'),
(template_model_id,79,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,80,1,0,'indname','isna'),
(template_model_id,80,1,0,'indcond','NULL'),
(template_model_id,80,2,0,'indname','isneg'),
(template_model_id,80,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,81,1,0,'indname','isna'),
(template_model_id,81,1,0,'indcond','NULL'),
(template_model_id,81,2,0,'indname','isneg'),
(template_model_id,81,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,82,1,0,'indname','isna'),
(template_model_id,82,1,0,'indcond','NULL'),
(template_model_id,82,2,0,'indname','isneg'),
(template_model_id,82,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,83,1,0,'indname','isna'),
(template_model_id,83,1,0,'indcond','NULL'),
(template_model_id,83,2,0,'indname','isneg'),
(template_model_id,83,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,84,1,0,'indname','isna'),
(template_model_id,84,1,0,'indcond','NULL'),
(template_model_id,84,2,0,'indname','isneg'),
(template_model_id,84,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,85,1,0,'indname','isna'),
(template_model_id,85,1,0,'indcond','NULL'),
(template_model_id,85,2,0,'indname','isneg'),
(template_model_id,85,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,86,1,0,'indname','isna'),
(template_model_id,86,1,0,'indcond','NULL'),
(template_model_id,86,2,0,'indname','isneg'),
(template_model_id,86,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,87,1,0,'indname','isna'),
(template_model_id,87,1,0,'indcond','NULL'),
(template_model_id,87,2,0,'indname','isneg'),
(template_model_id,87,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,88,1,0,'indname','isna'),
(template_model_id,88,1,0,'indcond','NULL'),
(template_model_id,88,2,0,'indname','isneg'),
(template_model_id,88,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,89,1,0,'indname','isna'),
(template_model_id,89,1,0,'indcond','NULL'),
(template_model_id,89,2,0,'indname','isneg'),
(template_model_id,89,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,90,1,0,'indname','isna'),
(template_model_id,90,1,0,'indcond','NULL'),
(template_model_id,90,2,0,'indname','isneg'),
(template_model_id,90,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,91,1,0,'indname','isna'),
(template_model_id,91,1,0,'indcond','NULL'),
(template_model_id,91,2,0,'indname','isneg'),
(template_model_id,91,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,92,1,0,'indname','isna'),
(template_model_id,92,1,0,'indcond','NULL'),
(template_model_id,92,2,0,'indname','isneg'),
(template_model_id,92,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,93,1,0,'indname','isna'),
(template_model_id,93,1,0,'indcond','NULL'),
(template_model_id,93,2,0,'indname','isneg'),
(template_model_id,93,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,94,1,0,'indname','isna'),
(template_model_id,94,1,0,'indcond','NULL'),
(template_model_id,94,2,0,'indname','isneg'),
(template_model_id,94,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,95,1,0,'indname','isna'),
(template_model_id,95,1,0,'indcond','NULL'),
(template_model_id,95,2,0,'indname','isneg'),
(template_model_id,95,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,96,1,0,'indname','isna'),
(template_model_id,96,1,0,'indcond','NULL'),
(template_model_id,96,2,0,'indname','isneg'),
(template_model_id,96,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,97,1,0,'indname','isna'),
(template_model_id,97,1,0,'indcond','NULL'),
(template_model_id,97,2,0,'indname','isneg'),
(template_model_id,97,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,98,1,0,'indname','isna'),
(template_model_id,98,1,0,'indcond','NULL'),
(template_model_id,98,2,0,'indname','isneg'),
(template_model_id,98,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,99,1,0,'indname','isna'),
(template_model_id,99,1,0,'indcond','NULL'),
(template_model_id,99,2,0,'indname','isneg'),
(template_model_id,99,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,100,1,0,'indname','isna'),
(template_model_id,100,1,0,'indcond','NULL'),
(template_model_id,100,2,0,'indname','isneg'),
(template_model_id,100,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,101,1,0,'indname','isna'),
(template_model_id,101,1,0,'indcond','NULL'),
(template_model_id,101,2,0,'indname','isneg'),
(template_model_id,101,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,102,1,0,'indname','isna'),
(template_model_id,102,1,0,'indcond','NULL'),
(template_model_id,102,2,0,'indname','isneg'),
(template_model_id,102,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,103,1,0,'indname','isna'),
(template_model_id,103,1,0,'indcond','NULL'),
(template_model_id,103,2,0,'indname','isneg'),
(template_model_id,103,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,104,1,0,'indname','isna'),
(template_model_id,104,1,0,'indcond','NULL'),
(template_model_id,104,2,0,'indname','isneg'),
(template_model_id,104,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,105,1,0,'indname','isna'),
(template_model_id,105,1,0,'indcond','NULL'),
(template_model_id,105,2,0,'indname','isneg'),
(template_model_id,105,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,106,1,0,'indname','isna'),
(template_model_id,106,1,0,'indcond','NULL'),
(template_model_id,106,2,0,'indname','isneg'),
(template_model_id,106,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,107,1,0,'indname','isna'),
(template_model_id,107,1,0,'indcond','NULL'),
(template_model_id,107,2,0,'indname','isneg'),
(template_model_id,107,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,108,1,0,'indname','isna'),
(template_model_id,108,1,0,'indcond','NULL'),
(template_model_id,108,2,0,'indname','isneg'),
(template_model_id,108,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,109,1,0,'indname','isna'),
(template_model_id,109,1,0,'indcond','NULL'),
(template_model_id,109,2,0,'indname','isneg'),
(template_model_id,109,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,110,1,0,'indname','isna'),
(template_model_id,110,1,0,'indcond','NULL'),
(template_model_id,110,2,0,'indname','isneg'),
(template_model_id,110,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,111,1,0,'indname','isna'),
(template_model_id,111,1,0,'indcond','NULL'),
(template_model_id,111,2,0,'indname','isneg'),
(template_model_id,111,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,112,1,0,'indname','isna'),
(template_model_id,112,1,0,'indcond','NULL'),
(template_model_id,112,2,0,'indname','isneg'),
(template_model_id,112,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,113,1,0,'indname','isna'),
(template_model_id,113,1,0,'indcond','NULL'),
(template_model_id,113,2,0,'indname','isneg'),
(template_model_id,113,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,114,1,0,'indname','isna'),
(template_model_id,114,1,0,'indcond','NULL'),
(template_model_id,115,1,0,'indname','isna'),
(template_model_id,115,1,0,'indcond','NULL'),
(template_model_id,116,1,0,'indname','isna'),
(template_model_id,116,1,0,'indcond','NULL'),
(template_model_id,116,2,0,'indname','isneg'),
(template_model_id,116,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,117,1,0,'indname','isna'),
(template_model_id,117,1,0,'indcond','NULL'),
(template_model_id,117,2,0,'indname','isneg'),
(template_model_id,117,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,118,1,0,'indname','isna'),
(template_model_id,118,1,0,'indcond','NULL'),
(template_model_id,118,2,0,'indname','isneg'),
(template_model_id,118,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,119,1,0,'indname','isna'),
(template_model_id,119,1,0,'indcond','NULL'),
(template_model_id,119,2,0,'indname','isneg'),
(template_model_id,119,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,120,1,0,'indname','isna'),
(template_model_id,120,1,0,'indcond','NULL'),
(template_model_id,120,2,0,'indname','isneg'),
(template_model_id,120,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,121,1,0,'indname','isna'),
(template_model_id,121,1,0,'indcond','NULL'),
(template_model_id,121,2,0,'indname','isneg'),
(template_model_id,121,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,122,1,0,'indname','isna'),
(template_model_id,122,1,0,'indcond','NULL'),
(template_model_id,122,2,0,'indname','isneg'),
(template_model_id,122,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,123,1,0,'indname','isna'),
(template_model_id,123,1,0,'indcond','NULL'),
(template_model_id,123,2,0,'indname','isneg'),
(template_model_id,123,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,124,1,0,'indname','isna'),
(template_model_id,124,1,0,'indcond','NULL'),
(template_model_id,124,2,0,'indname','isneg'),
(template_model_id,124,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,125,1,0,'indname','isna'),
(template_model_id,125,1,0,'indcond','NULL'),
(template_model_id,125,2,0,'indname','isneg'),
(template_model_id,125,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,126,1,0,'indname','isna'),
(template_model_id,126,1,0,'indcond','NULL'),
(template_model_id,126,2,0,'indname','isneg'),
(template_model_id,126,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,127,1,0,'indname','isna'),
(template_model_id,127,1,0,'indcond','NULL'),
(template_model_id,127,2,0,'indname','isneg'),
(template_model_id,127,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,128,1,0,'indname','isna'),
(template_model_id,128,1,0,'indcond','NULL'),
(template_model_id,128,2,0,'indname','isneg'),
(template_model_id,128,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,129,1,0,'indname','isna'),
(template_model_id,129,1,0,'indcond','NULL'),
(template_model_id,129,2,0,'indname','isneg'),
(template_model_id,129,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,130,1,0,'indname','isna'),
(template_model_id,130,1,0,'indcond','NULL'),
(template_model_id,130,2,0,'indname','isneg'),
(template_model_id,130,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,131,1,0,'indname','isna'),
(template_model_id,131,1,0,'indcond','NULL'),
(template_model_id,131,2,0,'indname','isneg'),
(template_model_id,131,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,132,1,0,'indname','isna'),
(template_model_id,132,1,0,'indcond','NULL'),
(template_model_id,132,2,0,'indname','isneg'),
(template_model_id,132,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,133,1,0,'indname','isna'),
(template_model_id,133,1,0,'indcond','NULL'),
(template_model_id,133,2,0,'indname','isneg'),
(template_model_id,133,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,134,1,0,'indname','isna'),
(template_model_id,134,1,0,'indcond','NULL'),
(template_model_id,134,2,0,'indname','isneg'),
(template_model_id,134,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,135,1,0,'indname','isna'),
(template_model_id,135,1,0,'indcond','NULL'),
(template_model_id,135,2,0,'indname','isneg'),
(template_model_id,135,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,136,1,0,'indname','isna'),
(template_model_id,136,1,0,'indcond','NULL'),
(template_model_id,136,2,0,'indname','isneg'),
(template_model_id,136,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,137,1,0,'indname','isna'),
(template_model_id,137,1,0,'indcond','NULL'),
(template_model_id,137,2,0,'indname','isneg'),
(template_model_id,137,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,138,1,0,'indname','isna'),
(template_model_id,138,1,0,'indcond','NULL'),
(template_model_id,138,2,0,'indname','isneg'),
(template_model_id,138,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,139,1,0,'indname','isna'),
(template_model_id,139,1,0,'indcond','NULL'),
(template_model_id,139,2,0,'indname','isneg'),
(template_model_id,139,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,140,1,0,'indname','isna'),
(template_model_id,140,1,0,'indcond','NULL'),
(template_model_id,140,2,0,'indname','isneg'),
(template_model_id,140,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,141,1,0,'indname','isna'),
(template_model_id,141,1,0,'indcond','NULL'),
(template_model_id,141,2,0,'indname','isneg'),
(template_model_id,141,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,142,1,0,'indname','isna'),
(template_model_id,142,1,0,'indcond','NULL'),
(template_model_id,142,2,0,'indname','isneg'),
(template_model_id,142,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,143,1,0,'indname','isna'),
(template_model_id,143,1,0,'indcond','NULL'),
(template_model_id,143,2,0,'indname','isneg'),
(template_model_id,143,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,144,1,0,'indname','isna'),
(template_model_id,144,1,0,'indcond','NULL'),
(template_model_id,144,2,0,'indname','isneg'),
(template_model_id,144,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,145,1,0,'indname','isna'),
(template_model_id,145,1,0,'indcond','NULL'),
(template_model_id,145,2,0,'indname','isneg'),
(template_model_id,145,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,146,1,0,'indname','isna'),
(template_model_id,146,1,0,'indcond','NULL'),
(template_model_id,146,2,0,'indname','isneg'),
(template_model_id,146,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,147,1,0,'indname','isna'),
(template_model_id,147,1,0,'indcond','NULL'),
(template_model_id,147,2,0,'indname','isneg'),
(template_model_id,147,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,148,1,0,'indname','isna'),
(template_model_id,148,1,0,'indcond','NULL'),
(template_model_id,148,2,0,'indname','isneg'),
(template_model_id,148,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,149,1,0,'indname','isna'),
(template_model_id,149,1,0,'indcond','NULL'),
(template_model_id,149,2,0,'indname','isneg'),
(template_model_id,149,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,150,1,0,'indname','isna'),
(template_model_id,150,1,0,'indcond','NULL'),
(template_model_id,150,2,0,'indname','isneg'),
(template_model_id,150,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,151,1,0,'indname','isna'),
(template_model_id,151,1,0,'indcond','NULL'),
(template_model_id,151,2,0,'indname','isneg'),
(template_model_id,151,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,152,1,0,'indname','isna'),
(template_model_id,152,1,0,'indcond','NULL'),
(template_model_id,152,2,0,'indname','isneg'),
(template_model_id,152,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,153,1,0,'indname','isna'),
(template_model_id,153,1,0,'indcond','NULL'),
(template_model_id,153,2,0,'indname','isneg'),
(template_model_id,153,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,154,1,0,'indname','isna'),
(template_model_id,154,1,0,'indcond','NULL'),
(template_model_id,154,2,0,'indname','isneg'),
(template_model_id,154,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,155,1,0,'indname','isna'),
(template_model_id,155,1,0,'indcond','NULL'),
(template_model_id,155,2,0,'indname','isneg'),
(template_model_id,155,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,156,1,0,'indname','isna'),
(template_model_id,156,1,0,'indcond','NULL'),
(template_model_id,156,2,0,'indname','isneg'),
(template_model_id,156,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,157,1,0,'indname','isna'),
(template_model_id,157,1,0,'indcond','NULL'),
(template_model_id,157,2,0,'indname','isneg'),
(template_model_id,157,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,158,1,0,'indname','isna'),
(template_model_id,158,1,0,'indcond','NULL'),
(template_model_id,158,2,0,'indname','isneg'),
(template_model_id,158,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,159,1,0,'indname','isna'),
(template_model_id,159,1,0,'indcond','NULL'),
(template_model_id,159,2,0,'indname','isneg'),
(template_model_id,159,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,160,1,0,'indname','isna'),
(template_model_id,160,1,0,'indcond','NULL'),
(template_model_id,160,2,0,'indname','isneg'),
(template_model_id,160,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,161,1,0,'indname','naorzero'),
(template_model_id,161,1,0,'indcond','NULL or 0'),
(template_model_id,161,2,0,'indname','isneg'),
(template_model_id,161,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,162,1,0,'indname','isna'),
(template_model_id,162,1,0,'indcond','NULL'),
(template_model_id,162,2,0,'indname','isneg'),
(template_model_id,162,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,163,1,0,'indname','isna'),
(template_model_id,163,1,0,'indcond','NULL'),
(template_model_id,163,2,0,'indname','isneg'),
(template_model_id,163,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,164,1,0,'indname','isna'),
(template_model_id,164,1,0,'indcond','NULL'),
(template_model_id,164,2,0,'indname','isneg'),
(template_model_id,164,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,165,1,0,'indname','isna'),
(template_model_id,165,1,0,'indcond','NULL'),
(template_model_id,165,2,0,'indname','isneg'),
(template_model_id,165,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,166,1,0,'indname','isna'),
(template_model_id,166,1,0,'indcond','NULL'),
(template_model_id,166,2,0,'indname','isneg'),
(template_model_id,166,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,167,1,0,'indname','isna'),
(template_model_id,167,1,0,'indcond','NULL'),
(template_model_id,167,2,0,'indname','isneg'),
(template_model_id,167,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,167,3,0,'indname','onemonth'),
(template_model_id,167,3,0,'indcond','[0,1]'),
(template_model_id,167,4,0,'indname','twofourmonths'),
(template_model_id,167,4,0,'indcond','[1,4]'),
(template_model_id,167,5,0,'indname','fivetenmonths'),
(template_model_id,167,5,0,'indcond','[4,10]'),
(template_model_id,168,1,0,'indname','isna'),
(template_model_id,168,1,0,'indcond','NULL'),
(template_model_id,168,2,0,'indname','isneg'),
(template_model_id,168,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,169,1,0,'indname','isna'),
(template_model_id,169,1,0,'indcond','NULL'),
(template_model_id,169,2,0,'indname','isneg'),
(template_model_id,169,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,170,1,0,'indname','isna'),
(template_model_id,170,1,0,'indcond','NULL'),
(template_model_id,170,2,0,'indname','isneg'),
(template_model_id,170,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,171,1,0,'indname','isna'),
(template_model_id,171,1,0,'indcond','NULL'),
(template_model_id,171,2,0,'indname','isneg'),
(template_model_id,171,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,172,1,0,'indname','isna'),
(template_model_id,172,1,0,'indcond','NULL'),
(template_model_id,172,2,0,'indname','isneg'),
(template_model_id,172,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,173,1,0,'indname','isna'),
(template_model_id,173,1,0,'indcond','NULL'),
(template_model_id,173,2,0,'indname','isneg'),
(template_model_id,173,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,174,1,0,'indname','isna'),
(template_model_id,174,1,0,'indcond','NULL'),
(template_model_id,174,2,0,'indname','isneg'),
(template_model_id,174,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,175,1,0,'indname','isna'),
(template_model_id,175,1,0,'indcond','NULL'),
(template_model_id,175,2,0,'indname','isneg'),
(template_model_id,175,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,176,1,0,'indname','isna'),
(template_model_id,176,1,0,'indcond','NULL'),
(template_model_id,176,2,0,'indname','isneg'),
(template_model_id,176,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,177,1,0,'indname','isna'),
(template_model_id,177,1,0,'indcond','NULL'),
(template_model_id,177,2,0,'indname','isneg'),
(template_model_id,177,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,178,1,0,'indname','isna'),
(template_model_id,178,1,0,'indcond','NULL'),
(template_model_id,178,2,0,'indname','isneg'),
(template_model_id,178,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,179,1,0,'indname','isna'),
(template_model_id,179,1,0,'indcond','NULL'),
(template_model_id,179,2,0,'indname','isneg'),
(template_model_id,179,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,180,1,0,'indname','isna'),
(template_model_id,180,1,0,'indcond','NULL'),
(template_model_id,180,2,0,'indname','isneg'),
(template_model_id,180,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,181,1,0,'indname','isna'),
(template_model_id,181,1,0,'indcond','NULL'),
(template_model_id,181,2,0,'indname','isneg'),
(template_model_id,181,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,182,1,0,'indname','isna'),
(template_model_id,182,1,0,'indcond','NULL'),
(template_model_id,182,2,0,'indname','isneg'),
(template_model_id,182,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,183,1,0,'indname','isna'),
(template_model_id,183,1,0,'indcond','NULL'),
(template_model_id,183,2,0,'indname','isneg'),
(template_model_id,183,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,184,1,0,'indname','isna'),
(template_model_id,184,1,0,'indcond','NULL'),
(template_model_id,184,2,0,'indname','isneg'),
(template_model_id,184,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,185,1,0,'indname','isna'),
(template_model_id,185,1,0,'indcond','NULL'),
(template_model_id,185,2,0,'indname','isneg'),
(template_model_id,185,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,186,1,0,'indname','isna'),
(template_model_id,186,1,0,'indcond','NULL'),
(template_model_id,186,2,0,'indname','isneg'),
(template_model_id,186,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,187,1,0,'indname','isna'),
(template_model_id,187,1,0,'indcond','NULL'),
(template_model_id,187,2,0,'indname','isneg'),
(template_model_id,187,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,188,1,0,'indname','isna'),
(template_model_id,188,1,0,'indcond','NULL'),
(template_model_id,188,2,0,'indname','isneg'),
(template_model_id,188,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,189,1,0,'indname','isna'),
(template_model_id,189,1,0,'indcond','NULL'),
(template_model_id,189,2,0,'indname','isneg'),
(template_model_id,189,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,190,1,0,'indname','isna'),
(template_model_id,190,1,0,'indcond','NULL'),
(template_model_id,190,2,0,'indname','isneg'),
(template_model_id,190,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,191,1,0,'indname','isna'),
(template_model_id,191,1,0,'indcond','NULL'),
(template_model_id,191,2,0,'indname','isneg'),
(template_model_id,191,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,192,1,0,'indname','isna'),
(template_model_id,192,1,0,'indcond','NULL'),
(template_model_id,192,2,0,'indname','isneg'),
(template_model_id,192,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,193,1,0,'indname','isna'),
(template_model_id,193,1,0,'indcond','NULL'),
(template_model_id,193,2,0,'indname','isneg'),
(template_model_id,193,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,194,1,0,'indname','isna'),
(template_model_id,194,1,0,'indcond','NULL'),
(template_model_id,194,2,0,'indname','isneg'),
(template_model_id,194,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,195,1,0,'indname','isna'),
(template_model_id,195,1,0,'indcond','NULL'),
(template_model_id,195,2,0,'indname','isneg'),
(template_model_id,195,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,196,1,0,'indname','isna'),
(template_model_id,196,1,0,'indcond','NULL'),
(template_model_id,196,2,0,'indname','isneg'),
(template_model_id,196,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,197,1,0,'indname','isna'),
(template_model_id,197,1,0,'indcond','NULL'),
(template_model_id,197,2,0,'indname','isneg'),
(template_model_id,197,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,198,1,0,'indname','isna'),
(template_model_id,198,1,0,'indcond','NULL'),
(template_model_id,198,2,0,'indname','isneg'),
(template_model_id,198,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,199,1,0,'indname','isna'),
(template_model_id,199,1,0,'indcond','NULL'),
(template_model_id,199,2,0,'indname','isneg'),
(template_model_id,199,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,200,1,0,'indname','isna'),
(template_model_id,200,1,0,'indcond','NULL'),
(template_model_id,200,2,0,'indname','isneg'),
(template_model_id,200,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,201,1,0,'indname','isna'),
(template_model_id,201,1,0,'indcond','NULL'),
(template_model_id,201,2,0,'indname','isneg'),
(template_model_id,201,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,202,1,0,'indname','isna'),
(template_model_id,202,1,0,'indcond','NULL'),
(template_model_id,202,2,0,'indname','isneg'),
(template_model_id,202,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,203,0,0,'vargroup','Demographics'),
(template_model_id,204,0,0,'vargroup','Demographics'),
(template_model_id,205,0,0,'vargroup','Handset'),
(template_model_id,206,0,0,'vargroup','Demographics'),
(template_model_id,207,0,0,'vargroup','Other'),
(template_model_id,208,0,0,'vargroup','Subscription details'),
(template_model_id,209,0,0,'vargroup','Social network'),
(template_model_id,210,0,0,'vargroup','Social network'),
(template_model_id,211,0,0,'vargroup','Demographics'),
(template_model_id,212,0,0,'vargroup','Subscription details'),
(template_model_id,213,0,0,'vargroup','Subscription details'),
(template_model_id,214,0,0,'vargroup','Demographics'),
(template_model_id,205,0,0,'cat_th','0.02'),
(template_model_id,215,1,0,'interactname','mr_ratio.X.voicecount'),
(template_model_id,215,2,0,'interactname','alpha.X.alias_count'),
(template_model_id,215,3,0,'interactname','alpha.X.mr_ratio'),
(template_model_id,215,4,0,'interactname','contr_length.X.voicecount'),
(template_model_id,215,5,0,'interactname','contr_length.X.smscount'),
(template_model_id,215,6,0,'interactname','alias_count.X.mr_ratio'),
(template_model_id,215,7,0,'interactname','kshell.X.mr_ratio'),
(template_model_id,215,8,0,'interactname','c.X.mr_ratio'),
(template_model_id,215,9,0,'interactname','c.X.k'),
(template_model_id,215,10,0,'interactname','week_entropy.X.alias_count'),
(template_model_id,215,1,0,'interactcond','mr_ratio * voicecount'),
(template_model_id,215,2,0,'interactcond','alpha * alias_count'),
(template_model_id,215,3,0,'interactcond','alpha * mr_ratio'),
(template_model_id,215,4,0,'interactcond','contr_length * voicecount'),
(template_model_id,215,5,0,'interactcond','contr_length * smscount'),
(template_model_id,215,6,0,'interactcond','alias_count * mr_ratio'),
(template_model_id,215,7,0,'interactcond','kshell * mr_ratio'),
(template_model_id,215,8,0,'interactcond','c * mr_ratio'),
(template_model_id,215,9,0,'interactcond','c * k'),
(template_model_id,215,10,0,'interactcond','week_entropy * alias_count'),
(template_model_id,215,1,0,'interactgroup','Interactions'),
(template_model_id,215,2,0,'interactgroup','Interactions'),
(template_model_id,215,3,0,'interactgroup','Interactions'),
(template_model_id,215,4,0,'interactgroup','Interactions'),
(template_model_id,215,5,0,'interactgroup','Interactions'),
(template_model_id,215,6,0,'interactgroup','Interactions'),
(template_model_id,215,7,0,'interactgroup','Interactions'),
(template_model_id,215,8,0,'interactgroup','Interactions'),
(template_model_id,215,9,0,'interactgroup','Interactions'),
(template_model_id,215,10,0,'interactgroup','Interactions');
END; 

$BODY$
LANGUAGE 'plpgsql' VOLATILE; 

ALTER FUNCTION work.insert_churn_inactivity_template_model() OWNER TO xsl; 
  
SELECT * FROM work.insert_churn_inactivity_template_model();


CREATE OR REPLACE FUNCTION work.insert_churn_postpaid_template_model()
RETURNS void AS

$BODY$ 
    
DECLARE
template_model_id integer;
  
BEGIN
    
SELECT core.nextval_with_partitions('work.tmp_model_sequence') INTO template_model_id;
                        
INSERT INTO work.module_template_models
VALUES
(template_model_id,0,0,0,'model_type','logistic'),
(template_model_id,0,0,0,'use_case','churn_postpaid'),
(template_model_id,0,0,0,'target_name','target_churn_postpaid'),
(template_model_id,0,0,0,'is_parent','true'),
(template_model_id,0,0,0,'model_name','churn_postpaid_template_model'),
(template_model_id,0,0,0,'tempmodel_date','2014-10-29 11:09:46'),
(template_model_id,1,0,0,'varname','alias_count'),
(template_model_id,2,0,0,'varname','callsmsratio'),
(template_model_id,3,0,0,'varname','mr_count_ratio'),
(template_model_id,4,0,0,'varname','mr_ratio'),
(template_model_id,5,0,0,'varname','smscount'),
(template_model_id,6,0,0,'varname','smscountday'),
(template_model_id,7,0,0,'varname','smscountevening'),
(template_model_id,8,0,0,'varname','smscountweekday'),
(template_model_id,9,0,0,'varname','voicecount'),
(template_model_id,10,0,0,'varname','voicecountday'),
(template_model_id,11,0,0,'varname','voicecountevening'),
(template_model_id,12,0,0,'varname','voicecountweekday'),
(template_model_id,13,0,0,'varname','voicesum'),
(template_model_id,14,0,0,'varname','voicesumday'),
(template_model_id,15,0,0,'varname','voicesumevening'),
(template_model_id,16,0,0,'varname','voicesumweekday'),
(template_model_id,17,0,0,'varname','week_entropy'),
(template_model_id,18,0,0,'varname','week_mean'),
(template_model_id,19,0,0,'varname','weekly_cell_id_count1'),
(template_model_id,20,0,0,'varname','weekly_cell_id_count2'),
(template_model_id,21,0,0,'varname','weekly_cell_id_count3'),
(template_model_id,22,0,0,'varname','weekly_cell_id_count4'),
(template_model_id,23,0,0,'varname','weekly_cost1'),
(template_model_id,24,0,0,'varname','weekly_cost2'),
(template_model_id,25,0,0,'varname','weekly_cost3'),
(template_model_id,26,0,0,'varname','weekly_cost4'),
(template_model_id,27,0,0,'varname','daily_voice_activity1'),
(template_model_id,28,0,0,'varname','daily_voice_activity2'),
(template_model_id,29,0,0,'varname','daily_voice_activity3'),
(template_model_id,30,0,0,'varname','daily_voice_activity4'),
(template_model_id,31,0,0,'varname','weekly_voice_cost1'),
(template_model_id,32,0,0,'varname','weekly_voice_cost2'),
(template_model_id,33,0,0,'varname','weekly_voice_cost3'),
(template_model_id,34,0,0,'varname','weekly_voice_cost4'),
(template_model_id,35,0,0,'varname','weekly_voice_count1'),
(template_model_id,36,0,0,'varname','weekly_voice_count2'),
(template_model_id,37,0,0,'varname','weekly_voice_count3'),
(template_model_id,38,0,0,'varname','weekly_voice_count4'),
(template_model_id,39,0,0,'varname','weekly_voice_duration1'),
(template_model_id,40,0,0,'varname','weekly_voice_duration2'),
(template_model_id,41,0,0,'varname','weekly_voice_duration3'),
(template_model_id,42,0,0,'varname','weekly_voice_duration4'),
(template_model_id,43,0,0,'varname','weekly_voice_neigh_count1'),
(template_model_id,44,0,0,'varname','weekly_voice_neigh_count2'),
(template_model_id,45,0,0,'varname','weekly_voice_neigh_count3'),
(template_model_id,46,0,0,'varname','weekly_voice_neigh_count4'),
(template_model_id,47,0,0,'varname','daily_sms_activity1'),
(template_model_id,48,0,0,'varname','daily_sms_activity2'),
(template_model_id,49,0,0,'varname','daily_sms_activity3'),
(template_model_id,50,0,0,'varname','daily_sms_activity4'),
(template_model_id,51,0,0,'varname','weekly_sms_cost1'),
(template_model_id,52,0,0,'varname','weekly_sms_cost2'),
(template_model_id,53,0,0,'varname','weekly_sms_cost3'),
(template_model_id,54,0,0,'varname','weekly_sms_cost4'),
(template_model_id,55,0,0,'varname','weekly_sms_count1'),
(template_model_id,56,0,0,'varname','weekly_sms_count2'),
(template_model_id,57,0,0,'varname','weekly_sms_count3'),
(template_model_id,58,0,0,'varname','weekly_sms_count4'),
(template_model_id,59,0,0,'varname','daily_data_activity1'),
(template_model_id,60,0,0,'varname','daily_data_activity2'),
(template_model_id,61,0,0,'varname','daily_data_activity3'),
(template_model_id,62,0,0,'varname','daily_data_activity4'),
(template_model_id,63,0,0,'varname','weekly_data_count1'),
(template_model_id,64,0,0,'varname','weekly_data_count2'),
(template_model_id,65,0,0,'varname','weekly_data_count3'),
(template_model_id,66,0,0,'varname','weekly_data_count4'),
(template_model_id,67,0,0,'varname','weekly_data_usage1'),
(template_model_id,68,0,0,'varname','weekly_data_usage2'),
(template_model_id,69,0,0,'varname','weekly_data_usage3'),
(template_model_id,70,0,0,'varname','weekly_data_usage4'),
(template_model_id,71,0,0,'varname','inact'),
(template_model_id,72,0,0,'varname','weekly_cell_id_count_rec1'),
(template_model_id,73,0,0,'varname','weekly_cell_id_count_rec2'),
(template_model_id,74,0,0,'varname','weekly_cell_id_count_rec3'),
(template_model_id,75,0,0,'varname','weekly_cell_id_count_rec4'),
(template_model_id,76,0,0,'varname','daily_voice_activity_rec1'),
(template_model_id,77,0,0,'varname','daily_voice_activity_rec2'),
(template_model_id,78,0,0,'varname','daily_voice_activity_rec3'),
(template_model_id,79,0,0,'varname','daily_voice_activity_rec4'),
(template_model_id,80,0,0,'varname','weekly_voice_count_rec1'),
(template_model_id,81,0,0,'varname','weekly_voice_count_rec2'),
(template_model_id,82,0,0,'varname','weekly_voice_count_rec3'),
(template_model_id,83,0,0,'varname','weekly_voice_count_rec4'),
(template_model_id,84,0,0,'varname','weekly_voice_duration_rec1'),
(template_model_id,85,0,0,'varname','weekly_voice_duration_rec2'),
(template_model_id,86,0,0,'varname','weekly_voice_duration_rec3'),
(template_model_id,87,0,0,'varname','weekly_voice_duration_rec4'),
(template_model_id,88,0,0,'varname','daily_sms_activity_rec1'),
(template_model_id,89,0,0,'varname','daily_sms_activity_rec2'),
(template_model_id,90,0,0,'varname','daily_sms_activity_rec3'),
(template_model_id,91,0,0,'varname','daily_sms_activity_rec4'),
(template_model_id,92,0,0,'varname','weekly_sms_count_rec1'),
(template_model_id,93,0,0,'varname','weekly_sms_count_rec2'),
(template_model_id,94,0,0,'varname','weekly_sms_count_rec3'),
(template_model_id,95,0,0,'varname','weekly_sms_count_rec4'),
(template_model_id,96,0,0,'varname','inact_rec'),
(template_model_id,97,0,0,'varname','knn'),
(template_model_id,98,0,0,'varname','knn_2'),
(template_model_id,99,0,0,'varname','k_offnet'),
(template_model_id,100,0,0,'varname','k_target'),
(template_model_id,101,0,0,'varname','age'),
(template_model_id,102,0,0,'varname','alpha'),
(template_model_id,103,0,0,'varname','c'),
(template_model_id,104,0,0,'varname','alpha_2'),
(template_model_id,105,0,0,'varname','c_2'),
(template_model_id,106,0,0,'varname','churn_score'),
(template_model_id,107,0,0,'varname','contr_length'),
(template_model_id,108,0,0,'varname','contr_remain'),
(template_model_id,109,0,0,'varname','handset_age'),
(template_model_id,110,0,0,'varname','k'),
(template_model_id,111,0,0,'varname','kshell'),
(template_model_id,112,0,0,'varname','socrev'),
(template_model_id,113,0,0,'varname','socrevest'),
(template_model_id,114,0,0,'varname','k_2'),
(template_model_id,115,0,0,'varname','kshell_2'),
(template_model_id,116,0,0,'varname','socrev_2'),
(template_model_id,117,0,0,'varname','socrevest_2'),
(template_model_id,118,0,0,'varname','subscriber_value'),
(template_model_id,119,0,0,'varname','wec'),
(template_model_id,120,0,0,'varname','wec_2'),
(template_model_id,121,0,0,'varname','handset_topic_1'),
(template_model_id,122,0,0,'varname','handset_topic_2'),
(template_model_id,123,0,0,'varname','handset_topic_3'),
(template_model_id,124,0,0,'varname','handset_topic_4'),
(template_model_id,125,0,0,'varname','handset_topic_5'),
(template_model_id,126,0,0,'varname','cell_events_topic_1_1'),
(template_model_id,127,0,0,'varname','cell_events_topic_2_1'),
(template_model_id,128,0,0,'varname','cell_events_topic_3_1'),
(template_model_id,129,0,0,'varname','cell_events_topic_4_1'),
(template_model_id,130,0,0,'varname','cell_events_topic_5_1'),
(template_model_id,131,0,0,'varname','cell_events_topic_1_2'),
(template_model_id,132,0,0,'varname','cell_events_topic_2_2'),
(template_model_id,133,0,0,'varname','cell_events_topic_3_2'),
(template_model_id,134,0,0,'varname','cell_events_topic_4_2'),
(template_model_id,135,0,0,'varname','cell_events_topic_5_2'),
(template_model_id,136,0,0,'varname','mobility'),
(template_model_id,137,0,0,'varname','alltime_long_diversity'),
(template_model_id,138,0,0,'varname','leisure_long_diversity'),
(template_model_id,139,0,0,'varname','business_long_diversity'),
(template_model_id,140,0,0,'varname','alltime_short_diversity'),
(template_model_id,141,0,0,'varname','leisure_short_diversity'),
(template_model_id,142,0,0,'varname','business_short_diversity'),
(template_model_id,1,0,0,'vartype','numeric'),
(template_model_id,2,0,0,'vartype','numeric'),
(template_model_id,3,0,0,'vartype','numeric'),
(template_model_id,4,0,0,'vartype','numeric'),
(template_model_id,5,0,0,'vartype','numeric'),
(template_model_id,6,0,0,'vartype','numeric'),
(template_model_id,7,0,0,'vartype','numeric'),
(template_model_id,8,0,0,'vartype','numeric'),
(template_model_id,9,0,0,'vartype','numeric'),
(template_model_id,10,0,0,'vartype','numeric'),
(template_model_id,11,0,0,'vartype','numeric'),
(template_model_id,12,0,0,'vartype','numeric'),
(template_model_id,13,0,0,'vartype','numeric'),
(template_model_id,14,0,0,'vartype','numeric'),
(template_model_id,15,0,0,'vartype','numeric'),
(template_model_id,16,0,0,'vartype','numeric'),
(template_model_id,17,0,0,'vartype','numeric'),
(template_model_id,18,0,0,'vartype','numeric'),
(template_model_id,19,0,0,'vartype','numeric'),
(template_model_id,20,0,0,'vartype','numeric'),
(template_model_id,21,0,0,'vartype','numeric'),
(template_model_id,22,0,0,'vartype','numeric'),
(template_model_id,23,0,0,'vartype','numeric'),
(template_model_id,24,0,0,'vartype','numeric'),
(template_model_id,25,0,0,'vartype','numeric'),
(template_model_id,26,0,0,'vartype','numeric'),
(template_model_id,27,0,0,'vartype','numeric'),
(template_model_id,28,0,0,'vartype','numeric'),
(template_model_id,29,0,0,'vartype','numeric'),
(template_model_id,30,0,0,'vartype','numeric'),
(template_model_id,31,0,0,'vartype','numeric'),
(template_model_id,32,0,0,'vartype','numeric'),
(template_model_id,33,0,0,'vartype','numeric'),
(template_model_id,34,0,0,'vartype','numeric'),
(template_model_id,35,0,0,'vartype','numeric'),
(template_model_id,36,0,0,'vartype','numeric'),
(template_model_id,37,0,0,'vartype','numeric'),
(template_model_id,38,0,0,'vartype','numeric'),
(template_model_id,39,0,0,'vartype','numeric'),
(template_model_id,40,0,0,'vartype','numeric'),
(template_model_id,41,0,0,'vartype','numeric'),
(template_model_id,42,0,0,'vartype','numeric'),
(template_model_id,43,0,0,'vartype','numeric'),
(template_model_id,44,0,0,'vartype','numeric'),
(template_model_id,45,0,0,'vartype','numeric'),
(template_model_id,46,0,0,'vartype','numeric'),
(template_model_id,47,0,0,'vartype','numeric'),
(template_model_id,48,0,0,'vartype','numeric'),
(template_model_id,49,0,0,'vartype','numeric'),
(template_model_id,50,0,0,'vartype','numeric'),
(template_model_id,51,0,0,'vartype','numeric'),
(template_model_id,52,0,0,'vartype','numeric'),
(template_model_id,53,0,0,'vartype','numeric'),
(template_model_id,54,0,0,'vartype','numeric'),
(template_model_id,55,0,0,'vartype','numeric'),
(template_model_id,56,0,0,'vartype','numeric'),
(template_model_id,57,0,0,'vartype','numeric'),
(template_model_id,58,0,0,'vartype','numeric'),
(template_model_id,59,0,0,'vartype','numeric'),
(template_model_id,60,0,0,'vartype','numeric'),
(template_model_id,61,0,0,'vartype','numeric'),
(template_model_id,62,0,0,'vartype','numeric'),
(template_model_id,63,0,0,'vartype','numeric'),
(template_model_id,64,0,0,'vartype','numeric'),
(template_model_id,65,0,0,'vartype','numeric'),
(template_model_id,66,0,0,'vartype','numeric'),
(template_model_id,67,0,0,'vartype','numeric'),
(template_model_id,68,0,0,'vartype','numeric'),
(template_model_id,69,0,0,'vartype','numeric'),
(template_model_id,70,0,0,'vartype','numeric'),
(template_model_id,71,0,0,'vartype','numeric'),
(template_model_id,72,0,0,'vartype','numeric'),
(template_model_id,73,0,0,'vartype','numeric'),
(template_model_id,74,0,0,'vartype','numeric'),
(template_model_id,75,0,0,'vartype','numeric'),
(template_model_id,76,0,0,'vartype','numeric'),
(template_model_id,77,0,0,'vartype','numeric'),
(template_model_id,78,0,0,'vartype','numeric'),
(template_model_id,79,0,0,'vartype','numeric'),
(template_model_id,80,0,0,'vartype','numeric'),
(template_model_id,81,0,0,'vartype','numeric'),
(template_model_id,82,0,0,'vartype','numeric'),
(template_model_id,83,0,0,'vartype','numeric'),
(template_model_id,84,0,0,'vartype','numeric'),
(template_model_id,85,0,0,'vartype','numeric'),
(template_model_id,86,0,0,'vartype','numeric'),
(template_model_id,87,0,0,'vartype','numeric'),
(template_model_id,88,0,0,'vartype','numeric'),
(template_model_id,89,0,0,'vartype','numeric'),
(template_model_id,90,0,0,'vartype','numeric'),
(template_model_id,91,0,0,'vartype','numeric'),
(template_model_id,92,0,0,'vartype','numeric'),
(template_model_id,93,0,0,'vartype','numeric'),
(template_model_id,94,0,0,'vartype','numeric'),
(template_model_id,95,0,0,'vartype','numeric'),
(template_model_id,96,0,0,'vartype','numeric'),
(template_model_id,97,0,0,'vartype','numeric'),
(template_model_id,98,0,0,'vartype','numeric'),
(template_model_id,99,0,0,'vartype','numeric'),
(template_model_id,100,0,0,'vartype','numeric'),
(template_model_id,101,0,0,'vartype','numeric'),
(template_model_id,102,0,0,'vartype','numeric'),
(template_model_id,103,0,0,'vartype','numeric'),
(template_model_id,104,0,0,'vartype','numeric'),
(template_model_id,105,0,0,'vartype','numeric'),
(template_model_id,106,0,0,'vartype','numeric'),
(template_model_id,107,0,0,'vartype','numeric'),
(template_model_id,108,0,0,'vartype','numeric'),
(template_model_id,109,0,0,'vartype','numeric'),
(template_model_id,110,0,0,'vartype','numeric'),
(template_model_id,111,0,0,'vartype','numeric'),
(template_model_id,112,0,0,'vartype','numeric'),
(template_model_id,113,0,0,'vartype','numeric'),
(template_model_id,114,0,0,'vartype','numeric'),
(template_model_id,115,0,0,'vartype','numeric'),
(template_model_id,116,0,0,'vartype','numeric'),
(template_model_id,117,0,0,'vartype','numeric'),
(template_model_id,118,0,0,'vartype','numeric'),
(template_model_id,119,0,0,'vartype','numeric'),
(template_model_id,120,0,0,'vartype','numeric'),
(template_model_id,121,0,0,'vartype','numeric'),
(template_model_id,122,0,0,'vartype','numeric'),
(template_model_id,123,0,0,'vartype','numeric'),
(template_model_id,124,0,0,'vartype','numeric'),
(template_model_id,125,0,0,'vartype','numeric'),
(template_model_id,126,0,0,'vartype','numeric'),
(template_model_id,127,0,0,'vartype','numeric'),
(template_model_id,128,0,0,'vartype','numeric'),
(template_model_id,129,0,0,'vartype','numeric'),
(template_model_id,130,0,0,'vartype','numeric'),
(template_model_id,131,0,0,'vartype','numeric'),
(template_model_id,132,0,0,'vartype','numeric'),
(template_model_id,133,0,0,'vartype','numeric'),
(template_model_id,134,0,0,'vartype','numeric'),
(template_model_id,135,0,0,'vartype','numeric'),
(template_model_id,136,0,0,'vartype','numeric'),
(template_model_id,137,0,0,'vartype','numeric'),
(template_model_id,138,0,0,'vartype','numeric'),
(template_model_id,139,0,0,'vartype','numeric'),
(template_model_id,140,0,0,'vartype','numeric'),
(template_model_id,141,0,0,'vartype','numeric'),
(template_model_id,142,0,0,'vartype','numeric'),
(template_model_id,143,0,0,'varname','country'),
(template_model_id,144,0,0,'varname','gender'),
(template_model_id,145,0,0,'varname','handset_model'),
(template_model_id,146,0,0,'varname','language'),
(template_model_id,147,0,0,'varname','no_churn_score'),
(template_model_id,148,0,0,'varname','payment_type'),
(template_model_id,149,0,0,'varname','separate_node'),
(template_model_id,150,0,0,'varname','separate_node_2'),
(template_model_id,151,0,0,'varname','subscriber_segment'),
(template_model_id,152,0,0,'varname','subscription_type'),
(template_model_id,153,0,0,'varname','tariff_plan'),
(template_model_id,154,0,0,'varname','zip'),
(template_model_id,143,0,0,'vartype','categorical'),
(template_model_id,144,0,0,'vartype','categorical'),
(template_model_id,145,0,0,'vartype','categorical'),
(template_model_id,146,0,0,'vartype','categorical'),
(template_model_id,147,0,0,'vartype','categorical'),
(template_model_id,148,0,0,'vartype','categorical'),
(template_model_id,149,0,0,'vartype','categorical'),
(template_model_id,150,0,0,'vartype','categorical'),
(template_model_id,151,0,0,'vartype','categorical'),
(template_model_id,152,0,0,'vartype','categorical'),
(template_model_id,153,0,0,'vartype','categorical'),
(template_model_id,154,0,0,'vartype','categorical'),
(template_model_id,1,0,0,'preprocessfun','ecdf'),
(template_model_id,2,0,0,'preprocessfun','ecdf'),
(template_model_id,3,0,0,'preprocessfun','ecdf'),
(template_model_id,4,0,0,'preprocessfun','ecdf'),
(template_model_id,5,0,0,'preprocessfun','ecdf'),
(template_model_id,6,0,0,'preprocessfun','ecdf'),
(template_model_id,7,0,0,'preprocessfun','ecdf'),
(template_model_id,8,0,0,'preprocessfun','ecdf'),
(template_model_id,9,0,0,'preprocessfun','ecdf'),
(template_model_id,10,0,0,'preprocessfun','ecdf'),
(template_model_id,11,0,0,'preprocessfun','ecdf'),
(template_model_id,12,0,0,'preprocessfun','ecdf'),
(template_model_id,13,0,0,'preprocessfun','ecdf'),
(template_model_id,14,0,0,'preprocessfun','ecdf'),
(template_model_id,15,0,0,'preprocessfun','ecdf'),
(template_model_id,16,0,0,'preprocessfun','ecdf'),
(template_model_id,17,0,0,'preprocessfun','ecdf'),
(template_model_id,18,0,0,'preprocessfun','ecdf'),
(template_model_id,19,0,0,'preprocessfun','ecdf'),
(template_model_id,20,0,0,'preprocessfun','ecdf'),
(template_model_id,21,0,0,'preprocessfun','ecdf'),
(template_model_id,22,0,0,'preprocessfun','ecdf'),
(template_model_id,23,0,0,'preprocessfun','ecdf'),
(template_model_id,24,0,0,'preprocessfun','ecdf'),
(template_model_id,25,0,0,'preprocessfun','ecdf'),
(template_model_id,26,0,0,'preprocessfun','ecdf'),
(template_model_id,27,0,0,'preprocessfun','ecdf'),
(template_model_id,28,0,0,'preprocessfun','ecdf'),
(template_model_id,29,0,0,'preprocessfun','ecdf'),
(template_model_id,30,0,0,'preprocessfun','ecdf'),
(template_model_id,31,0,0,'preprocessfun','ecdf'),
(template_model_id,32,0,0,'preprocessfun','ecdf'),
(template_model_id,33,0,0,'preprocessfun','ecdf'),
(template_model_id,34,0,0,'preprocessfun','ecdf'),
(template_model_id,35,0,0,'preprocessfun','ecdf'),
(template_model_id,36,0,0,'preprocessfun','ecdf'),
(template_model_id,37,0,0,'preprocessfun','ecdf'),
(template_model_id,38,0,0,'preprocessfun','ecdf'),
(template_model_id,39,0,0,'preprocessfun','ecdf'),
(template_model_id,40,0,0,'preprocessfun','ecdf'),
(template_model_id,41,0,0,'preprocessfun','ecdf'),
(template_model_id,42,0,0,'preprocessfun','ecdf'),
(template_model_id,43,0,0,'preprocessfun','ecdf'),
(template_model_id,44,0,0,'preprocessfun','ecdf'),
(template_model_id,45,0,0,'preprocessfun','ecdf'),
(template_model_id,46,0,0,'preprocessfun','ecdf'),
(template_model_id,47,0,0,'preprocessfun','ecdf'),
(template_model_id,48,0,0,'preprocessfun','ecdf'),
(template_model_id,49,0,0,'preprocessfun','ecdf'),
(template_model_id,50,0,0,'preprocessfun','ecdf'),
(template_model_id,51,0,0,'preprocessfun','ecdf'),
(template_model_id,52,0,0,'preprocessfun','ecdf'),
(template_model_id,53,0,0,'preprocessfun','ecdf'),
(template_model_id,54,0,0,'preprocessfun','ecdf'),
(template_model_id,55,0,0,'preprocessfun','ecdf'),
(template_model_id,56,0,0,'preprocessfun','ecdf'),
(template_model_id,57,0,0,'preprocessfun','ecdf'),
(template_model_id,58,0,0,'preprocessfun','ecdf'),
(template_model_id,59,0,0,'preprocessfun','ecdf'),
(template_model_id,60,0,0,'preprocessfun','ecdf'),
(template_model_id,61,0,0,'preprocessfun','ecdf'),
(template_model_id,62,0,0,'preprocessfun','ecdf'),
(template_model_id,63,0,0,'preprocessfun','ecdf'),
(template_model_id,64,0,0,'preprocessfun','ecdf'),
(template_model_id,65,0,0,'preprocessfun','ecdf'),
(template_model_id,66,0,0,'preprocessfun','ecdf'),
(template_model_id,67,0,0,'preprocessfun','ecdf'),
(template_model_id,68,0,0,'preprocessfun','ecdf'),
(template_model_id,69,0,0,'preprocessfun','ecdf'),
(template_model_id,70,0,0,'preprocessfun','ecdf'),
(template_model_id,71,0,0,'preprocessfun','ecdf'),
(template_model_id,72,0,0,'preprocessfun','ecdf'),
(template_model_id,73,0,0,'preprocessfun','ecdf'),
(template_model_id,74,0,0,'preprocessfun','ecdf'),
(template_model_id,75,0,0,'preprocessfun','ecdf'),
(template_model_id,76,0,0,'preprocessfun','ecdf'),
(template_model_id,77,0,0,'preprocessfun','ecdf'),
(template_model_id,78,0,0,'preprocessfun','ecdf'),
(template_model_id,79,0,0,'preprocessfun','ecdf'),
(template_model_id,80,0,0,'preprocessfun','ecdf'),
(template_model_id,81,0,0,'preprocessfun','ecdf'),
(template_model_id,82,0,0,'preprocessfun','ecdf'),
(template_model_id,83,0,0,'preprocessfun','ecdf'),
(template_model_id,84,0,0,'preprocessfun','ecdf'),
(template_model_id,85,0,0,'preprocessfun','ecdf'),
(template_model_id,86,0,0,'preprocessfun','ecdf'),
(template_model_id,87,0,0,'preprocessfun','ecdf'),
(template_model_id,88,0,0,'preprocessfun','ecdf'),
(template_model_id,89,0,0,'preprocessfun','ecdf'),
(template_model_id,90,0,0,'preprocessfun','ecdf'),
(template_model_id,91,0,0,'preprocessfun','ecdf'),
(template_model_id,92,0,0,'preprocessfun','ecdf'),
(template_model_id,93,0,0,'preprocessfun','ecdf'),
(template_model_id,94,0,0,'preprocessfun','ecdf'),
(template_model_id,95,0,0,'preprocessfun','ecdf'),
(template_model_id,96,0,0,'preprocessfun','ecdf'),
(template_model_id,97,0,0,'preprocessfun','ecdf'),
(template_model_id,98,0,0,'preprocessfun','ecdf'),
(template_model_id,99,0,0,'preprocessfun','ecdf'),
(template_model_id,100,0,0,'preprocessfun','ecdf'),
(template_model_id,101,0,0,'preprocessfun','ecdf'),
(template_model_id,102,0,0,'preprocessfun','ecdf'),
(template_model_id,103,0,0,'preprocessfun','ecdf'),
(template_model_id,104,0,0,'preprocessfun','ecdf'),
(template_model_id,105,0,0,'preprocessfun','ecdf'),
(template_model_id,106,0,0,'preprocessfun','ecdf'),
(template_model_id,108,0,0,'preprocessfun','ecdf'),
(template_model_id,109,0,0,'preprocessfun','ecdf'),
(template_model_id,110,0,0,'preprocessfun','ecdf'),
(template_model_id,111,0,0,'preprocessfun','ecdf'),
(template_model_id,112,0,0,'preprocessfun','ecdf'),
(template_model_id,113,0,0,'preprocessfun','ecdf'),
(template_model_id,114,0,0,'preprocessfun','ecdf'),
(template_model_id,115,0,0,'preprocessfun','ecdf'),
(template_model_id,116,0,0,'preprocessfun','ecdf'),
(template_model_id,117,0,0,'preprocessfun','ecdf'),
(template_model_id,118,0,0,'preprocessfun','ecdf'),
(template_model_id,119,0,0,'preprocessfun','ecdf'),
(template_model_id,120,0,0,'preprocessfun','ecdf'),
(template_model_id,121,0,0,'preprocessfun','ecdf'),
(template_model_id,122,0,0,'preprocessfun','ecdf'),
(template_model_id,123,0,0,'preprocessfun','ecdf'),
(template_model_id,124,0,0,'preprocessfun','ecdf'),
(template_model_id,125,0,0,'preprocessfun','ecdf'),
(template_model_id,126,0,0,'preprocessfun','ecdf'),
(template_model_id,127,0,0,'preprocessfun','ecdf'),
(template_model_id,128,0,0,'preprocessfun','ecdf'),
(template_model_id,129,0,0,'preprocessfun','ecdf'),
(template_model_id,130,0,0,'preprocessfun','ecdf'),
(template_model_id,131,0,0,'preprocessfun','ecdf'),
(template_model_id,132,0,0,'preprocessfun','ecdf'),
(template_model_id,133,0,0,'preprocessfun','ecdf'),
(template_model_id,134,0,0,'preprocessfun','ecdf'),
(template_model_id,135,0,0,'preprocessfun','ecdf'),
(template_model_id,136,0,0,'preprocessfun','ecdf'),
(template_model_id,137,0,0,'preprocessfun','ecdf'),
(template_model_id,138,0,0,'preprocessfun','ecdf'),
(template_model_id,139,0,0,'preprocessfun','ecdf'),
(template_model_id,140,0,0,'preprocessfun','ecdf'),
(template_model_id,141,0,0,'preprocessfun','ecdf'),
(template_model_id,142,0,0,'preprocessfun','ecdf'),
(template_model_id,107,0,0,'preprocessfun','pow'),
(template_model_id,107,1,0,'preprocessparam','0.2'),
(template_model_id,1,1,0,'rcs','3'),
(template_model_id,2,1,0,'rcs','3'),
(template_model_id,3,1,0,'rcs','3'),
(template_model_id,4,1,0,'rcs','3'),
(template_model_id,5,1,0,'rcs','3'),
(template_model_id,6,1,0,'rcs','3'),
(template_model_id,7,1,0,'rcs','3'),
(template_model_id,8,1,0,'rcs','3'),
(template_model_id,9,1,0,'rcs','3'),
(template_model_id,10,1,0,'rcs','3'),
(template_model_id,11,1,0,'rcs','3'),
(template_model_id,12,1,0,'rcs','3'),
(template_model_id,13,1,0,'rcs','3'),
(template_model_id,14,1,0,'rcs','3'),
(template_model_id,15,1,0,'rcs','3'),
(template_model_id,16,1,0,'rcs','3'),
(template_model_id,17,1,0,'rcs','3'),
(template_model_id,18,1,0,'rcs','3'),
(template_model_id,19,1,0,'rcs','3'),
(template_model_id,20,1,0,'rcs','3'),
(template_model_id,21,1,0,'rcs','3'),
(template_model_id,22,1,0,'rcs','3'),
(template_model_id,23,1,0,'rcs','3'),
(template_model_id,24,1,0,'rcs','3'),
(template_model_id,25,1,0,'rcs','3'),
(template_model_id,26,1,0,'rcs','3'),
(template_model_id,27,1,0,'rcs','3'),
(template_model_id,28,1,0,'rcs','3'),
(template_model_id,29,1,0,'rcs','3'),
(template_model_id,30,1,0,'rcs','3'),
(template_model_id,31,1,0,'rcs','3'),
(template_model_id,32,1,0,'rcs','3'),
(template_model_id,33,1,0,'rcs','3'),
(template_model_id,34,1,0,'rcs','3'),
(template_model_id,35,1,0,'rcs','3'),
(template_model_id,36,1,0,'rcs','3'),
(template_model_id,37,1,0,'rcs','3'),
(template_model_id,38,1,0,'rcs','3'),
(template_model_id,39,1,0,'rcs','3'),
(template_model_id,40,1,0,'rcs','3'),
(template_model_id,41,1,0,'rcs','3'),
(template_model_id,42,1,0,'rcs','3'),
(template_model_id,43,1,0,'rcs','3'),
(template_model_id,44,1,0,'rcs','3'),
(template_model_id,45,1,0,'rcs','3'),
(template_model_id,46,1,0,'rcs','3'),
(template_model_id,47,1,0,'rcs','3'),
(template_model_id,48,1,0,'rcs','3'),
(template_model_id,49,1,0,'rcs','3'),
(template_model_id,50,1,0,'rcs','3'),
(template_model_id,51,1,0,'rcs','3'),
(template_model_id,52,1,0,'rcs','3'),
(template_model_id,53,1,0,'rcs','3'),
(template_model_id,54,1,0,'rcs','3'),
(template_model_id,55,1,0,'rcs','3'),
(template_model_id,56,1,0,'rcs','3'),
(template_model_id,57,1,0,'rcs','3'),
(template_model_id,58,1,0,'rcs','3'),
(template_model_id,59,1,0,'rcs','3'),
(template_model_id,60,1,0,'rcs','3'),
(template_model_id,61,1,0,'rcs','3'),
(template_model_id,62,1,0,'rcs','3'),
(template_model_id,63,1,0,'rcs','3'),
(template_model_id,64,1,0,'rcs','3'),
(template_model_id,65,1,0,'rcs','3'),
(template_model_id,66,1,0,'rcs','3'),
(template_model_id,67,1,0,'rcs','3'),
(template_model_id,68,1,0,'rcs','3'),
(template_model_id,69,1,0,'rcs','3'),
(template_model_id,70,1,0,'rcs','3'),
(template_model_id,71,1,0,'rcs','5'),
(template_model_id,72,1,0,'rcs','3'),
(template_model_id,73,1,0,'rcs','3'),
(template_model_id,74,1,0,'rcs','3'),
(template_model_id,75,1,0,'rcs','3'),
(template_model_id,76,1,0,'rcs','3'),
(template_model_id,77,1,0,'rcs','3'),
(template_model_id,78,1,0,'rcs','3'),
(template_model_id,79,1,0,'rcs','3'),
(template_model_id,80,1,0,'rcs','3'),
(template_model_id,81,1,0,'rcs','3'),
(template_model_id,82,1,0,'rcs','3'),
(template_model_id,83,1,0,'rcs','3'),
(template_model_id,84,1,0,'rcs','3'),
(template_model_id,85,1,0,'rcs','3'),
(template_model_id,86,1,0,'rcs','3'),
(template_model_id,87,1,0,'rcs','3'),
(template_model_id,88,1,0,'rcs','3'),
(template_model_id,89,1,0,'rcs','3'),
(template_model_id,90,1,0,'rcs','3'),
(template_model_id,91,1,0,'rcs','3'),
(template_model_id,92,1,0,'rcs','3'),
(template_model_id,93,1,0,'rcs','3'),
(template_model_id,94,1,0,'rcs','3'),
(template_model_id,95,1,0,'rcs','3'),
(template_model_id,96,1,0,'rcs','5'),
(template_model_id,97,1,0,'rcs','3'),
(template_model_id,98,1,0,'rcs','3'),
(template_model_id,99,1,0,'rcs','3'),
(template_model_id,100,1,0,'rcs','3'),
(template_model_id,101,1,0,'rcs','3'),
(template_model_id,102,1,0,'rcs','3'),
(template_model_id,103,1,0,'rcs','3'),
(template_model_id,104,1,0,'rcs','3'),
(template_model_id,105,1,0,'rcs','3'),
(template_model_id,107,1,0,'rcs','5'),
(template_model_id,109,1,0,'rcs','3'),
(template_model_id,110,1,0,'rcs','3'),
(template_model_id,111,1,0,'rcs','3'),
(template_model_id,112,1,0,'rcs','3'),
(template_model_id,113,1,0,'rcs','3'),
(template_model_id,114,1,0,'rcs','3'),
(template_model_id,115,1,0,'rcs','3'),
(template_model_id,116,1,0,'rcs','3'),
(template_model_id,117,1,0,'rcs','3'),
(template_model_id,118,1,0,'rcs','3'),
(template_model_id,119,1,0,'rcs','3'),
(template_model_id,120,1,0,'rcs','3'),
(template_model_id,1,1,0,'freqlim','0.3'),
(template_model_id,2,1,0,'freqlim','0.1'),
(template_model_id,3,1,0,'freqlim','0.1'),
(template_model_id,4,1,0,'freqlim','0.1'),
(template_model_id,5,1,0,'freqlim','0.3'),
(template_model_id,6,1,0,'freqlim','0.3'),
(template_model_id,7,1,0,'freqlim','0.3'),
(template_model_id,8,1,0,'freqlim','0.3'),
(template_model_id,9,1,0,'freqlim','0.3'),
(template_model_id,10,1,0,'freqlim','0.3'),
(template_model_id,11,1,0,'freqlim','0.3'),
(template_model_id,12,1,0,'freqlim','0.3'),
(template_model_id,13,1,0,'freqlim','0.1'),
(template_model_id,14,1,0,'freqlim','0.1'),
(template_model_id,15,1,0,'freqlim','0.1'),
(template_model_id,16,1,0,'freqlim','0.1'),
(template_model_id,17,1,0,'freqlim','0.1'),
(template_model_id,18,1,0,'freqlim','0.1'),
(template_model_id,19,1,0,'freqlim','0.1'),
(template_model_id,20,1,0,'freqlim','0.1'),
(template_model_id,21,1,0,'freqlim','0.1'),
(template_model_id,22,1,0,'freqlim','0.1'),
(template_model_id,23,1,0,'freqlim','0.1'),
(template_model_id,24,1,0,'freqlim','0.1'),
(template_model_id,25,1,0,'freqlim','0.1'),
(template_model_id,26,1,0,'freqlim','0.1'),
(template_model_id,27,1,0,'freqlim','0.3'),
(template_model_id,28,1,0,'freqlim','0.3'),
(template_model_id,29,1,0,'freqlim','0.3'),
(template_model_id,30,1,0,'freqlim','0.3'),
(template_model_id,31,1,0,'freqlim','0.1'),
(template_model_id,32,1,0,'freqlim','0.1'),
(template_model_id,33,1,0,'freqlim','0.1'),
(template_model_id,34,1,0,'freqlim','0.1'),
(template_model_id,35,1,0,'freqlim','0.1'),
(template_model_id,36,1,0,'freqlim','0.1'),
(template_model_id,37,1,0,'freqlim','0.1'),
(template_model_id,38,1,0,'freqlim','0.1'),
(template_model_id,39,1,0,'freqlim','0.1'),
(template_model_id,40,1,0,'freqlim','0.1'),
(template_model_id,41,1,0,'freqlim','0.1'),
(template_model_id,42,1,0,'freqlim','0.1'),
(template_model_id,43,1,0,'freqlim','0.1'),
(template_model_id,44,1,0,'freqlim','0.1'),
(template_model_id,45,1,0,'freqlim','0.1'),
(template_model_id,46,1,0,'freqlim','0.1'),
(template_model_id,47,1,0,'freqlim','0.3'),
(template_model_id,48,1,0,'freqlim','0.3'),
(template_model_id,49,1,0,'freqlim','0.3'),
(template_model_id,50,1,0,'freqlim','0.3'),
(template_model_id,51,1,0,'freqlim','0.1'),
(template_model_id,52,1,0,'freqlim','0.1'),
(template_model_id,53,1,0,'freqlim','0.1'),
(template_model_id,54,1,0,'freqlim','0.1'),
(template_model_id,55,1,0,'freqlim','0.1'),
(template_model_id,56,1,0,'freqlim','0.1'),
(template_model_id,57,1,0,'freqlim','0.1'),
(template_model_id,58,1,0,'freqlim','0.1'),
(template_model_id,59,1,0,'freqlim','0.3'),
(template_model_id,60,1,0,'freqlim','0.3'),
(template_model_id,61,1,0,'freqlim','0.3'),
(template_model_id,62,1,0,'freqlim','0.3'),
(template_model_id,63,1,0,'freqlim','0.1'),
(template_model_id,64,1,0,'freqlim','0.1'),
(template_model_id,65,1,0,'freqlim','0.1'),
(template_model_id,66,1,0,'freqlim','0.1'),
(template_model_id,67,1,0,'freqlim','0.1'),
(template_model_id,68,1,0,'freqlim','0.1'),
(template_model_id,69,1,0,'freqlim','0.1'),
(template_model_id,70,1,0,'freqlim','0.1'),
(template_model_id,71,1,0,'freqlim','0.1'),
(template_model_id,72,1,0,'freqlim','0.1'),
(template_model_id,73,1,0,'freqlim','0.1'),
(template_model_id,74,1,0,'freqlim','0.1'),
(template_model_id,75,1,0,'freqlim','0.1'),
(template_model_id,76,1,0,'freqlim','0.3'),
(template_model_id,77,1,0,'freqlim','0.3'),
(template_model_id,78,1,0,'freqlim','0.3'),
(template_model_id,79,1,0,'freqlim','0.3'),
(template_model_id,80,1,0,'freqlim','0.1'),
(template_model_id,81,1,0,'freqlim','0.1'),
(template_model_id,82,1,0,'freqlim','0.1'),
(template_model_id,83,1,0,'freqlim','0.1'),
(template_model_id,84,1,0,'freqlim','0.1'),
(template_model_id,85,1,0,'freqlim','0.1'),
(template_model_id,86,1,0,'freqlim','0.1'),
(template_model_id,87,1,0,'freqlim','0.1'),
(template_model_id,88,1,0,'freqlim','0.3'),
(template_model_id,89,1,0,'freqlim','0.3'),
(template_model_id,90,1,0,'freqlim','0.3'),
(template_model_id,91,1,0,'freqlim','0.3'),
(template_model_id,92,1,0,'freqlim','0.1'),
(template_model_id,93,1,0,'freqlim','0.1'),
(template_model_id,94,1,0,'freqlim','0.1'),
(template_model_id,95,1,0,'freqlim','0.1'),
(template_model_id,96,1,0,'freqlim','0.1'),
(template_model_id,97,1,0,'freqlim','0.3'),
(template_model_id,98,1,0,'freqlim','0.3'),
(template_model_id,99,1,0,'freqlim','0.3'),
(template_model_id,100,1,0,'freqlim','0.1'),
(template_model_id,101,1,0,'freqlim','0.1'),
(template_model_id,102,1,0,'freqlim','0.1'),
(template_model_id,103,1,0,'freqlim','0.1'),
(template_model_id,104,1,0,'freqlim','0.1'),
(template_model_id,105,1,0,'freqlim','0.1'),
(template_model_id,106,1,0,'freqlim','0.1'),
(template_model_id,107,1,0,'freqlim','0.1'),
(template_model_id,108,1,0,'freqlim','0.1'),
(template_model_id,109,1,0,'freqlim','0.1'),
(template_model_id,110,1,0,'freqlim','0.3'),
(template_model_id,111,1,0,'freqlim','0.3'),
(template_model_id,112,1,0,'freqlim','0.1'),
(template_model_id,113,1,0,'freqlim','0.1'),
(template_model_id,114,1,0,'freqlim','0.3'),
(template_model_id,115,1,0,'freqlim','0.3'),
(template_model_id,116,1,0,'freqlim','0.1'),
(template_model_id,117,1,0,'freqlim','0.1'),
(template_model_id,118,1,0,'freqlim','0.1'),
(template_model_id,119,1,0,'freqlim','0.1'),
(template_model_id,120,1,0,'freqlim','0.1'),
(template_model_id,121,1,0,'freqlim','0.1'),
(template_model_id,122,1,0,'freqlim','0.1'),
(template_model_id,123,1,0,'freqlim','0.1'),
(template_model_id,124,1,0,'freqlim','0.1'),
(template_model_id,125,1,0,'freqlim','0.1'),
(template_model_id,126,1,0,'freqlim','0.1'),
(template_model_id,127,1,0,'freqlim','0.1'),
(template_model_id,128,1,0,'freqlim','0.1'),
(template_model_id,129,1,0,'freqlim','0.1'),
(template_model_id,130,1,0,'freqlim','0.1'),
(template_model_id,131,1,0,'freqlim','0.1'),
(template_model_id,132,1,0,'freqlim','0.1'),
(template_model_id,133,1,0,'freqlim','0.1'),
(template_model_id,134,1,0,'freqlim','0.1'),
(template_model_id,135,1,0,'freqlim','0.1'),
(template_model_id,136,1,0,'freqlim','0.1'),
(template_model_id,137,1,0,'freqlim','0.1'),
(template_model_id,138,1,0,'freqlim','0.1'),
(template_model_id,139,1,0,'freqlim','0.1'),
(template_model_id,140,1,0,'freqlim','0.1'),
(template_model_id,141,1,0,'freqlim','0.1'),
(template_model_id,142,1,0,'freqlim','0.1'),
(template_model_id,1,0,0,'vargroup','Social network'),
(template_model_id,2,0,0,'vargroup','Voice'),
(template_model_id,3,0,0,'vargroup','Social network'),
(template_model_id,4,0,0,'vargroup','Social network'),
(template_model_id,5,0,0,'vargroup','SMS'),
(template_model_id,6,0,0,'vargroup','SMS'),
(template_model_id,7,0,0,'vargroup','SMS'),
(template_model_id,8,0,0,'vargroup','SMS'),
(template_model_id,9,0,0,'vargroup','Voice'),
(template_model_id,10,0,0,'vargroup','Voice'),
(template_model_id,11,0,0,'vargroup','Voice'),
(template_model_id,12,0,0,'vargroup','Voice'),
(template_model_id,13,0,0,'vargroup','Voice'),
(template_model_id,14,0,0,'vargroup','Voice'),
(template_model_id,15,0,0,'vargroup','Voice'),
(template_model_id,16,0,0,'vargroup','Voice'),
(template_model_id,17,0,0,'vargroup','Voice'),
(template_model_id,18,0,0,'vargroup','Voice'),
(template_model_id,19,0,0,'vargroup','Mobility'),
(template_model_id,20,0,0,'vargroup','Mobility'),
(template_model_id,21,0,0,'vargroup','Mobility'),
(template_model_id,22,0,0,'vargroup','Mobility'),
(template_model_id,23,0,0,'vargroup','Other call data'),
(template_model_id,24,0,0,'vargroup','Other call data'),
(template_model_id,25,0,0,'vargroup','Other call data'),
(template_model_id,26,0,0,'vargroup','Other call data'),
(template_model_id,27,0,0,'vargroup','Voice'),
(template_model_id,28,0,0,'vargroup','Voice'),
(template_model_id,29,0,0,'vargroup','Voice'),
(template_model_id,30,0,0,'vargroup','Voice'),
(template_model_id,31,0,0,'vargroup','Voice'),
(template_model_id,32,0,0,'vargroup','Voice'),
(template_model_id,33,0,0,'vargroup','Voice'),
(template_model_id,34,0,0,'vargroup','Voice'),
(template_model_id,35,0,0,'vargroup','Voice'),
(template_model_id,36,0,0,'vargroup','Voice'),
(template_model_id,37,0,0,'vargroup','Voice'),
(template_model_id,38,0,0,'vargroup','Voice'),
(template_model_id,39,0,0,'vargroup','Voice'),
(template_model_id,40,0,0,'vargroup','Voice'),
(template_model_id,41,0,0,'vargroup','Voice'),
(template_model_id,42,0,0,'vargroup','Voice'),
(template_model_id,43,0,0,'vargroup','Voice'),
(template_model_id,44,0,0,'vargroup','Voice'),
(template_model_id,45,0,0,'vargroup','Voice'),
(template_model_id,46,0,0,'vargroup','Voice'),
(template_model_id,47,0,0,'vargroup','SMS'),
(template_model_id,48,0,0,'vargroup','SMS'),
(template_model_id,49,0,0,'vargroup','SMS'),
(template_model_id,50,0,0,'vargroup','SMS'),
(template_model_id,51,0,0,'vargroup','SMS'),
(template_model_id,52,0,0,'vargroup','SMS'),
(template_model_id,53,0,0,'vargroup','SMS'),
(template_model_id,54,0,0,'vargroup','SMS'),
(template_model_id,55,0,0,'vargroup','SMS'),
(template_model_id,56,0,0,'vargroup','SMS'),
(template_model_id,57,0,0,'vargroup','SMS'),
(template_model_id,58,0,0,'vargroup','SMS'),
(template_model_id,59,0,0,'vargroup','Data'),
(template_model_id,60,0,0,'vargroup','Data'),
(template_model_id,61,0,0,'vargroup','Data'),
(template_model_id,62,0,0,'vargroup','Data'),
(template_model_id,63,0,0,'vargroup','Data'),
(template_model_id,64,0,0,'vargroup','Data'),
(template_model_id,65,0,0,'vargroup','Data'),
(template_model_id,66,0,0,'vargroup','Data'),
(template_model_id,67,0,0,'vargroup','Data'),
(template_model_id,68,0,0,'vargroup','Data'),
(template_model_id,69,0,0,'vargroup','Data'),
(template_model_id,70,0,0,'vargroup','Data'),
(template_model_id,71,0,0,'vargroup','Other call data'),
(template_model_id,72,0,0,'vargroup','Mobility'),
(template_model_id,73,0,0,'vargroup','Mobility'),
(template_model_id,74,0,0,'vargroup','Mobility'),
(template_model_id,75,0,0,'vargroup','Mobility'),
(template_model_id,76,0,0,'vargroup','Voice'),
(template_model_id,77,0,0,'vargroup','Voice'),
(template_model_id,78,0,0,'vargroup','Voice'),
(template_model_id,79,0,0,'vargroup','Voice'),
(template_model_id,80,0,0,'vargroup','Voice'),
(template_model_id,81,0,0,'vargroup','Voice'),
(template_model_id,82,0,0,'vargroup','Voice'),
(template_model_id,83,0,0,'vargroup','Voice'),
(template_model_id,84,0,0,'vargroup','Voice'),
(template_model_id,85,0,0,'vargroup','Voice'),
(template_model_id,86,0,0,'vargroup','Voice'),
(template_model_id,87,0,0,'vargroup','Voice'),
(template_model_id,88,0,0,'vargroup','SMS'),
(template_model_id,89,0,0,'vargroup','SMS'),
(template_model_id,90,0,0,'vargroup','SMS'),
(template_model_id,91,0,0,'vargroup','SMS'),
(template_model_id,92,0,0,'vargroup','SMS'),
(template_model_id,93,0,0,'vargroup','SMS'),
(template_model_id,94,0,0,'vargroup','SMS'),
(template_model_id,95,0,0,'vargroup','SMS'),
(template_model_id,96,0,0,'vargroup','Other call data'),
(template_model_id,97,0,0,'vargroup','Social network'),
(template_model_id,98,0,0,'vargroup','Social network'),
(template_model_id,99,0,0,'vargroup','Social network'),
(template_model_id,100,0,0,'vargroup','Social network'),
(template_model_id,101,0,0,'vargroup','Demographics'),
(template_model_id,102,0,0,'vargroup','Social network'),
(template_model_id,103,0,0,'vargroup','Social network'),
(template_model_id,104,0,0,'vargroup','Social network'),
(template_model_id,105,0,0,'vargroup','Social network'),
(template_model_id,106,0,0,'vargroup','Other'),
(template_model_id,107,0,0,'vargroup','Subscription details'),
(template_model_id,108,0,0,'vargroup','Subscription details'),
(template_model_id,109,0,0,'vargroup','Handset'),
(template_model_id,110,0,0,'vargroup','Social network'),
(template_model_id,111,0,0,'vargroup','Social network'),
(template_model_id,112,0,0,'vargroup','Social network'),
(template_model_id,113,0,0,'vargroup','Social network'),
(template_model_id,114,0,0,'vargroup','Social network'),
(template_model_id,115,0,0,'vargroup','Social network'),
(template_model_id,116,0,0,'vargroup','Social network'),
(template_model_id,117,0,0,'vargroup','Social network'),
(template_model_id,118,0,0,'vargroup','Other'),
(template_model_id,119,0,0,'vargroup','Social network'),
(template_model_id,120,0,0,'vargroup','Social network'),
(template_model_id,121,0,0,'vargroup','Handset'),
(template_model_id,122,0,0,'vargroup','Handset'),
(template_model_id,123,0,0,'vargroup','Handset'),
(template_model_id,124,0,0,'vargroup','Handset'),
(template_model_id,125,0,0,'vargroup','Handset'),
(template_model_id,126,0,0,'vargroup','cell_events'),
(template_model_id,127,0,0,'vargroup','cell_events'),
(template_model_id,128,0,0,'vargroup','cell_events'),
(template_model_id,129,0,0,'vargroup','cell_events'),
(template_model_id,130,0,0,'vargroup','cell_events'),
(template_model_id,131,0,0,'vargroup','cell_events'),
(template_model_id,132,0,0,'vargroup','cell_events'),
(template_model_id,133,0,0,'vargroup','cell_events'),
(template_model_id,134,0,0,'vargroup','cell_events'),
(template_model_id,135,0,0,'vargroup','cell_events'),
(template_model_id,136,0,0,'vargroup','geolocation'),
(template_model_id,137,0,0,'vargroup','geolocation'),
(template_model_id,138,0,0,'vargroup','geolocation'),
(template_model_id,139,0,0,'vargroup','geolocation'),
(template_model_id,140,0,0,'vargroup','geolocation'),
(template_model_id,141,0,0,'vargroup','geolocation'),
(template_model_id,142,0,0,'vargroup','geolocation'),
(template_model_id,1,1,0,'indname','isna'),
(template_model_id,1,1,0,'indcond','NULL'),
(template_model_id,1,2,0,'indname','iszero'),
(template_model_id,1,2,0,'indcond','0'),
(template_model_id,1,3,0,'indname','isna'),
(template_model_id,1,3,0,'indcond','NULL'),
(template_model_id,1,4,0,'indname','isneg'),
(template_model_id,1,4,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,2,1,0,'indname','isna'),
(template_model_id,2,1,0,'indcond','NULL'),
(template_model_id,2,2,0,'indname','isneg'),
(template_model_id,2,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,3,1,0,'indname','isna'),
(template_model_id,3,1,0,'indcond','NULL'),
(template_model_id,3,2,0,'indname','isneg'),
(template_model_id,3,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,4,1,0,'indname','isna'),
(template_model_id,4,1,0,'indcond','NULL'),
(template_model_id,4,2,0,'indname','isneg'),
(template_model_id,4,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,5,1,0,'indname','isna'),
(template_model_id,5,1,0,'indcond','NULL'),
(template_model_id,5,2,0,'indname','isneg'),
(template_model_id,5,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,6,1,0,'indname','isna'),
(template_model_id,6,1,0,'indcond','NULL'),
(template_model_id,6,2,0,'indname','isneg'),
(template_model_id,6,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,7,1,0,'indname','isna'),
(template_model_id,7,1,0,'indcond','NULL'),
(template_model_id,7,2,0,'indname','isneg'),
(template_model_id,7,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,8,1,0,'indname','isna'),
(template_model_id,8,1,0,'indcond','NULL'),
(template_model_id,8,2,0,'indname','isneg'),
(template_model_id,8,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,9,1,0,'indname','isna'),
(template_model_id,9,1,0,'indcond','NULL'),
(template_model_id,9,2,0,'indname','isneg'),
(template_model_id,9,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,10,1,0,'indname','isna'),
(template_model_id,10,1,0,'indcond','NULL'),
(template_model_id,10,2,0,'indname','isneg'),
(template_model_id,10,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,11,1,0,'indname','isna'),
(template_model_id,11,1,0,'indcond','NULL'),
(template_model_id,11,2,0,'indname','isneg'),
(template_model_id,11,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,12,1,0,'indname','isna'),
(template_model_id,12,1,0,'indcond','NULL'),
(template_model_id,12,2,0,'indname','isneg'),
(template_model_id,12,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,13,1,0,'indname','isna'),
(template_model_id,13,1,0,'indcond','NULL'),
(template_model_id,13,2,0,'indname','isneg'),
(template_model_id,13,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,14,1,0,'indname','isna'),
(template_model_id,14,1,0,'indcond','NULL'),
(template_model_id,14,2,0,'indname','isneg'),
(template_model_id,14,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,15,1,0,'indname','isna'),
(template_model_id,15,1,0,'indcond','NULL'),
(template_model_id,15,2,0,'indname','isneg'),
(template_model_id,15,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,16,1,0,'indname','isna'),
(template_model_id,16,1,0,'indcond','NULL'),
(template_model_id,16,2,0,'indname','isneg'),
(template_model_id,16,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,17,1,0,'indname','isna'),
(template_model_id,17,1,0,'indcond','NULL'),
(template_model_id,17,2,0,'indname','isneg'),
(template_model_id,17,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,18,1,0,'indname','isna'),
(template_model_id,18,1,0,'indcond','NULL'),
(template_model_id,18,2,0,'indname','isneg'),
(template_model_id,18,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,19,1,0,'indname','isna'),
(template_model_id,19,1,0,'indcond','NULL'),
(template_model_id,19,2,0,'indname','isneg'),
(template_model_id,19,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,20,1,0,'indname','isna'),
(template_model_id,20,1,0,'indcond','NULL'),
(template_model_id,20,2,0,'indname','isneg'),
(template_model_id,20,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,21,1,0,'indname','isna'),
(template_model_id,21,1,0,'indcond','NULL'),
(template_model_id,21,2,0,'indname','isneg'),
(template_model_id,21,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,22,1,0,'indname','isna'),
(template_model_id,22,1,0,'indcond','NULL'),
(template_model_id,22,2,0,'indname','isneg'),
(template_model_id,22,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,23,1,0,'indname','isna'),
(template_model_id,23,1,0,'indcond','NULL'),
(template_model_id,23,2,0,'indname','isneg'),
(template_model_id,23,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,24,1,0,'indname','isna'),
(template_model_id,24,1,0,'indcond','NULL'),
(template_model_id,24,2,0,'indname','isneg'),
(template_model_id,24,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,25,1,0,'indname','isna'),
(template_model_id,25,1,0,'indcond','NULL'),
(template_model_id,25,2,0,'indname','isneg'),
(template_model_id,25,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,26,1,0,'indname','isna'),
(template_model_id,26,1,0,'indcond','NULL'),
(template_model_id,26,2,0,'indname','isneg'),
(template_model_id,26,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,27,1,0,'indname','isna'),
(template_model_id,27,1,0,'indcond','NULL'),
(template_model_id,27,2,0,'indname','isneg'),
(template_model_id,27,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,28,1,0,'indname','isna'),
(template_model_id,28,1,0,'indcond','NULL'),
(template_model_id,28,2,0,'indname','isneg'),
(template_model_id,28,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,29,1,0,'indname','isna'),
(template_model_id,29,1,0,'indcond','NULL'),
(template_model_id,29,2,0,'indname','isneg'),
(template_model_id,29,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,30,1,0,'indname','isna'),
(template_model_id,30,1,0,'indcond','NULL'),
(template_model_id,30,2,0,'indname','isneg'),
(template_model_id,30,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,31,1,0,'indname','isna'),
(template_model_id,31,1,0,'indcond','NULL'),
(template_model_id,31,2,0,'indname','isneg'),
(template_model_id,31,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,32,1,0,'indname','isna'),
(template_model_id,32,1,0,'indcond','NULL'),
(template_model_id,32,2,0,'indname','isneg'),
(template_model_id,32,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,33,1,0,'indname','isna'),
(template_model_id,33,1,0,'indcond','NULL'),
(template_model_id,33,2,0,'indname','isneg'),
(template_model_id,33,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,34,1,0,'indname','isna'),
(template_model_id,34,1,0,'indcond','NULL'),
(template_model_id,34,2,0,'indname','isneg'),
(template_model_id,34,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,35,1,0,'indname','isna'),
(template_model_id,35,1,0,'indcond','NULL'),
(template_model_id,35,2,0,'indname','isneg'),
(template_model_id,35,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,36,1,0,'indname','isna'),
(template_model_id,36,1,0,'indcond','NULL'),
(template_model_id,36,2,0,'indname','isneg'),
(template_model_id,36,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,37,1,0,'indname','isna'),
(template_model_id,37,1,0,'indcond','NULL'),
(template_model_id,37,2,0,'indname','isneg'),
(template_model_id,37,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,38,1,0,'indname','isna'),
(template_model_id,38,1,0,'indcond','NULL'),
(template_model_id,38,2,0,'indname','isneg'),
(template_model_id,38,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,39,1,0,'indname','isna'),
(template_model_id,39,1,0,'indcond','NULL'),
(template_model_id,39,2,0,'indname','isneg'),
(template_model_id,39,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,40,1,0,'indname','isna'),
(template_model_id,40,1,0,'indcond','NULL'),
(template_model_id,40,2,0,'indname','isneg'),
(template_model_id,40,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,41,1,0,'indname','isna'),
(template_model_id,41,1,0,'indcond','NULL'),
(template_model_id,41,2,0,'indname','isneg'),
(template_model_id,41,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,42,1,0,'indname','isna'),
(template_model_id,42,1,0,'indcond','NULL'),
(template_model_id,42,2,0,'indname','isneg'),
(template_model_id,42,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,43,1,0,'indname','isna'),
(template_model_id,43,1,0,'indcond','NULL'),
(template_model_id,43,2,0,'indname','isneg'),
(template_model_id,43,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,44,1,0,'indname','isna'),
(template_model_id,44,1,0,'indcond','NULL'),
(template_model_id,44,2,0,'indname','isneg'),
(template_model_id,44,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,45,1,0,'indname','isna'),
(template_model_id,45,1,0,'indcond','NULL'),
(template_model_id,45,2,0,'indname','isneg'),
(template_model_id,45,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,46,1,0,'indname','isna'),
(template_model_id,46,1,0,'indcond','NULL'),
(template_model_id,46,2,0,'indname','isneg'),
(template_model_id,46,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,47,1,0,'indname','isna'),
(template_model_id,47,1,0,'indcond','NULL'),
(template_model_id,47,2,0,'indname','isneg'),
(template_model_id,47,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,48,1,0,'indname','isna'),
(template_model_id,48,1,0,'indcond','NULL'),
(template_model_id,48,2,0,'indname','isneg'),
(template_model_id,48,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,49,1,0,'indname','isna'),
(template_model_id,49,1,0,'indcond','NULL'),
(template_model_id,49,2,0,'indname','isneg'),
(template_model_id,49,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,50,1,0,'indname','isna'),
(template_model_id,50,1,0,'indcond','NULL'),
(template_model_id,50,2,0,'indname','isneg'),
(template_model_id,50,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,51,1,0,'indname','isna'),
(template_model_id,51,1,0,'indcond','NULL'),
(template_model_id,51,2,0,'indname','isneg'),
(template_model_id,51,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,52,1,0,'indname','isna'),
(template_model_id,52,1,0,'indcond','NULL'),
(template_model_id,52,2,0,'indname','isneg'),
(template_model_id,52,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,53,1,0,'indname','isna'),
(template_model_id,53,1,0,'indcond','NULL'),
(template_model_id,53,2,0,'indname','isneg'),
(template_model_id,53,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,54,1,0,'indname','isna'),
(template_model_id,54,1,0,'indcond','NULL'),
(template_model_id,54,2,0,'indname','isneg'),
(template_model_id,54,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,55,1,0,'indname','isna'),
(template_model_id,55,1,0,'indcond','NULL'),
(template_model_id,55,2,0,'indname','isneg'),
(template_model_id,55,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,56,1,0,'indname','isna'),
(template_model_id,56,1,0,'indcond','NULL'),
(template_model_id,56,2,0,'indname','isneg'),
(template_model_id,56,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,57,1,0,'indname','isna'),
(template_model_id,57,1,0,'indcond','NULL'),
(template_model_id,57,2,0,'indname','isneg'),
(template_model_id,57,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,58,1,0,'indname','isna'),
(template_model_id,58,1,0,'indcond','NULL'),
(template_model_id,58,2,0,'indname','isneg'),
(template_model_id,58,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,59,1,0,'indname','isna'),
(template_model_id,59,1,0,'indcond','NULL'),
(template_model_id,59,2,0,'indname','isneg'),
(template_model_id,59,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,60,1,0,'indname','isna'),
(template_model_id,60,1,0,'indcond','NULL'),
(template_model_id,60,2,0,'indname','isneg'),
(template_model_id,60,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,61,1,0,'indname','isna'),
(template_model_id,61,1,0,'indcond','NULL'),
(template_model_id,61,2,0,'indname','isneg'),
(template_model_id,61,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,62,1,0,'indname','isna'),
(template_model_id,62,1,0,'indcond','NULL'),
(template_model_id,62,2,0,'indname','isneg'),
(template_model_id,62,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,63,1,0,'indname','isna'),
(template_model_id,63,1,0,'indcond','NULL'),
(template_model_id,63,2,0,'indname','isneg'),
(template_model_id,63,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,64,1,0,'indname','isna'),
(template_model_id,64,1,0,'indcond','NULL'),
(template_model_id,64,2,0,'indname','isneg'),
(template_model_id,64,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,65,1,0,'indname','isna'),
(template_model_id,65,1,0,'indcond','NULL'),
(template_model_id,65,2,0,'indname','isneg'),
(template_model_id,65,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,66,1,0,'indname','isna'),
(template_model_id,66,1,0,'indcond','NULL'),
(template_model_id,66,2,0,'indname','isneg'),
(template_model_id,66,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,67,1,0,'indname','isna'),
(template_model_id,67,1,0,'indcond','NULL'),
(template_model_id,67,2,0,'indname','isneg'),
(template_model_id,67,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,68,1,0,'indname','isna'),
(template_model_id,68,1,0,'indcond','NULL'),
(template_model_id,68,2,0,'indname','isneg'),
(template_model_id,68,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,69,1,0,'indname','isna'),
(template_model_id,69,1,0,'indcond','NULL'),
(template_model_id,69,2,0,'indname','isneg'),
(template_model_id,69,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,70,1,0,'indname','isna'),
(template_model_id,70,1,0,'indcond','NULL'),
(template_model_id,70,2,0,'indname','isneg'),
(template_model_id,70,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,71,1,0,'indname','isna'),
(template_model_id,71,1,0,'indcond','NULL'),
(template_model_id,71,2,0,'indname','isneg'),
(template_model_id,71,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,72,1,0,'indname','isna'),
(template_model_id,72,1,0,'indcond','NULL'),
(template_model_id,72,2,0,'indname','isneg'),
(template_model_id,72,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,73,1,0,'indname','isna'),
(template_model_id,73,1,0,'indcond','NULL'),
(template_model_id,73,2,0,'indname','isneg'),
(template_model_id,73,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,74,1,0,'indname','isna'),
(template_model_id,74,1,0,'indcond','NULL'),
(template_model_id,74,2,0,'indname','isneg'),
(template_model_id,74,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,75,1,0,'indname','isna'),
(template_model_id,75,1,0,'indcond','NULL'),
(template_model_id,75,2,0,'indname','isneg'),
(template_model_id,75,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,76,1,0,'indname','isna'),
(template_model_id,76,1,0,'indcond','NULL'),
(template_model_id,76,2,0,'indname','isneg'),
(template_model_id,76,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,77,1,0,'indname','isna'),
(template_model_id,77,1,0,'indcond','NULL'),
(template_model_id,77,2,0,'indname','isneg'),
(template_model_id,77,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,78,1,0,'indname','isna'),
(template_model_id,78,1,0,'indcond','NULL'),
(template_model_id,78,2,0,'indname','isneg'),
(template_model_id,78,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,79,1,0,'indname','isna'),
(template_model_id,79,1,0,'indcond','NULL'),
(template_model_id,79,2,0,'indname','isneg'),
(template_model_id,79,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,80,1,0,'indname','isna'),
(template_model_id,80,1,0,'indcond','NULL'),
(template_model_id,80,2,0,'indname','isneg'),
(template_model_id,80,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,81,1,0,'indname','isna'),
(template_model_id,81,1,0,'indcond','NULL'),
(template_model_id,81,2,0,'indname','isneg'),
(template_model_id,81,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,82,1,0,'indname','isna'),
(template_model_id,82,1,0,'indcond','NULL'),
(template_model_id,82,2,0,'indname','isneg'),
(template_model_id,82,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,83,1,0,'indname','isna'),
(template_model_id,83,1,0,'indcond','NULL'),
(template_model_id,83,2,0,'indname','isneg'),
(template_model_id,83,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,84,1,0,'indname','isna'),
(template_model_id,84,1,0,'indcond','NULL'),
(template_model_id,84,2,0,'indname','isneg'),
(template_model_id,84,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,85,1,0,'indname','isna'),
(template_model_id,85,1,0,'indcond','NULL'),
(template_model_id,85,2,0,'indname','isneg'),
(template_model_id,85,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,86,1,0,'indname','isna'),
(template_model_id,86,1,0,'indcond','NULL'),
(template_model_id,86,2,0,'indname','isneg'),
(template_model_id,86,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,87,1,0,'indname','isna'),
(template_model_id,87,1,0,'indcond','NULL'),
(template_model_id,87,2,0,'indname','isneg'),
(template_model_id,87,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,88,1,0,'indname','isna'),
(template_model_id,88,1,0,'indcond','NULL'),
(template_model_id,88,2,0,'indname','isneg'),
(template_model_id,88,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,89,1,0,'indname','isna'),
(template_model_id,89,1,0,'indcond','NULL'),
(template_model_id,89,2,0,'indname','isneg'),
(template_model_id,89,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,90,1,0,'indname','isna'),
(template_model_id,90,1,0,'indcond','NULL'),
(template_model_id,90,2,0,'indname','isneg'),
(template_model_id,90,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,91,1,0,'indname','isna'),
(template_model_id,91,1,0,'indcond','NULL'),
(template_model_id,91,2,0,'indname','isneg'),
(template_model_id,91,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,92,1,0,'indname','isna'),
(template_model_id,92,1,0,'indcond','NULL'),
(template_model_id,92,2,0,'indname','isneg'),
(template_model_id,92,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,93,1,0,'indname','isna'),
(template_model_id,93,1,0,'indcond','NULL'),
(template_model_id,93,2,0,'indname','isneg'),
(template_model_id,93,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,94,1,0,'indname','isna'),
(template_model_id,94,1,0,'indcond','NULL'),
(template_model_id,94,2,0,'indname','isneg'),
(template_model_id,94,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,95,1,0,'indname','isna'),
(template_model_id,95,1,0,'indcond','NULL'),
(template_model_id,95,2,0,'indname','isneg'),
(template_model_id,95,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,96,1,0,'indname','isna'),
(template_model_id,96,1,0,'indcond','NULL'),
(template_model_id,96,2,0,'indname','isneg'),
(template_model_id,96,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,97,1,0,'indname','isna'),
(template_model_id,97,1,0,'indcond','NULL'),
(template_model_id,97,2,0,'indname','isneg'),
(template_model_id,97,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,98,1,0,'indname','isna'),
(template_model_id,98,1,0,'indcond','NULL'),
(template_model_id,98,2,0,'indname','isneg'),
(template_model_id,98,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,99,1,0,'indname','isna'),
(template_model_id,99,1,0,'indcond','NULL'),
(template_model_id,99,2,0,'indname','isneg'),
(template_model_id,99,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,100,1,0,'indname','isna'),
(template_model_id,100,1,0,'indcond','NULL'),
(template_model_id,100,2,0,'indname','isneg'),
(template_model_id,100,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,101,1,0,'indname','naorzero'),
(template_model_id,101,1,0,'indcond','NULL or 0'),
(template_model_id,101,2,0,'indname','isneg'),
(template_model_id,101,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,102,1,0,'indname','isna'),
(template_model_id,102,1,0,'indcond','NULL'),
(template_model_id,102,2,0,'indname','isneg'),
(template_model_id,102,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,103,1,0,'indname','isna'),
(template_model_id,103,1,0,'indcond','NULL'),
(template_model_id,103,2,0,'indname','isneg'),
(template_model_id,103,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,104,1,0,'indname','isna'),
(template_model_id,104,1,0,'indcond','NULL'),
(template_model_id,104,2,0,'indname','isneg'),
(template_model_id,104,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,105,1,0,'indname','isna'),
(template_model_id,105,1,0,'indcond','NULL'),
(template_model_id,105,2,0,'indname','isneg'),
(template_model_id,105,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,106,1,0,'indname','isna'),
(template_model_id,106,1,0,'indcond','NULL'),
(template_model_id,106,2,0,'indname','isneg'),
(template_model_id,106,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,107,1,0,'indname','isna'),
(template_model_id,107,1,0,'indcond','NULL'),
(template_model_id,107,2,0,'indname','isneg'),
(template_model_id,107,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,108,1,0,'indname','isna'),
(template_model_id,108,1,0,'indcond','NULL'),
(template_model_id,108,2,0,'indname','isneg'),
(template_model_id,108,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,109,1,0,'indname','isna'),
(template_model_id,109,1,0,'indcond','NULL'),
(template_model_id,109,2,0,'indname','isneg'),
(template_model_id,109,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,110,1,0,'indname','isna'),
(template_model_id,110,1,0,'indcond','NULL'),
(template_model_id,110,2,0,'indname','isneg'),
(template_model_id,110,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,111,1,0,'indname','isna'),
(template_model_id,111,1,0,'indcond','NULL'),
(template_model_id,111,2,0,'indname','isneg'),
(template_model_id,111,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,112,1,0,'indname','isna'),
(template_model_id,112,1,0,'indcond','NULL'),
(template_model_id,112,2,0,'indname','isneg'),
(template_model_id,112,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,113,1,0,'indname','isna'),
(template_model_id,113,1,0,'indcond','NULL'),
(template_model_id,113,2,0,'indname','isneg'),
(template_model_id,113,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,114,1,0,'indname','isna'),
(template_model_id,114,1,0,'indcond','NULL'),
(template_model_id,114,2,0,'indname','isneg'),
(template_model_id,114,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,115,1,0,'indname','isna'),
(template_model_id,115,1,0,'indcond','NULL'),
(template_model_id,115,2,0,'indname','isneg'),
(template_model_id,115,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,116,1,0,'indname','isna'),
(template_model_id,116,1,0,'indcond','NULL'),
(template_model_id,116,2,0,'indname','isneg'),
(template_model_id,116,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,117,1,0,'indname','isna'),
(template_model_id,117,1,0,'indcond','NULL'),
(template_model_id,117,2,0,'indname','isneg'),
(template_model_id,117,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,118,1,0,'indname','isna'),
(template_model_id,118,1,0,'indcond','NULL'),
(template_model_id,118,2,0,'indname','isneg'),
(template_model_id,118,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,119,1,0,'indname','isna'),
(template_model_id,119,1,0,'indcond','NULL'),
(template_model_id,119,2,0,'indname','isneg'),
(template_model_id,119,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,120,1,0,'indname','isna'),
(template_model_id,120,1,0,'indcond','NULL'),
(template_model_id,120,2,0,'indname','isneg'),
(template_model_id,120,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,121,1,0,'indname','isna'),
(template_model_id,121,1,0,'indcond','NULL'),
(template_model_id,121,2,0,'indname','isneg'),
(template_model_id,121,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,122,1,0,'indname','isna'),
(template_model_id,122,1,0,'indcond','NULL'),
(template_model_id,122,2,0,'indname','isneg'),
(template_model_id,122,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,123,1,0,'indname','isna'),
(template_model_id,123,1,0,'indcond','NULL'),
(template_model_id,123,2,0,'indname','isneg'),
(template_model_id,123,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,124,1,0,'indname','isna'),
(template_model_id,124,1,0,'indcond','NULL'),
(template_model_id,124,2,0,'indname','isneg'),
(template_model_id,124,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,125,1,0,'indname','isna'),
(template_model_id,125,1,0,'indcond','NULL'),
(template_model_id,125,2,0,'indname','isneg'),
(template_model_id,125,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,126,1,0,'indname','isna'),
(template_model_id,126,1,0,'indcond','NULL'),
(template_model_id,126,2,0,'indname','isneg'),
(template_model_id,126,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,127,1,0,'indname','isna'),
(template_model_id,127,1,0,'indcond','NULL'),
(template_model_id,127,2,0,'indname','isneg'),
(template_model_id,127,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,128,1,0,'indname','isna'),
(template_model_id,128,1,0,'indcond','NULL'),
(template_model_id,128,2,0,'indname','isneg'),
(template_model_id,128,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,129,1,0,'indname','isna'),
(template_model_id,129,1,0,'indcond','NULL'),
(template_model_id,129,2,0,'indname','isneg'),
(template_model_id,129,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,130,1,0,'indname','isna'),
(template_model_id,130,1,0,'indcond','NULL'),
(template_model_id,130,2,0,'indname','isneg'),
(template_model_id,130,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,131,1,0,'indname','isna'),
(template_model_id,131,1,0,'indcond','NULL'),
(template_model_id,131,2,0,'indname','isneg'),
(template_model_id,131,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,132,1,0,'indname','isna'),
(template_model_id,132,1,0,'indcond','NULL'),
(template_model_id,132,2,0,'indname','isneg'),
(template_model_id,132,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,133,1,0,'indname','isna'),
(template_model_id,133,1,0,'indcond','NULL'),
(template_model_id,133,2,0,'indname','isneg'),
(template_model_id,133,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,134,1,0,'indname','isna'),
(template_model_id,134,1,0,'indcond','NULL'),
(template_model_id,134,2,0,'indname','isneg'),
(template_model_id,134,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,135,1,0,'indname','isna'),
(template_model_id,135,1,0,'indcond','NULL'),
(template_model_id,135,2,0,'indname','isneg'),
(template_model_id,135,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,136,1,0,'indname','isna'),
(template_model_id,136,1,0,'indcond','NULL'),
(template_model_id,136,2,0,'indname','isneg'),
(template_model_id,136,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,137,1,0,'indname','isna'),
(template_model_id,137,1,0,'indcond','NULL'),
(template_model_id,137,2,0,'indname','isneg'),
(template_model_id,137,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,138,1,0,'indname','isna'),
(template_model_id,138,1,0,'indcond','NULL'),
(template_model_id,138,2,0,'indname','isneg'),
(template_model_id,138,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,139,1,0,'indname','isna'),
(template_model_id,139,1,0,'indcond','NULL'),
(template_model_id,139,2,0,'indname','isneg'),
(template_model_id,139,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,140,1,0,'indname','isna'),
(template_model_id,140,1,0,'indcond','NULL'),
(template_model_id,140,2,0,'indname','isneg'),
(template_model_id,140,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,141,1,0,'indname','isna'),
(template_model_id,141,1,0,'indcond','NULL'),
(template_model_id,141,2,0,'indname','isneg'),
(template_model_id,141,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,142,1,0,'indname','isna'),
(template_model_id,142,1,0,'indcond','NULL'),
(template_model_id,142,2,0,'indname','isneg'),
(template_model_id,142,2,0,'indcond','[-Inf,0,0,1]'),
(template_model_id,143,0,0,'vargroup','Demographics'),
(template_model_id,144,0,0,'vargroup','Demographics'),
(template_model_id,145,0,0,'vargroup','Handset'),
(template_model_id,146,0,0,'vargroup','Demographics'),
(template_model_id,147,0,0,'vargroup','Other'),
(template_model_id,148,0,0,'vargroup','Subscription details'),
(template_model_id,149,0,0,'vargroup','Social network'),
(template_model_id,150,0,0,'vargroup','Social network'),
(template_model_id,151,0,0,'vargroup','Demographics'),
(template_model_id,152,0,0,'vargroup','Subscription details'),
(template_model_id,153,0,0,'vargroup','Subscription details'),
(template_model_id,154,0,0,'vargroup','Demographics'),
(template_model_id,145,0,0,'cat_th','0.02'),
(template_model_id,155,1,0,'interactname','mr_ratio.X.voicecount'),
(template_model_id,155,2,0,'interactname','alpha.X.alias_count'),
(template_model_id,155,3,0,'interactname','alpha.X.mr_ratio'),
(template_model_id,155,4,0,'interactname','contr_length.X.voicecount'),
(template_model_id,155,5,0,'interactname','contr_length.X.smscount'),
(template_model_id,155,6,0,'interactname','alias_count.X.mr_ratio'),
(template_model_id,155,7,0,'interactname','kshell.X.mr_ratio'),
(template_model_id,155,8,0,'interactname','c.X.mr_ratio'),
(template_model_id,155,9,0,'interactname','c.X.k'),
(template_model_id,155,10,0,'interactname','week_entropy.X.alias_count'),
(template_model_id,155,1,0,'interactcond','mr_ratio * voicecount'),
(template_model_id,155,2,0,'interactcond','alpha * alias_count'),
(template_model_id,155,3,0,'interactcond','alpha * mr_ratio'),
(template_model_id,155,4,0,'interactcond','contr_length * voicecount'),
(template_model_id,155,5,0,'interactcond','contr_length * smscount'),
(template_model_id,155,6,0,'interactcond','alias_count * mr_ratio'),
(template_model_id,155,7,0,'interactcond','kshell * mr_ratio'),
(template_model_id,155,8,0,'interactcond','c * mr_ratio'),
(template_model_id,155,9,0,'interactcond','c * k'),
(template_model_id,155,10,0,'interactcond','week_entropy * alias_count'),
(template_model_id,155,1,0,'interactgroup','Interactions'),
(template_model_id,155,2,0,'interactgroup','Interactions'),
(template_model_id,155,3,0,'interactgroup','Interactions'),
(template_model_id,155,4,0,'interactgroup','Interactions'),
(template_model_id,155,5,0,'interactgroup','Interactions'),
(template_model_id,155,6,0,'interactgroup','Interactions'),
(template_model_id,155,7,0,'interactgroup','Interactions'),
(template_model_id,155,8,0,'interactgroup','Interactions'),
(template_model_id,155,9,0,'interactgroup','Interactions'),
(template_model_id,155,10,0,'interactgroup','Interactions');
END; 

$BODY$
LANGUAGE 'plpgsql' VOLATILE; 

ALTER FUNCTION work.insert_churn_postpaid_template_model() OWNER TO xsl; 
  
SELECT * FROM work.insert_churn_postpaid_template_model();



DROP FUNCTION work.initialize_lda(integer, text, integer, text, integer);

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
  
  
------------------Move the hard coded parameters to a separate table: work.scoring_muc_parameters----------------
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
 
  
DROP FUNCTION work.initialize_wf_muc_common(text[], boolean[], text, date, integer, text, integer, text, integer, text, integer, text[], text[], boolean);

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

 
DROP FUNCTION work.initialize_churn_inactivity(integer, integer, boolean, boolean);  

  
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

DROP FUNCTION work.initialize_churn_postpaid(integer, integer, boolean);


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

DROP FUNCTION work.initialize_zero_day_prediction(integer, integer, boolean, boolean);

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

DROP FUNCTION work.initialize_product(integer, integer,boolean);


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

  
---------------------ARPU and Tenure segmentation----------------------------

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


DROP FUNCTION work.insert_campaign_target_list_churn_inactivity(in_mod_job_id integer, in_target_id integer, ctrl_grp_size double precision, target_limit text, blocklist_id text);


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

  
DROP FUNCTION work.insert_campaign_target_list_churn_postpaid(integer, integer, double precision, text, text);

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

  
DROP FUNCTION work.insert_campaign_target_list_sni(
  in_mod_job_id integer, 
  in_target_id integer, 
  target_limit text, 
  ctrl_grp_size double precision, 
  descvar text, 
  order_name text, 
  min_val double precision, 
  max_val double precision,
  blocklist_id text
);  

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

  
ALTER TABLE work.monthly_arpu 
ADD COLUMN segmentation TEXT DEFAULT NULL::TEXT;


ALTER TABLE work.monthly_arpu
ALTER COLUMN segmentation DROP DEFAULT;
  
  
  
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

DROP FUNCTION IF EXISTS core.stack_to_matrix_batch(in_mod_job_id integer)