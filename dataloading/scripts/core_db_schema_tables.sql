-- Copyright (c) 2012 Comptel Oy. All rights reserved.  
-- This software is the proprietary information of Comptel Oy. 
-- Use is subject to license terms. --


-- IF schema is NOT created with .sh scripts provided with installation package
-- remove these comments, as database, role and language must exist before this script starts
-- create database xsl; 
-- create role xsl;
-- create procedural language plpgsql;

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

select install_check('core', 'partition_date_tables');

DROP FUNCTION install_check(text, text);


create schema tmp;
alter schema tmp owner to xsl;

create schema data;
alter schema data owner to xsl;

CREATE SCHEMA aliases;
ALTER SCHEMA aliases OWNER TO xsl;

CREATE SCHEMA core;
ALTER SCHEMA core OWNER TO xsl;


--Partitioning auxiliary tables

--For storing partition configurations for tables partitioned by date/timestamp column.
CREATE TABLE core.partition_date_tables (
  table_name text NOT NULL,
  compresslevel int, --NULL for no compression,
  retention_dates int DEFAULT 360,
  cleanup boolean DEFAULT TRUE, --TRUE for the table to be included in automatic cleanup
  PRIMARY KEY(table_name)
) DISTRIBUTED BY (table_name);
ALTER TABLE core.partition_date_tables OWNER TO xsl;

--For storing partition configurations for tables partitioned by period (YYYYWW) column.
CREATE TABLE core.partition_period_tables (
  table_name text NOT NULL,
  compresslevel int, --NULL for no compression,
  retention_dates int DEFAULT 360,
  cleanup boolean DEFAULT TRUE, --TRUE for the table to be included in automatic cleanup
  PRIMARY KEY(table_name)
) DISTRIBUTED BY (table_name);
ALTER TABLE core.partition_period_tables OWNER TO xsl;

--For storing the partition create times for tables that are partitioned by a date/timestamp field
CREATE TABLE core.partition_date_create_times (
  table_name text NOT NULL,
  data_date date NOT NULL,
  time_created timestamp NOT NULL,
  cleanup boolean NOT NULL,
  PRIMARY KEY(table_name, data_date)
) DISTRIBUTED BY (table_name, data_date);
ALTER TABLE core.partition_date_create_times OWNER TO xsl;

--For storing the partition create times for tables that are partitioned by a period field (YYYYWW)
CREATE TABLE core.partition_period_create_times (
  table_name text NOT NULL,
  period integer NOT NULL,
  time_created timestamp NOT NULL,
  cleanup boolean NOT NULL,
  PRIMARY KEY(table_name, period)
) DISTRIBUTED BY (table_name, period);
ALTER TABLE core.partition_period_create_times OWNER TO xsl;

-- data storage tables

-- An intermediate storage table used to define subs on-/off-net status between given dates
CREATE TABLE aliases.aliases_updated (
  string_id text NOT NULL,
  alias_id integer,
  on_black_list smallint NOT NULL DEFAULT 0, 
  in_out_network smallint NOT NULL DEFAULT 0,
  net_id integer,
  prev_net_id integer,
  validity date ) 
WITH (appendonly=TRUE, compresslevel=7)
DISTRIBUTED BY (alias_id);
ALTER TABLE aliases.aliases_updated OWNER TO xsl;


-- Includes all the network names that are found from the data (CRM/CDR/TOPUP)
create table aliases.network_list(
 net_id serial,
 net_name text,
 net_description text
) distributed by (net_id); 
ALTER TABLE aliases.network_list OWNER TO xsl;


-- data.in_split_weekly contains the weekly aggregates of network data
CREATE TABLE data.in_split_weekly
(
  alias_a integer NOT NULL,
  alias_b integer NOT NULL,
  cdr_w double precision,
  sms_w double precision,
  mms_w double precision,
  video_w double precision,
  --data_usage_w double precision,
  v_c double precision,
  v_s double precision,
  v_c_week double precision,
  v_s_week double precision,
  v_c_day double precision,
  v_s_day double precision,
  v_c_eve double precision,
  v_s_eve double precision,
  sms_c_week double precision,
  sms_c_day double precision,
  sms_c_eve double precision,
  data_usage_day double precision,
  data_usage_weekday double precision,
  data_usage_eve double precision,
  weekly_data_usage double precision,
  weekly_data_usage_cost double precision,
  weekly_cost double precision,
  period integer NOT NULL
)
WITH (appendonly=true, compresslevel=5)
distributed by (alias_a)
PARTITION BY RANGE (period) (PARTITION "187401" START (187401) END (187402));
ALTER TABLE data.in_split_weekly OWNER TO xsl;

INSERT INTO core.partition_period_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.in_split_weekly', 5, 360);

-- data.in_split_aggregates contains extra aggregates, used in addition to data.in_split_weekly in modelling
CREATE TABLE data.in_split_aggregates
(
  alias_id integer NOT NULL,
  in_out_network smallint,
  voicecount integer,
  voicesum double precision,
  voicecountweekday integer,
  voicesumweekday double precision,
  voicecountday integer,
  voicesumday double precision,
  voicecountevening integer,
  voicesumevening double precision,
  smscount integer,
  smscountweekday integer,
  smscountday integer,
  smscountevening integer,
  mc_alias_count integer,
  rc_alias_count integer,
  rc_voicecount integer,
  rc_smscount integer,
  data_usage_sumday double precision,
  data_usage_sumevening double precision,
  data_usage_sumweekday double precision,
  weekly_data_usage_sum double precision,  
  weekly_data_usage_cost_sum double precision, 
  weekly_cost_sum double precision,
  period integer NOT NULL
) 
WITH (appendonly=true, compresslevel=5)
distributed by (alias_id)
PARTITION BY RANGE (period) (PARTITION "187401" START (187401) END (187402));
ALTER TABLE data.in_split_aggregates OWNER TO xsl;

INSERT INTO core.partition_period_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.in_split_aggregates', 5, 360);

-- Contains weekly aggregated CDR data. It used in the data loading phase.
CREATE TABLE data.call_types_weekly
(
  alias_id integer,
  direction character(1),
  monday date,
  call_type smallint,
  max_call_time timestamp without time zone,
  row_count integer,
  day_count smallint,
  neigh_count integer,
  sum_call_duration integer,
  sum_call_cost double precision,
  cell_id_count integer,
  sum_parameter1 bigint,
  sum_parameter2 bigint,
  sum_parameter3 bigint,
  sum_parameter4 bigint
)
WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (monday) (PARTITION "1874-04-25" START ('1874-04-25'::date) END ('1874-04-26'::date));
ALTER TABLE data.call_types_weekly OWNER TO xsl;

INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.call_types_weekly', 7, 360);

-- CRM data tables ----------------------------------------

-- Used by template data loading workflow (CRM part) and defined based on the data guide
CREATE TABLE tmp.crm
(
  source_file text,           -- name of the flat file that has been uploaded to the table
  -- All the rest fields are defined in the data guide. 
  csp_internal_subs_id text,
  msisdn text,
  subscription_type text,
  payment_type text,
  gender text,
  birth_date text,
  switch_on_date text,
  switch_off_date text,
  churn_notification_date text,
  binding_contract text,
  binding_contract_start_date text,
  binding_contract_length text,
  subscriber_value text,
  subscriber_segment text,
  handset_model text,
  handset_begin_date text,
  tariff_plan text,
  zip text,
  country text,
  language text,
  churn_score text,
  "timestamp" text
) WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY (msisdn);
ALTER TABLE tmp.crm OWNER TO xsl;


-- Used by template data loading workflow (CRM part)
CREATE TABLE tmp.crm_err
(
  cmdtime timestamp with time zone,
  relname text,
  filename text,
  linenum integer,
  bytenum integer,
  errmsg text,
  rawdata text,
  rawbytes bytea
)
WITH (OIDS=FALSE)
DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.crm_err OWNER TO xsl;


-- Used by template data loading workflow (CRM part) and defined based on the data guide
CREATE TABLE tmp.crm_staging
(
  source_file text,           -- Name of the flat file that has been uploaded to the table
  -- All the rest fields are defined in the data guide. Typically very customer/data spesific in practice.
  csp_internal_subs_id text,
  string_id text,             -- In the data guide, this is a MSISDN but renamed to SL links internal representation
  subscription_type text,
  payment_type text,
  gender text,
  birth_date date,
  switch_on_date date,
  switch_off_date date,
  churn_notification_date date,
  binding_contract text,
  binding_contract_start_date date,
  binding_contract_length integer,
  subscriber_value double precision,
  subscriber_segment text,
  handset_model text,
  handset_begin_date date,
  tariff_plan text,
  zip text,
  country text,
  language text,
  churn_score double precision,
  "timestamp" date
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY (string_id);
ALTER TABLE tmp.crm_staging OWNER TO xsl;


-- Used by template data loading workflow (CRM part) and defined based on the data guide
CREATE TABLE data.crm
(
  source_file text,
  alias_id integer,   -- This is actually msisdn
  -- All the rest fields are defined in the data guide. Typically very customer/data spesific in practice.
  csp_internal_subs_id text,
  --string_id text,
  subscription_type text,
  payment_type text,
  gender text,
  birth_date date,
  switch_on_date date,
  switch_off_date date,
  churn_notification_date date,
  binding_contract text,
  binding_contract_start_date date,
  binding_contract_length integer,
  subscriber_value double precision,
  subscriber_segment text,
  handset_model text,
  handset_begin_date date,
  tariff_plan text,
  zip text,
  country text,
  language text,
  churn_score double precision,
  "timestamp" date
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE ("timestamp") (PARTITION "1874-04-25" START ('1874-04-25'::date) END ('1874-04-26'::date));
ALTER TABLE data.crm OWNER TO xsl;

INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.crm', 5, 360);

-- Used by template data loading workflow (CRM part) and defined based on the data guide
CREATE TABLE data.in_crm
(
  alias_id integer,   -- This is actually msisdn
  csp_internal_subs_id text,
  subscription_type text,
  payment_type text,
  gender text,
  birth_date date,
  switch_on_date date,
  switch_off_date date,
  churn_notification_date date,
  binding_contract text,
  binding_contract_start_date date,
  binding_contract_length integer,
  subscriber_value double precision,
  subscriber_segment text,
  handset_model text,
  handset_begin_date date,
  tariff_plan text,
  zip text,
  country text,
  language text,
  churn_score double precision,
  "timestamp" date,
  date_inserted date  -- This is max(switch_on_date)+1. It may be different as "timestamp"
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (date_inserted) (PARTITION "1874-04-25" START ('1874-04-25'::date) END ('1874-04-26'::date));
ALTER TABLE data.in_crm OWNER TO xsl;

INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.in_crm', 5, 360);

CREATE TABLE data.crm_snapshot_date_inserted_mapping 
(
  date_inserted date,
  snapshot_date date,
  UNIQUE (date_inserted)
)
DISTRIBUTED BY (date_inserted);
ALTER TABLE data.crm_snapshot_date_inserted_mapping OWNER TO xsl;

-- CDR data tables ----------------------------------------

-- Used by template data loading workflow (CDR part) and defined based on the data guide
CREATE TABLE tmp.cdr
(
  source_file text,
  -- All the rest fields are defined in the data guide.
  a_number text,
  b_number text,
  a_network text,
  b_network text,
  duration text,
  type text,
  call_time text,
  a_cell_id text,
  b_cell_id text,
  remaining_credits text,
  a_call_cost text,
  termination_reason text
) WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (a_number);
ALTER TABLE tmp.cdr OWNER TO xsl;


-- Used by template data loading workflow (CDR part)
CREATE TABLE tmp.cdr_err
(
  cmdtime timestamp with time zone,
  relname text,
  filename text,
  linenum integer,
  bytenum integer,
  errmsg text,
  rawdata text,
  rawbytes bytea
)
WITH (OIDS=FALSE)
DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.cdr_err OWNER TO xsl;


-- Used by template data loading workflow (CDR part) and defined based on the data guide
CREATE TABLE tmp.cdr_staging
(
  source_file text,
  -- All the rest fields are defined in the data guide.
  a_number text,
  b_number text,
  a_network text,
  b_network text,
  call_length double precision,   -- This field is named as duration in the data guide
  call_type integer,              -- This field is named as type in the data guide
  call_time timestamp without time zone,
  a_cell_id integer,
  b_cell_id integer,
  remaining_credits double precision,
  a_call_cost double precision,
  termination_reason text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (a_number);
ALTER TABLE tmp.cdr_staging OWNER TO xsl;


-- Used by template data loading workflow (CDR part) and defined based on the data guide
CREATE TABLE data.cdr
(
  alias_a integer,
  alias_b integer,
  a_network integer,
  b_network integer,
  call_length double precision,
  call_type integer,
  call_time timestamp without time zone,
  a_cell_id integer,
  b_cell_id integer,
  remaining_credits double precision,
  a_call_cost double precision,
  termination_reason text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (alias_a)
PARTITION BY RANGE (call_time) (PARTITION "1874-04-25" START ('1874-04-25'::date) END ('1874-04-26'::date));
ALTER TABLE data.cdr OWNER TO xsl;

INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.cdr', 7, 360);

-- Used by template data loading workflow (CDR part) and defined based on the data guide
CREATE TABLE tmp.cdr_full_weeks
(
  alias_a integer,
  alias_b integer,
  a_network integer,
  b_network integer,
  call_length double precision,
  call_type integer,
  call_time timestamp without time zone,
  a_cell_id integer,
  b_cell_id integer,
  remaining_credits double precision,
  a_call_cost double precision,
  termination_reason text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (alias_a);
ALTER TABLE tmp.cdr_full_weeks OWNER TO xsl;


-- TOPUP data tables ----------------------------------------

-- Used by template data loading workflow (TOPUP part) and defined based on the data guide
CREATE TABLE tmp.topup
(
  source_file text,
  -- All the rest fields are defined in the data guide.
  charged_id text,
  receiving_id text,
  credit_amount text,
  topup_cost text,
  is_credit_transfer text,
  topup_channel text,
  credit_balance text,
  topup_timestamp text
) WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (charged_id);
ALTER TABLE tmp.topup OWNER TO xsl;


-- Used by template data loading workflow (TOPUP part)
CREATE TABLE tmp.topup_err
(
  cmdtime timestamp with time zone,
  relname text,
  filename text,
  linenum integer,
  bytenum integer,
  errmsg text,
  rawdata text,
  rawbytes bytea
)
WITH (OIDS=FALSE)
DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.topup_err OWNER TO xsl;


-- Used by template data loading workflow (TOPUP part) and defined based on the data guide
CREATE TABLE tmp.topup_staging
(
  source_file text,
  -- All the rest fields are defined in the data guide.
  charged_id text,
  receiving_id text,
  credit_amount double precision,
  topup_cost double precision,
  is_credit_transfer integer,
  topup_channel text,
  credit_balance double precision,
  topup_timestamp timestamp without time zone
) WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (charged_id);
ALTER TABLE tmp.topup_staging OWNER TO xsl;


-- Used by template data loading workflow (TOPUP part) and defined based on the data guide
CREATE TABLE data.topup
(
  charged_id integer,
  receiving_id integer,
  credit_amount double precision,
  topup_cost double precision,
  is_credit_transfer boolean,
  channel text,
  credit_balance double precision,
  "timestamp" timestamp without time zone
) WITH (APPENDONLY=true, COMPRESSLEVEL=7, OIDS=FALSE)
DISTRIBUTED BY (charged_id)
PARTITION BY RANGE ("timestamp") (PARTITION "1874-04-25" START ('1874-04-25'::date) END ('1874-04-26'::date));
ALTER TABLE data.topup OWNER TO xsl;

INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.topup', 7, 360);

-- PRODUCT data tables ----------------------------------------

-- Defined based on the data guide
CREATE TABLE tmp.product
(
  source_file text,           -- name of the flat file that has been uploaded to the table
  -- All the rest fields are defined in the data guide. 
  string_id text,
  product_id text,
  subproduct_id text,
  product_possible text,
  product_score text,
  product_taken_date text,
  product_churn_date text,
  date_inserted text
) WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY (string_id);
ALTER TABLE tmp.product OWNER TO xsl;


CREATE TABLE tmp.product_err
(
  cmdtime timestamp with time zone,
  relname text,
  filename text,
  linenum integer,
  bytenum integer,
  errmsg text,
  rawdata text,
  rawbytes bytea
)
WITH (OIDS=FALSE)
DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.product_err OWNER TO xsl;


-- Defined based on the data guide (+ subproduct_id in addition to SL 2.0 data guide format)
CREATE TABLE tmp.product_staging
(
  source_file text,           -- Name of the flat file that has been uploaded to the table
  -- All the rest fields are defined in the data guide. Typically very customer/data spesific in practice.
  string_id text,            
  product_id text,
  subproduct_id text,
  product_possible boolean,
  product_score double precision,
  product_taken_date date,
  product_churn_date date,
  date_inserted date
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY (string_id);
ALTER TABLE tmp.product_staging OWNER TO xsl;


CREATE TABLE data.product
(
  source_file text,           -- Name of the flat file that has been uploaded to the table
  -- All the rest fields are defined in the data guide. Typically very customer/data spesific in practice.
  alias_id text,            
  product_id text,
  subproduct_id text,
  product_possible boolean,
  product_score double precision,
  product_taken_date date,
  product_churn_date date,
  date_inserted date
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, OIDS=FALSE)
DISTRIBUTED BY (alias_id)
PARTITION BY RANGE (date_inserted) (PARTITION "1874-04-25" START ('1874-04-25'::date) END ('1874-04-26'::date));
ALTER TABLE data.product OWNER TO xsl;

INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) 
VALUES ('data.product', 5, 360);


-- GENERIC MONITORING OF DATA LOADING AND PROCESSING ----------------------------------------

CREATE TABLE data.processed_files (
  source_file          text,
  processing_timestamp timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  max_timestamp timestamp without time zone,
  rowcount integer,
  parameter1 double precision,
  parameter2 double precision,
  dataset_id text,
  PRIMARY KEY (source_file)
) DISTRIBUTED BY (source_file);
ALTER TABLE data.processed_files OWNER TO xsl;

CREATE TABLE data.processed_data (
  dataset_id text,
  data_type text,
  data_date date,
  processing_timestamp timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  rowcount integer,
  parameter1 double precision,
  parameter2 double precision,
  PRIMARY KEY (dataset_id, data_type, data_date)
) DISTRIBUTED BY (dataset_id);
ALTER TABLE data.processed_data OWNER TO xsl;

CREATE TABLE data.failed_rows_stats (
  data_source text,
  source_file text,
  rowcount integer,
  PRIMARY KEY (source_file)
) DISTRIBUTED BY (source_file);
ALTER TABLE data.failed_rows_stats OWNER TO xsl;


CREATE TABLE data.crm_statistics
(
  date_inserted date,
  active_distinct_alias bigint,
  prepaid_percentage double precision,
  avg_tenure_months numeric,
  new_subs_percentage numeric,
  primary key (date_inserted)
) WITH (OIDS=FALSE)
DISTRIBUTED BY (date_inserted);
ALTER TABLE data.crm_statistics OWNER TO xsl;


CREATE TABLE data.cdr_weekly_statistics
(
  table_name text,
  period text,
  rowcount bigint,
  dis_alias_a bigint,
  dis_alias_b bigint,
  avg_voice double precision,
  avg_voice_sum double precision,
  avg_sms double precision,
  primary key (table_name, period)
) WITH (OIDS=FALSE)
DISTRIBUTED BY (table_name, period);
ALTER TABLE data.cdr_weekly_statistics OWNER TO xsl;


CREATE TABLE data.data_usage_weekly_stats
(
  table_name text,
  period text,
  rowcount bigint,
  dis_alias bigint,
  mms_percentage double precision,
  mms_avg double precision,
  mms_day double precision,
  data_percentage double precision,
  data_avg double precision,
  data_day double precision,
  primary key (table_name, period)
) WITH (OIDS=FALSE)
DISTRIBUTED BY (table_name, period);
ALTER TABLE data.data_usage_weekly_stats OWNER TO xsl;


CREATE TABLE data.topup_statistics
(
  type_of_event text,
  period integer,
  topup_events bigint,
  dis_charged bigint,
  dis_receiving bigint,
  avg_credit_amount double precision,
  avg_credit_balance double precision,
  primary key (type_of_event, period)
) WITH (OIDS=FALSE)
DISTRIBUTED BY (type_of_event, period);
ALTER TABLE data.topup_statistics OWNER TO xsl;

-- Data Availability Check table
CREATE TABLE tmp.data_quality
(
  data_source text NOT NULL, -- 'CDR', 'CRM', 'TOPUP'
  data_date date NOT NULL, -- e.g., 14-01-2013
  status integer NOT NULL, -- 2-OK, 3-WARNING, 4-ERROR
  error_count_preloading integer,
  error_count_aggregate integer,
  error_count_other integer,
  error_count_preloading_by_row integer,
  CONSTRAINT data_quality_pkey PRIMARY KEY (data_source, data_date)
) DISTRIBUTED BY (data_source, data_date);
ALTER TABLE tmp.data_quality OWNER TO xsl;

CREATE TABLE data.data_quality
(
  data_source text NOT NULL, -- 'CDR', 'CRM', 'TOPUP'
  data_date date NOT NULL, -- e.g., 14-01-2013
  status integer NOT NULL, -- 2-OK, 3-WARNING, 4-ERROR
  error_count_preloading integer,
  error_count_aggregate integer,
  error_count_other integer,
  error_count_preloading_by_row integer,
  CONSTRAINT data_quality_pkey PRIMARY KEY (data_source, data_date)
) DISTRIBUTED BY (data_source, data_date);
ALTER TABLE data.data_quality OWNER TO xsl;


-- ALIASING ---------------------------------------------------------------------

CREATE TABLE aliases.string_id (
  alias_id serial,
  string_id text,
  date_inserted date,
  PRIMARY KEY (string_id) )
DISTRIBUTED BY (string_id);
ALTER TABLE aliases.string_id OWNER TO xsl;

CREATE TABLE aliases.network (
  alias_id integer,
  validity date,
  data_source_id smallint, -- 1=CDR, 2=TOPUP
  net_id integer)
WITH (appendonly=TRUE, compresslevel=7)
DISTRIBUTED BY (alias_id);
ALTER TABLE aliases.network OWNER TO xsl;

-- BLACKLIST ---------------------------------------------------------------------

CREATE TABLE tmp.blacklist_err
(
  cmdtime timestamp with time zone,
  relname text,
  filename text,
  linenum integer,
  bytenum integer,
  errmsg text,
  rawdata text,
  rawbytes bytea
)
WITH (OIDS=FALSE)
DISTRIBUTED RANDOMLY ;
ALTER TABLE tmp.blacklist_err OWNER TO xsl;

CREATE TABLE tmp.blacklist
(
  source_file text,
  subscriber_id text,
  blacklist_id text,
  timestamp text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=7, 
  OIDS=FALSE
)
DISTRIBUTED BY (subscriber_id);
ALTER TABLE tmp.blacklist OWNER TO xsl;

CREATE TABLE tmp.blacklist_staging
(
  source_file text,
  subscriber_id text,
  blacklist_id text,
  timestamp date
)
WITH (APPENDONLY=true, COMPRESSLEVEL=7, 
  OIDS=FALSE
)
DISTRIBUTED BY (subscriber_id);
ALTER TABLE tmp.blacklist_staging OWNER TO xsl;

CREATE TABLE aliases.blacklist
(
  alias_id integer,
  blacklist_id text,
  validity date,
  on_black_list smallint NOT NULL DEFAULT 0
)
WITH (OIDS=FALSE)
DISTRIBUTED BY (alias_id);
ALTER TABLE aliases.blacklist
  OWNER TO xsl;
  
-- DATA VALIDATION ERRORS ------------------------------------------------------------

CREATE TABLE tmp.validation_errors
(
  data_source character varying,
  file_short_name character varying,
  error_code character varying,
  data_type character varying,
  severity character varying,
  error_desc character varying,
  file_row_num character varying,
  file_column_num character varying,
  file_full_row character varying,
  data_date date
) DISTRIBUTED BY (data_source);
ALTER TABLE tmp.validation_errors OWNER TO xsl;

CREATE TABLE tmp.validation_errors_err
(
  cmdtime timestamp with time zone,
  relname text,
  filename text,
  linenum integer,
  bytenum integer,
  errmsg text,
  rawdata text,
  rawbytes bytea
)DISTRIBUTED RANDOMLY;
ALTER TABLE tmp.validation_errors_err OWNER TO xsl;

CREATE TABLE data.validation_errors
(
  file_short_name character varying,
  error_code character varying,
  data_type character varying,
  severity character varying,
  error_desc character varying,
  file_row_num character varying,
  file_column_num character varying,
  file_full_row character varying,
  data_date date,
  id serial
) DISTRIBUTED BY (data_date, data_type)
PARTITION BY RANGE (data_date) (PARTITION "1874-04-25" START ('1874-04-25'::date) END ('1874-04-26'::date));
ALTER TABLE data.validation_errors OWNER TO xsl;

INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates)
VALUES ('data.validation_errors', 7, 360);


CREATE TABLE data.blocklist (
  id                         varchar(255),
  msisdn                     varchar(100)
) 
DISTRIBUTED BY (id, msisdn);
ALTER TABLE data.blocklist OWNER TO xsl;

CREATE TABLE data.data_source_lookup (
  table_name text NOT NULL,
  data_source text NOT NULL
)
DISTRIBUTED BY (table_name, data_source);
ALTER TABLE data.data_source_lookup OWNER TO xsl;

INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.cdr', 'cdr');
INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.topup', 'topup');
INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.product', 'product_takeup');
INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.in_split_aggregates', 'cdr');
INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.data.in_split_weekly', 'cdr');
INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.in_crm', 'crm');
INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.crm', 'crm');
-- INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.call_types_weekly', 'cdr'); handle data.call_types_weekly separately
-- INSERT INTO data.data_source_lookup(table_name, data_source) VALUES ('data.validation_errors', '');  exclude data.validation_errors as a auxiliary table
