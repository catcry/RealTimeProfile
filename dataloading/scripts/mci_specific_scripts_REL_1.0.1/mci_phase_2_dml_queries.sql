/*
 DML queries for Iranian localization.
*/

INSERT INTO tmp.dsttime VALUES ('2017', '2017-03-22 00:00:00', '2017-09-22 00:00:00', 4.5, 3.5);
INSERT INTO tmp.dsttime VALUES ('2018', '2018-03-22 00:00:00', '2018-09-22 00:00:00', 4.5, 3.5);
INSERT INTO tmp.dsttime VALUES ('2019', '2019-03-22 00:00:00', '2019-09-22 00:00:00', 4.5, 3.5);
INSERT INTO tmp.dsttime VALUES ('2020', '2020-03-21 00:00:00', '2020-09-21 00:00:00', 4.5, 3.5);
INSERT INTO tmp.dsttime VALUES ('2021', '2021-03-22 00:00:00', '2021-09-22 00:00:00', 4.5, 3.5);
INSERT INTO tmp.dsttime VALUES ('2022', '2022-03-22 00:00:00', '2022-09-22 00:00:00', 4.5, 3.5);
INSERT INTO tmp.dsttime VALUES ('2023', '2023-03-22 00:00:00', '2023-09-22 00:00:00', 4.5, 3.5);
INSERT INTO tmp.dsttime VALUES ('2024', '2024-03-21 00:00:00', '2024-09-21 00:00:00', 4.5, 3.5);


/*
 DML queries for Special Hour definitions.
*/

INSERT INTO tmp.specialhours VALUES ('business', 8, 18);
INSERT INTO tmp.specialhours VALUES ('peakvoice', 7, 23);
INSERT INTO tmp.specialhours VALUES ('peakdata', 7, 2);


/*
 DML queries for CDR aggregation checking.
*/

-- TRUNCATE TABLE tmp.mci_file_list;
INSERT INTO tmp.mci_file_list VALUES ('FAA_BILLING-'          , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOST-COMLIVE-'  , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOST-RECLIVE-'  , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOST-SMSLIVE-'  , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOSTA-RECLIVE-' , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOSTB-RECLIVE-' , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOSTA-SMSLIVE-' , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOSTB-SMSLIVE-' , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOSTA-COMLIVE-' , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSPOSTB-COMLIVE-' , 'daily', 'No', 'BILLING', 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_HUAMSC-'           , 'daily', 'No', 'HUAWEI' , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_HUAMSCLIVE-'       , 'daily', 'No', 'HUAWEI' , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_NOKIAMSC-'         , 'daily', 'No', 'NOKIA'  , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_NOKIAMSCLIVE-'     , 'daily', 'No', 'NOKIA'  , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSA-REC-'         , 'daily', 'No', 'REC'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCS-RECLIVE-'      , 'daily', 'No', 'REC'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCS-REC-'          , 'daily', 'No', 'REC'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSA-SMS-'         , 'daily', 'No', 'SMS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCS-SMS-'          , 'daily', 'No', 'SMS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCS-SMSLIVE-'      , 'daily', 'No', 'SMS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCS-COM-'          , 'daily', 'No', 'COM'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSA-COM-'         , 'daily', 'No', 'COM'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCS-COMLIVE-'      , 'daily', 'No', 'COM'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSB-COM-'         , 'daily', 'No', 'COM'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_OCSB-COM-'         , 'daily', 'No', 'COM'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_VGS-'              , 'daily', 'No', 'VGS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_VGSA-'             , 'daily', 'No', 'VGS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_VGSB-'             , 'daily', 'No', 'VGS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_VGSC-'             , 'daily', 'No', 'VGS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_VGSD-'             , 'daily', 'No', 'VGS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_VGSPOST-'          , 'daily', 'No', 'VGS'    , 0.3);
INSERT INTO tmp.mci_file_list VALUES ('FAA_VGSPRE-'           , 'daily', 'No', 'VGS'    , 0.3);

/*
 DML queries for partitioning.
*/

-- Table: data.customer_care
DELETE FROM core.partition_date_tables WHERE table_name = 'data.customer_care';
INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) VALUES ('data.customer_care', 5, 180);

-- Table: data.portability
DELETE FROM core.partition_date_tables WHERE table_name = 'data.portability';
INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) VALUES ('data.portability', 5, 180); 

-- Table: data.pre_aggregates
DELETE FROM core.partition_date_tables WHERE table_name = 'data.pre_aggregates';
INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) VALUES ('data.pre_aggregates', 5, 360);

-- Table: data.product_takeup
DELETE FROM core.partition_date_tables WHERE table_name = 'data.product_takeup';
INSERT INTO core.partition_date_tables (table_name, compresslevel, retention_dates) VALUES ('data.product_takeup', 5, 360);

-- Table: results.module_export_portout
DELETE FROM core.partition_sequence_tables WHERE table_name = 'results.module_export_portout';
INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) VALUES ('results.module_export_portout', 'work.module_sequence', 5);

-- Table: results.rtp_dump
DELETE FROM core.partition_sequence_tables WHERE table_name = 'results.rtp_dump';
INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) VALUES ('results.rtp_dump', 'work.module_sequence', 5);

-- Table: results.rtp_dump_delta
DELETE FROM core.partition_sequence_tables WHERE table_name = 'results.rtp_dump_delta';
INSERT INTO core.partition_sequence_tables (table_name, sequence_name, compresslevel) VALUES ('results.rtp_dump_delta', 'work.module_sequence', 5);
