/*
This file contains the schema upgrade sql to greenplum 4.3.3.0.
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
