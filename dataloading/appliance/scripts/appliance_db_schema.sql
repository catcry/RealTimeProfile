DROP SCHEMA IF EXISTS appliance CASCADE;

CREATE SCHEMA appliance;
ALTER SCHEMA appliance OWNER TO xsl;

DROP TABLE IF EXISTS appliance.churn_verification;

CREATE TABLE appliance.churn_verification
(
id varchar(20) NOT NULL,
usecase varchar(20) NOT NULL,
msisdn varchar (50) NOT NULL,
churn_propensity_score double precision,
primary key (id,usecase,msisdn)
)
DISTRIBUTED BY (id,usecase, msisdn);

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION appliance.verify_usecase(loadid_1 text, loadid_2 text, use_case text, threshold double precision)
  RETURNS boolean AS
$BODY$
/* SUMMARY
 * A function which checks that the result of a churn run matches the expected result
 * for certain usecases (postpaid churn or inactivity churn)
 * - number of rows for the use cases must be the same for a certain use case
 * - the msisdn numbers must match
 * - the values for the propensity score for the actual and the expected result must
 * - be highly correlated
 */
DECLARE
    rows_loadid_1 integer = -1;
    rows_loadid_2 integer = -1;
    diffs_msisdn_1 integer = -1;
    diffs_msisdn_2 integer = -1;
    errors integer = 0;
    correlation_coefficient double precision;

BEGIN

    select count(*) from appliance.churn_verification where id = loadid_1 and usecase = use_case into rows_loadid_1;
    select count(*) from appliance.churn_verification where id = loadid_2 and usecase = use_case into rows_loadid_2;

    select count(*) from
    (
	select msisdn from appliance.churn_verification where id = loadid_1 and usecase = use_case
	except
	select msisdn from appliance.churn_verification where id = loadid_2 and usecase = use_case
    ) a into diffs_msisdn_1;

    select count(*) from
    (
	select msisdn from appliance.churn_verification where id = loadid_2 and usecase = use_case
	except
	select msisdn from appliance.churn_verification where id = loadid_1 and usecase = use_case
    ) a into diffs_msisdn_2;

    select
     corr(l_score, r_score) as corr_score
     from
    (
    select
     l.churn_propensity_score as l_score, r.churn_propensity_score as r_score
    from appliance.churn_verification l
    join
    appliance.churn_verification r
    on l.msisdn = r.msisdn
    and l.id = loadid_1
    and r.id = loadid_2
    and l.usecase = r.usecase
    and l.usecase = use_case
    ) a into correlation_coefficient;

    if (rows_loadid_1 = 0) then
        RAISE NOTICE 'No data has been loaded for usecase = % and load (result) %',
                      use_case, loadid_1;
        errors = errors + 1;
    end if;


    if (rows_loadid_1 != rows_loadid_2) then
        RAISE NOTICE 'Number of rows differ for usecase = %: % = %, % = %',
                      use_case, loadid_1, rows_loadid_1, loadid_2, rows_loadid_2;
        errors = errors + 1;
    end if;


    if (diffs_msisdn_1 != 0) then
        RAISE NOTICE 'Usecase: %: there are % msisdn number in % result, which are not in % result.',
                      use_case, diffs_msisdn_1, loadid_1, loadid_2;
        errors = errors + 1;
    end if;

    if (diffs_msisdn_2 != 0) then
        RAISE NOTICE 'Usecase: %: there are % msisdn number in % result, which are not in % result.',
                      use_case, diffs_msisdn_2, loadid_2, loadid_1;
        errors = errors + 1;
    end if;

    if (correlation_coefficient < threshold) then
        RAISE NOTICE 'Usecase: %: correlation coefficient % too low, should be at least %',
                      use_case, correlation_coefficient, threshold;
        errors = errors + 1;
    else
        RAISE NOTICE 'Usecase: %: correlation coefficient is: %',
                      use_case, correlation_coefficient;
    end if;

    if (errors > 0) then
        RAISE NOTICE 'Verification failed for usecase %. There were % errors', use_case, errors;
        return false;
    else
        RAISE NOTICE 'Verification succeeded for usecase %.', use_case;
        return true;
    end if;
        
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION appliance.verify_usecase(text, text, text, double precision) OWNER TO xsl;

----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION appliance.verify()
  RETURNS boolean AS
$BODY$
/* SUMMARY
 * Calls verification function for use cases churn_inactivity and churn_postpaid.
 */
DECLARE
    result_churn_inactivity boolean;
    result_churn_postpaid boolean;
    result boolean = true;
    threshold double precision = 0.8;

BEGIN


    select appliance.verify_usecase('expected', 'actual', 'churn_inactivity', threshold) into result_churn_inactivity;
    select appliance.verify_usecase('expected', 'actual', 'churn_postpaid', threshold) into result_churn_postpaid;


    if (result_churn_inactivity = false) then
        RAISE NOTICE 'Verification for churn_inactivity failed';
        result = false;
    end if;

    if (result_churn_postpaid = false) then
        RAISE NOTICE 'Verification for churn_postpaid failed';
        result = false;
    end if;
        
    return result;

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE;
ALTER FUNCTION appliance.verify() OWNER TO xsl;

----------------------------------------------------------------------------------------
