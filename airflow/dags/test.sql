CREATE OR REPLACE FUNCTION process_dates(input_dates date[])
RETURNS VOID AS
$$
DECLARE
    t2date date;
    tcrmdate date;
BEGIN
    FOREACH t2date IN ARRAY input_dates LOOP
        -- Execute the first query
        EXECUTE '
            SELECT data_date AS tcrmdate 
            FROM cor.partition_date_create_time 
            WHERE table_name = ''data.in_crm''
            AND data_date > now()::DATE - INTERVAL ''3 months''
            AND data_date <= (
                SELECT MAX(data_date) - INTERVAL ''14 days'' - INTERVAL ''28 days'' 
                FROM core.partition_date_create_times 
                WHERE table_name = ''data_call_types_weekly'' 
                AND data_date <= now()::DATE
            )
            AND data_date > $1 - INTERVAL ''14 days''
            AND data_date <= $1
            ORDER BY data_date DESC 
            LIMIT 1;'
        INTO tcrmdate
        USING t2date;

        -- Execute the second query
        EXECUTE '
            SELECT * 
            FROM work.create_target_list_churn_inactivity_laith($1, $2);'
        USING t2date, tcrmdate;

        -- Execute the third query
        EXECUTE '
            SELECT * 
            FROM work.create_target_list_portout_laith($1, $2);'
        USING t2date, tcrmdate;
    END LOOP;
END;
$$
LANGUAGE plpgsql;











select count(a.count) from
            (select count(source_file)
             from tmp.topup
             group by source_file
             union
             select count(source_file)
             from tmp.cdr
             group by source_file
             union
             select count(source_file)
             from tmp.customer_care
             group by source_file) as a
            ;