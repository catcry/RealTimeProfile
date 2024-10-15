truncate tmp.data_quality;

insert into tmp.data_quality (
select * from data.data_quality
);

truncate data.data_quality;

insert into data.data_quality (data_source, data_date, status, errar_count_preloading, error_count_aggregate, error_count_other, error_count_preloading_by_row)
(
select data_type, data_date, max(status), sum(error_count_preloading), sum(error_count_aggregate), sum(error_count_other), sum(error_count_preloading_by_row)
from
(
(
select data_type, data_date, 2 as status, 0 as error_count_preloading, 0 as error_count_aggregate, 0 as error_count_other, 0 as error_count_preloading_by_row from data.processed_data where dataset_id='WORKFLOW_RUN_ID'
)
union all
(
select
data_type,
data_date,
Max(case when severity='WARNING' then 3
when severity='CRITICAL' then 4
else -1 --unknown
end
) as status,
SUM( case when error_code::int between 10000 and 19999 then 1 else 0 end) as error_count_preloading,
SUM(case when error_code::int between 20000 and 29999 then 1 else 0 end) as error_count_aggregate,
SUM( case when error_code::int NOT between 10000 and 29999 then 1 else 0 end) as error_count_other,
(case when SUM(case when error_code::int between 10000 and 19999 then 1 else 0 end) > 0 then 1 else 0 end) as error_count_preloading_by_row
from tmp.validation_errors group by data_type, data_date, file_row_num
)

)b

group by data_type, data_date
);

insert into data.data_quality
select a.* from tmp.data_quality a
left outer join data.data_quality b
on a.data_source = b.data_source
and a.data_date = b.data_date
where b.data_source is null;


