-- INSERT NEW DATA
insert into data.validation_errors
(
    data_date,
    data_type,
    file_short_name,
    file_row_num,
    file_column_num,
    error_code,
    error_desc,
    file_full_row,
    severity
)
(
select
    data_date,
    data_type,
    file_short_name,
    file_row_num,
    file_column_num,
    error_code,
    error_desc,
    file_full_row,
    severity
from tmp.validation_errors
);
