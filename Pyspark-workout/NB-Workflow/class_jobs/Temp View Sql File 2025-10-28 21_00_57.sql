create or replace temp view tblprof_tvw
as
select COUNT(1) COUNT_RECORD,profession,age from databricks_practice.outputdb.tblprof
group by profession,age
order by 2,3;