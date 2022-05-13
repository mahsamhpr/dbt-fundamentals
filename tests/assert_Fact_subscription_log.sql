with centers as (select 'FF' || "id"     as NK_CENTER,
                        "migration_date" as migration_date
                 from {{source('membersegment', 'EXERP_FF_CENTER')}}
                 union
                 select 'SA' || "id"     as NK_CENTER,
                        "migration_date" as migration_date
                 from DWH.PSA.EXERP_SA_CENTER),
     dates
         as (select dateadd(day, -1, date_trunc(month,
                                                dateadd(month, -row_number() over (order by seq4()), current_date))) as period
             from table (generator(rowcount => 50))
             order by period),
     new as (select d.period, new.NK_CENTER, count(new.NK_SUBSCRIPTION) as new_count
             from dates d
                      inner join DM.RPT.FACT_SUBSCRIPTION_LOG new
                                 on d.period between ACTIVE_PHASE and dateadd(day, -1, nvl(END_PHASE, '2099-12-31'))
                      inner join DM.RPT.DIM_CENTER DC on new.NK_CENTER = DC.NK_CENTER and
                                                         date_trunc(month, dc.OPENING_DATE) <= date_trunc(month, d.period)
                      inner join centers on new.NK_CENTER = centers.NK_CENTER and
                                            date_trunc(month, nvl(centers.migration_date, '1970-01-01')) <= date_trunc(month, d.period)
             group by 1, 2)
select new.period,
       new.NK_CENTER,
       new_count,
       sum(old.ACTIVE_AT_PERIOD_END) as old_count,
       new_count - old_count         as delta
from new
         left join RPT.FACT_MEMBERSHIP_MONTH old on new.period = old.DATE and new.NK_CENTER = old.NK_CENTER
group by 1, 2, 3
order by 2, 1
;