{{ config(
materialized='table'
) }}

with ff_log as (select 'FF' || s."center_id"                                        as NK_CENTER,
                       'FF' || s."id"                                               as NK_SUBSCRIPTION,
                       'FF' || s."person_id"                                        as NK_CONTACT,
                       'FF' || p."master_product_id" || '_' || p."product_group_id" as NK_PRODUCT,
                       case
                           when ssl."state" = 'CREATED' then 'CREATE_PHASE'
                           when ssl."state" = 'ACTIVE' then 'ACTIVE_PHASE'
                           when ssl."state" = 'FROZEN' then 'ACTIVE_PHASE'
                           when ssl."state" = 'WINDOW' then 'END_PHASE'
                           when ssl."state" = 'ENDED' then 'END_PHASE'
                           end                                                      as phase,
                       pg."name"                                                    as product_group_name,
                       case
                           when pg."name" in ('1 Week Free Trial', 'Christmas Cards', 'Challenge Nov 2017', 'November TEMPORARY') then 'TRIAL'
                           when pg."name" like 'Donate4W%' then 'TRIAL'
                           when pg."name" in ('Add-on Memberships') then 'ADDON'
                           when pg."name" = 'Barter Memberships' or p."name" like '%Barter%' then 'BARTER'
                           when pg."name" = 'Drop In' then 'DROP IN'
                           when pg."name" = 'Gift Memberships' then 'GIFT'
                           when pg."name" = 'Free Memberships' then 'FREE'
                           when pg."name" like 'PT%' then 'PT'
                           when pg."name" = 'Staff Memberships' then 'STAFF'
                           when pg."name" = 'Special Memberships' then 'SPECIAL'
                           when pg."name" = 'Digital Membership' then 'DIGITAL'
                           else 'SUBSCRIPTION' end                                  as product_group,
                       coalesce(iff(ssl."state" = 'CREATED', "entry_start_datetime", null),
                                iff(ssl."state" in ('ACTIVE', 'FROZEN'), "entry_start_datetime", null),
                                iff(ssl."state" in ('WINDOW', 'ENDED'), "entry_start_datetime",
                                    null))                                          as state_datetime,
                       s."creation_datetime"                                        as alt_create_datetime,
                       s."start_date"                                               as alt_start_date,
                       s."end_date"                                                 as alt_end_date,
                       s."stop_datetime"                                            as stop_datetime,
                       s."stop_cancel_datetime"                                     as stop_cancel_datetime
                from {{source('membersegment', 'EXERP_FF_SUBSCRIPTION')}}
                         left join {{source('membersegment', 'EXERP_FF_SUBSCRIPTION_STATE_LOG')}} ssl on s."id" = ssl."subscription_id"
                         inner join {{source('membersegment', 'EXERP_FF_PRODUCT')}} p on s."product_id" = p."id"
                         inner join {{source('membersegment', 'EXERP_FF_PRODUCT_GROUP')}} pg on p."product_group_id" = pg."id"
                    qualify row_number() over (partition by NK_SUBSCRIPTION, phase order by ssl."ets") = 1),
     sa_log as (select 'SA' || s."center_id"                                        as NK_CENTER,
                       'SA' || s."id"                                               as NK_SUBSCRIPTION,
                       'SA' || s."person_id"                                        as NK_CONTACT,
                       'SA' || p."master_product_id" || '_' || p."product_group_id" as NK_PRODUCT,
                       case
                           when ssl."state" = 'CREATED' then 'CREATE_PHASE'
                           when ssl."state" = 'ACTIVE' then 'ACTIVE_PHASE'
                           when ssl."state" = 'FROZEN' then 'ACTIVE_PHASE'
                           when ssl."state" = 'WINDOW' then 'END_PHASE'
                           when ssl."state" = 'ENDED' then 'END_PHASE'
                           end                                                      as phase,
                       pg."name"                                                    as product_group_name,
                       case
                           when pg."name" in ('1 Week Free Trial', 'Christmas Cards', 'Challenge Nov 2017', 'November TEMPORARY') then 'TRIAL'
                           when pg."name" like 'Donate4W%' then 'TRIAL'
                           when pg."name" in ('Add-on Memberships') then 'ADDON'
                           when pg."name" = 'Barter Memberships' or p."name" like '%Barter%' then 'BARTER'
                           when pg."name" = 'Drop In' then 'DROP IN'
                           when pg."name" = 'Gift Memberships' then 'GIFT'
                           when pg."name" = 'Free Memberships' then 'FREE'
                           when pg."name" like 'PT%' then 'PT'
                           when pg."name" = 'Staff Memberships' then 'STAFF'
                           when pg."name" = 'Special Memberships' then 'SPECIAL'
                           when pg."name" = 'Digital Membership' then 'DIGITAL'
                           else 'SUBSCRIPTION' end                                  as product_group,
                       coalesce(iff(ssl."state" = 'CREATED', "entry_start_datetime", null),
                                iff(ssl."state" in ('ACTIVE', 'FROZEN'), "entry_start_datetime", null),
                                iff(ssl."state" in ('WINDOW', 'ENDED'), "entry_start_datetime",
                                    null))                                          as state_datetime,
                       s."creation_datetime"                                        as alt_create_datetime,
                       s."start_date"                                               as alt_start_date,
                       s."end_date"                                                 as alt_end_date,
                       s."stop_datetime"                                            as stop_datetime,
                       s."stop_cancel_datetime"                                     as stop_cancel_datetime
                from {{source('membersegment', 'EXERP_SA_SUBSCRIPTION')}} s
                         left join {{source('membersegment', 'EXERP_SA_SUBSCRIPTION_STATE_LOG')}} ssl on s."id" = ssl."subscription_id"
                         inner join {{source('membersegment', 'EXERP_SA_PRODUCT')}} p on s."product_id" = p."id"
                         inner join {{source('membersegment', 'EXERP_SA_PRODUCT_GROUP')}} pg on p."product_group_id" = pg."id"
                    qualify row_number() over (partition by NK_SUBSCRIPTION, phase order by ssl."ets") = 1),
     all_rows as (select *
                  from ff_log pivot ( max(state_datetime) for phase in ('CREATE_PHASE', 'ACTIVE_PHASE', 'END_PHASE'))
                  union
                  select *
                  from sa_log pivot ( max(state_datetime) for phase in ('CREATE_PHASE', 'ACTIVE_PHASE', 'END_PHASE'))),
     phases as (select nk_center,
                       nk_subscription,
                       nk_contact,
                       NK_PRODUCT,
                       product_group_name,
                       product_group,
                       to_date(stop_datetime)                                                                                      as stop_date,
                       to_date(stop_cancel_datetime)                                                                               as stop_cancel_date,
                    /*
                    1) If ACTIVE_PHASE is null and END_PHASE is later than CREATE_PHASE, it means that the subscription was either cancelled or regretted before
                       its start date. When END_PHASE = CREATE_PHASE we could have a transfer, an extension, a cancellation or regret, or some other weird shit.
                    2) If END_PHASE is null it means that the subscription has yet to be ended. It could, however, be terminated and have an actual end date.
                    3) If we do not have a CREATE_EVENT it means that the subscription was not created in our instance of Exerp, but acquired from somewhere else.
                       Then we use the CREATE_DATETIME and START_DATE attributes from the Exerp Subscription table instead.
                    */
                       nvl("'CREATE_PHASE'", alt_create_datetime)                                                                  as create_phase_datetime,
                       nvl("'ACTIVE_PHASE'", alt_start_date)                                                                       as active_phase_datetime,
                       nvl("'END_PHASE'", alt_end_date)                                                                            as end_phase_datetime,
                       to_date(create_phase_datetime)                                                                              as create_phase,
                       to_date(active_phase_datetime)                                                                              as active_phase,
                       to_date(end_phase_datetime)                                                                                 as end_phase,
                       row_number() over (partition by NK_CONTACT order by create_phase_datetime, active_phase_datetime)           as sub_no,
                       row_number() over (partition by NK_CONTACT order by create_phase_datetime desc, active_phase_datetime desc) as sub_no_desc
                from all_rows),
    /*
        1) Find the latest running end phase date. We need to figure out of if the members have older subscriptions that are still active
           when activating the next one.
        2) Filter on Product Groups related to such subscriptions.
        3) Remove subscriptions that were never activated.
        4) Remove subscription where the end phase starts on the same day as the active phase.
     */
     pay_seq_1 as (select nk_subscription,
                          nk_contact,
                          product_group,
                          create_phase,
                          active_phase,
                          end_phase,
                          sub_no,
                          sub_no_desc,
                          min(active_phase) over (partition by NK_CONTACT order by sub_no_desc )            as earliest_active_phase,
                          max(nvl(end_phase, '2099-12-31')) over (partition by NK_CONTACT order by sub_no ) as latest_end_phase
                   from phases
                   where PRODUCT_GROUP in ('SUBSCRIPTION', 'BARTER', 'STAFF')
                     and active_phase is not null
                     and nvl(end_phase, '2099-12-31') > active_phase),
    /*
        1) Calculate the number of days from the end of the previous subscription to the current one.
        2) Calculate the number of days from the end of the current subscription to the next one.
        3) Set a flag (1/0) to indicate the start/end of a sequence based on if there is a gap > 1 day.
     */
     pay_seq_2 as (select nk_subscription,
                          nk_contact,
                          product_group,
                          create_phase,
                          active_phase,
                          end_phase,
                          sub_no,
                          sub_no_desc,
                          earliest_active_phase,
                          latest_end_phase,
                          datediff(day, lag(latest_end_phase) over (partition by NK_CONTACT order by sub_no), earliest_active_phase)  as days_since_prev_payable_seq,
                          datediff(day, latest_end_phase, lead(earliest_active_phase) over (partition by NK_CONTACT order by sub_no)) as days_to_next_payable_seq,
                          iff(days_since_prev_payable_seq > 1, 1, 0)                                                                  as payable_seq_start_flag,
                          iff(days_to_next_payable_seq > 1, 1, 0)                                                                     as payable_seq_end_flag
                   from pay_seq_1),
    /*
        1) Number the sequences based on a cumulated sum of the flags from the previous step.
     */
     pay_seq_3 as (select nk_subscription,
                          nk_contact,
                          product_group,
                          create_phase,
                          active_phase,
                          end_phase,
                          sub_no,
                          sub_no_desc,
                          days_since_prev_payable_seq,
                          sum(payable_seq_start_flag) over (partition by NK_CONTACT order by sub_no)    as payable_seq_no,
                          sum(payable_seq_end_flag) over (partition by NK_CONTACT order by sub_no_desc) as payable_seq_no_desc
                   from pay_seq_2),
    /*
        1) Keep the first value of days_since_... for all subscriptions within a payable sequence.
        2) Populate all rows with the first Active Phase date and last End Phase date.
     */
     pay_seq_4 as (select nk_subscription,
                          nk_contact,
                          create_phase,
                          active_phase,
                          nvl(end_phase, '2099-12-31')                                                                                                                              as end_phase,
                          first_value(days_since_prev_payable_seq) over (partition by NK_CONTACT, payable_seq_no order by sub_no)                                                   as days_since_prev_payable_seq,
                          min(ACTIVE_PHASE) over (partition by NK_CONTACT, payable_seq_no order by sub_no rows between unbounded preceding and unbounded following)                 as payable_seq_start_date,
                          max(nvl(end_phase, '2099-12-31')) over (partition by NK_CONTACT, payable_seq_no order by sub_no rows between unbounded preceding and unbounded following) as payable_seq_end_date,
                          payable_seq_no,
                          payable_seq_no_desc
                   from pay_seq_3),
    /*
        1) Add info from the payable sequence steps to the initial Phases step. This concludes the job.
     */
     result as
         (select phases.*,
                 days_since_prev_payable_seq,
                 payable_seq_start_date,
                 payable_seq_end_date,
                 payable_seq_no,
                 payable_seq_no_desc
          from phases
                   left join pay_seq_4 using (NK_SUBSCRIPTION))
select *, current_timestamp as ETL_CREATEDATETIME
from result
;
