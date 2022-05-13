{{ config(
materialized='table'
) }}

with source as (select NK_CONTACT,
                       min(CREATE_PHASE)                                                      as CREATE_DATE,
                       PAYABLE_SEQ_START_DATE,
                       PAYABLE_SEQ_END_DATE,
                       max(STOP_DATE)                                                         as STOP_DATE,
                       nvl(DAYS_SINCE_PREV_PAYABLE_SEQ, 999)                                  as DAYS_SINCE_PREV_PAYABLE_SEQ,
                       PAYABLE_SEQ_NO,
                       least(DATEADD(days, 55, PAYABLE_SEQ_START_DATE), PAYABLE_SEQ_END_DATE) as ONBOARDING_END_DATE,
                       DATEADD(days, -55, PAYABLE_SEQ_END_DATE)                               as MAX_TERMINATION_DATE
                from   {{ ref('FACT_SUBSCRIPTION_LOG' )}}
                where PAYABLE_SEQ_NO is not null
                  and PAYABLE_SEQ_END_DATE >= '2018-01-01'
                group by 1, 3, 4, 6, 7),
     offboarding as
         /*
            1) The off-boarding phase starts at the date the members gives notice that the last subscription of a sequence should be terminated,
               but no more that 56 days before the END_DATE.
            2) The off-boarding can't start before the START_DATE.
            3) The off-boarding can't start after the END_DATE.
          */
         (select PAYABLE_SEQ_START_DATE,
                 PAYABLE_SEQ_END_DATE,
                 STOP_DATE,
                 PAYABLE_SEQ_NO,
                 MAX_TERMINATION_DATE,
                 NK_CONTACT,
                 greatest(MAX_TERMINATION_DATE, iff(nvl(STOP_DATE, '2099-12-31') > PAYABLE_SEQ_END_DATE, PAYABLE_SEQ_END_DATE, nvl(STOP_DATE, '2099-12-31')), PAYABLE_SEQ_START_DATE) as from_date,
                 PAYABLE_SEQ_END_DATE                                                                                                                                                 as to_date,
                 'OFFBOARDING'                                                                                                                                                        as state
          from source
          where PAYABLE_SEQ_END_DATE != '2099-12-31'
            and nvl(STOP_DATE, '2099-12-31') < PAYABLE_SEQ_END_DATE),
     onboarding as
         /*
            1) The on-boarding phase runs for 56 days from the start of an unbroken payable sequence.
            2) On-boarding will not take place if the previous payable sequence ended within 180 days of the current one.
            3) On-boarding must stop no later than the day before the off-boarding starts.
          */
         (select s.NK_CONTACT,
                 s.PAYABLE_SEQ_NO,
                 s.PAYABLE_SEQ_START_DATE                                                                                                                                               as from_date,
                 nvl(o.from_date, '2099-12-31')                                                                                                                                         as offboarding_from_date,
                 nvl(o.to_date, '2099-12-31')                                                                                                                                           as offboarding_to_date,
                 least(s.ONBOARDING_END_DATE, dateadd(day, -1, offboarding_from_date))                                                                                                  as to_date,
                 'ONBOARDING'                                                                                                                                                           as state,
                 datediff(day, s.PAYABLE_SEQ_START_DATE, least(s.ONBOARDING_END_DATE, dateadd(day, -1, o.from_date))) >= 55                                                             as is_complete,
                 lag(DAYS_SINCE_PREV_PAYABLE_SEQ) over (partition by NK_CONTACT order by PAYABLE_SEQ_NO) > 180                                                                          as prev_should_be_onboarding,
                 lag(datediff(day, s.PAYABLE_SEQ_START_DATE, least(s.ONBOARDING_END_DATE, dateadd(day, -1, o.from_date))) >= 55) over (partition by NK_CONTACT order by PAYABLE_SEQ_NO) as was_prev_completed
          from source s
                   left join offboarding o using (nk_contact, payable_seq_no)
              qualify (s.DAYS_SINCE_PREV_PAYABLE_SEQ > 180
                  or (prev_should_be_onboarding = true and was_prev_completed = false))
                  and (s.PAYABLE_SEQ_START_DATE <= least(s.ONBOARDING_END_DATE, dateadd(day, -1, offboarding_from_date)))),
     active as
         /*
            1) The active phase is whatever is left of the time between the on-boarding and off-boarding phases.
          */
         (select s.NK_CONTACT,
                 s.PAYABLE_SEQ_NO,
                 nvl(dateadd(day, 1, onb.to_date), s.PAYABLE_SEQ_START_DATE)   as from_date,
                 nvl(dateadd(day, -1, offb.from_date), s.PAYABLE_SEQ_END_DATE) as to_date,
                 'ACTIVE'                                                      as state
          from source s
                   left join onboarding onb using (nk_contact, payable_seq_no)
                   left join offboarding offb using (nk_contact, payable_seq_no)
          where nvl(dateadd(day, 1, onb.to_date), s.PAYABLE_SEQ_START_DATE) < nvl(dateadd(day, -1, offb.from_date), s.PAYABLE_SEQ_END_DATE)),
     signup as
         /*
            1) The sign-up phase is the period from the first CREATE_DATE of the payable sequence until the day before the START_DATE.
          */
         (select NK_CONTACT,
                 PAYABLE_SEQ_NO,
                 nvl(lag(PAYABLE_SEQ_END_DATE) over (partition by NK_CONTACT order by PAYABLE_SEQ_NO), '1970-01-01') as prev_end_date,
                 greatest(CREATE_DATE, dateadd(day, 1, prev_end_date))                                               as from_date,
                 dateadd(day, -1, PAYABLE_SEQ_START_DATE)                                                            as to_date,
                 'SIGNUP'                                                                                            as state
          from source
              qualify CREATE_DATE < PAYABLE_SEQ_START_DATE
                  and to_date >= from_date),
     winback as
         /*
            1) The win-back phase starts as soon as a sequence has ended
            2) It lasts for a maximum 6 months or until the day before the first phase of next sequence starts.
          */
         (select s.NK_CONTACT,
                 s.PAYABLE_SEQ_NO,
                 dateadd(day, 1, s.PAYABLE_SEQ_END_DATE)                                                                                                   as from_date,
                 nvl(lead(s.PAYABLE_SEQ_START_DATE) over (partition by s.NK_CONTACT order by s.PAYABLE_SEQ_NO), '2099-12-31')                              as next_payable_seq_start_date,
                 nvl(lead(su.from_date) over (partition by s.NK_CONTACT order by s.PAYABLE_SEQ_NO), '2099-12-31')                                          as next_signup_start_date,
                 least(dateadd(day, 180, s.PAYABLE_SEQ_END_DATE), dateadd(day, -1, next_payable_seq_start_date), dateadd(day, -1, next_signup_start_date)) as to_date,
                 'WINBACK'                                                                                                                                 as state
          from source s
                   left join signup su using (NK_CONTACT, PAYABLE_SEQ_NO)
              qualify PAYABLE_SEQ_END_DATE != '2099-12-31'
                  and least(dateadd(day, 180, s.PAYABLE_SEQ_END_DATE), next_payable_seq_start_date, next_signup_start_date) > dateadd(day, 1, s.PAYABLE_SEQ_END_DATE)),
     totals as (select NK_CONTACT,
                       PAYABLE_SEQ_NO,
                       from_date,
                       to_date,
                       state
                from onboarding
                union
                select NK_CONTACT,
                       PAYABLE_SEQ_NO,
                       from_date,
                       to_date,
                       state
                from offboarding
                union
                select NK_CONTACT,
                       PAYABLE_SEQ_NO,
                       from_date,
                       to_date,
                       state
                from signup
                union
                select NK_CONTACT,
                       PAYABLE_SEQ_NO,
                       from_date,
                       to_date,
                       state
                from winback
                union
                select NK_CONTACT,
                       PAYABLE_SEQ_NO,
                       from_date,
                       to_date,
                       state
                from active)
select *,
       current_timestamp as ETL_CREATEDATETIME
from totals
;