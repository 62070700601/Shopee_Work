-- Main query
-- นับ avg Return_SOC_LHTransported
-- set enable_unaligned_array_join = 1
with Return_SOC_LHTransported as 
(
select
    shipment_id
    ,destination_station
    ,time_stamp
    ,chargeable_weight_in_kg
    ,weight_not_zero
from 
    (         
    select 
        fleet_order_Return_SOC_LHTransported.shipment_id
        ,staion_table_name.station_name as destination_station
        ,fleet_order_Return_SOC_LHTransported.time_stamp
        ,chargeable_weight_in_kg
        ,if(chargeable_weight_in_kg !=0,chargeable_weight_in_kg,null) as weight_not_zero
        ,row_number() over (partition by fleet_order_Return_SOC_LHTransported.shipment_id,staion_table_name.station_name order by time_stamp asc) as row_number 
    from 
        (
        select 
            order_track.shipment_id
            ,order_track.station_id
            ,staion_table_name.station_name
            ,order_track.status
            ,FROM_UNIXTIME(order_track.ctime-3600) as time_stamp
            ,lag(order_track.status) over (partition by shipment_id order by FROM_UNIXTIME(order_track.ctime-3600) desc) as next_status
            ,lead(order_track.status) over (partition by shipment_id order by FROM_UNIXTIME(order_track.ctime-3600) desc) as previous_last_status
            ,lag(order_track.station_id) over (partition by shipment_id order by FROM_UNIXTIME(order_track.ctime-3600) desc) as destination_station1
            ,lead(order_track.station_id) over (partition by shipment_id order by FROM_UNIXTIME(order_track.ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
        left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
        on order_track.station_id = staion_table_name.id
        where 
            date(FROM_UNIXTIME(order_track.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second ) 
            and order_track.status in (65,10)
        order by 
            FROM_UNIXTIME(order_track.ctime-3600) desc
        ) fleet_order_Return_SOC_LHTransported
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Return_SOC_LHTransported.destination_station1 = staion_table_name.id
    left join thopsbi_spx.dwd_pub_shipment_info_df_th
    on fleet_order_Return_SOC_LHTransported.shipment_id = thopsbi_spx.dwd_pub_shipment_info_df_th.shipment_id
    where 
        date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and fleet_order_Return_SOC_LHTransported.status = 65
        and fleet_order_Return_SOC_LHTransported.station_name in ('SOCE','CERC','SORC-A','NORC-A','NORC-B','NERC-A','NERC-B','SORC-B')
        and fleet_order_Return_SOC_LHTransported.next_status = 10
    group by 
        fleet_order_Return_SOC_LHTransported.shipment_id
        ,staion_table_name.station_name
        ,fleet_order_Return_SOC_LHTransported.time_stamp
        ,chargeable_weight_in_kg
        ,if(chargeable_weight_in_kg !=0,chargeable_weight_in_kg,null)
    order by 
        fleet_order_Return_SOC_LHTransported.time_stamp asc
    )
where 
    row_number = 1
group by 
    shipment_id
    ,destination_station
    ,time_stamp
    ,chargeable_weight_in_kg
    ,weight_not_zero
)
,Return_LMHub_Received as 
(
select
    shipment_id
    ,station_name
    ,time_stamp 
from 
    (
     select 
        fleet_order_Return_LMHub_Received.shipment_id
        ,fleet_order_Return_LMHub_Received.station_name
        ,fleet_order_Return_LMHub_Received.time_stamp
        ,fleet_order_Return_LMHub_Received.status
        ,row_number() over (partition by fleet_order_Return_LMHub_Received.shipment_id,fleet_order_Return_LMHub_Received.station_name order by time_stamp asc) as row_number 
    from 
    (
        select 
            order_track.shipment_id
            ,order_track.station_id
            ,staion_table_name.station_name
            ,order_track.status
            ,FROM_UNIXTIME(order_track.ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(order_track.ctime-3600) asc) as rank_num
            ,lead(order_track.status) over (partition by shipment_id order by FROM_UNIXTIME(order_track.ctime-3600) desc) as previous_last_status
            ,lead(order_track.station_id) over (partition by shipment_id order by FROM_UNIXTIME(order_track.ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
        left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
        on station_id = staion_table_name.id
        where 
            date(FROM_UNIXTIME(order_track.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second ) 
            and order_track.status in (65,10)
        order by FROM_UNIXTIME(order_track.ctime-3600) desc
    ) fleet_order_Return_LMHub_Received
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on station_name_lead = staion_table_name.id
    where 
        previous_last_status = 65
        and staion_table_name.station_name in ('SOCE','CERC','SORC-A','NORC-A','NORC-B','NERC-A','NERC-B','SORC-B')
        and fleet_order_Return_LMHub_Received.status = 10
        and date(time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        fleet_order_Return_LMHub_Received.shipment_id
        ,fleet_order_Return_LMHub_Received.station_name
        ,fleet_order_Return_LMHub_Received.time_stamp
        ,fleet_order_Return_LMHub_Received.status
    order by 
        time_stamp asc
    )
where 
    row_number = 1
group by 
    shipment_id
    ,station_name
    ,time_stamp 
)
,lost as 
(
    select 
        shipment_id
        ,staion_table_name.station_name
        ,time_stamp
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (65,10,11)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_lost
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on station_name_lead = staion_table_name.id
    where 
        previous_last_status = 10
        and fleet_order_lost.status = 11
        and date(time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,staion_table_name.station_name
        ,time_stamp
    order by 
        time_stamp asc
)
,lost_after_Return_SOC_LHTransported as
(
    select 
        lost.shipment_id
        ,lost.station_name
        ,lost.time_stamp
    from Return_LMHub_Received
    inner join lost
    on Return_LMHub_Received.shipment_id = lost.shipment_id
    and Return_LMHub_Received.station_name = lost.station_name 
    group by 
        lost.shipment_id
        ,lost.station_name
        ,lost.time_stamp
)
,Return_LMHub_Returned as
-- end status
(
  select 
        shipment_id
        ,staion_table_name.station_name
        ,time_stamp
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second ) 
            and status in (65,125)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Return_LMHub_Returned
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on station_id = staion_table_name.id
    where 
        previous_last_status = 65
        and fleet_order_Return_LMHub_Returned.status = 125
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second ) 
    group by 
        shipment_id
        ,staion_table_name.station_name
        ,time_stamp
    order by 
        time_stamp asc
) 
,Return_LMHub_Onhold as
(
     select 
        shipment_id
        ,staion_table_name.station_name
        ,time_stamp
        ,on_hold_reason
        ,on_hold_reason_name
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,on_hold_reason
            ,case 
                when on_hold_reason = 0 then 'No Onhold Reason'
                when on_hold_reason = 40 then 'Return seller is close'
                when on_hold_reason = 41 then 'Seller change addresss'
                when on_hold_reason = 43 then 'Seller is not contactable'
                when on_hold_reason = 44 then 'Seller request reschedule'
                when on_hold_reason = 45 then 'Return missorted parcel'
                when on_hold_reason = 46 then 'Return traffic accident'
                when on_hold_reason = 47 then 'Return disaster, heavy rain, flooding'
                when on_hold_reason = 48 then 'Return parcel lost'
                when on_hold_reason = 49 then 'Damaged return parcel'
                when on_hold_reason = 50 then 'Rejected by return seller'
                when on_hold_reason = 51 then 'Insufficient time for return'
                when on_hold_reason = 52 then 'Return location is inaccessible'
                when on_hold_reason = 53 then 'Return seller number not in service'
                when on_hold_reason = 62 then 'Cannot contact the store'
                when on_hold_reason = 63 then 'Ops support on-hold'
            end as on_hold_reason_name
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second ) 
            -- date(event_timestamp) between date('2022-09-05 00:00:00.000') and date('2022-09-11 00:00:00.000')
            and status in (65,126)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Onhold
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on station_id = staion_table_name.id
    where 
        fleet_order_Onhold.status = 126
        and previous_last_status = 65
        and date(time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,staion_table_name.station_name
        ,time_stamp
        ,on_hold_reason
        ,on_hold_reason_name
    order by 
        date(time_stamp) asc
),Return_LMHub_Onhold_Only as 
(
select 
    Return_LMHub_Onhold.shipment_id
    ,Return_LMHub_Onhold.station_name
    ,Return_LMHub_Onhold.time_stamp
    ,Return_LMHub_Onhold.on_hold_reason
    ,Return_LMHub_Onhold.on_hold_reason_name
from Return_LMHub_Onhold
left join lost_after_Return_SOC_LHTransported
on Return_LMHub_Onhold.shipment_id = lost_after_Return_SOC_LHTransported.shipment_id
and Return_LMHub_Onhold.station_name = lost_after_Return_SOC_LHTransported.station_name
left join Return_LMHub_Returned
on Return_LMHub_Onhold.shipment_id = Return_LMHub_Returned.shipment_id
and Return_LMHub_Onhold.station_name = Return_LMHub_Returned.station_name
where 
    lost_after_Return_SOC_LHTransported.shipment_id is null
    and Return_LMHub_Returned.shipment_id is null
    and date(Return_LMHub_Onhold.time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    Return_LMHub_Onhold.shipment_id
    ,Return_LMHub_Onhold.station_name
    ,Return_LMHub_Onhold.time_stamp
    ,Return_LMHub_Onhold.on_hold_reason
    ,Return_LMHub_Onhold.on_hold_reason_name
)
select 
    date(Return_SOC_LHTransported.time_stamp) as report_date
    ,Return_SOC_LHTransported.destination_station
    ,count(Return_SOC_LHTransported.shipment_id) as count_SOC_LHTransported
    ,count(Return_LMHub_Received.shipment_id) as count_Return_LMHub_Received
    ,count(lost_after_Return_SOC_LHTransported.shipment_id) as count_lost
    ,count(Return_LMHub_Returned.shipment_id)  as count_Return_LMHub_Returned
    ,count(Return_LMHub_Onhold_Only.shipment_id) as count_Onhold_Only
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 0 then 1 end) as "count no onhold reason"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 40 then 1 end) as "count return seller is close"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 41 then 1 end) as "count seller change address"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 43 then 1 end) as "count seller is not contactable"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 44 then 1 end) as "count seller request reschedule"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 45 then 1 end) as "count return missorted parcel"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 46 then 1 end) as "count return traffic accident"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 47 then 1 end) as "count return disaster, heavy rain, flooding"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 48 then 1 end) as "count return parcel lost"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 49 then 1 end) as "count damaged return parcel"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 50 then 1 end) as "count rejected by return seller"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 51 then 1 end) as "count Insufficient time for return"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 52 then 1 end) as "count return location is inaccessible"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 53 then 1 end) as "count return seller number not in service"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 62 then 1 end) as "count cannot contact the store"
    ,count(case when Return_LMHub_Onhold_Only.on_hold_reason = 63 then 1 end) as "count Ops support on-hold"
    ,avg(weight_not_zero) as avg_Return_SOC_LHTransported
    -- ,array_join(slice(array_agg(distinct Return_SOC_LHTransported.shipment_id),1,3),',') as shipment_id_example -- result is varchar
    ,slice(array_agg(distinct Return_SOC_LHTransported.shipment_id),1,3) as shipment_id_example -- result is array
from Return_SOC_LHTransported
left join Return_LMHub_Received
on Return_SOC_LHTransported.shipment_id = Return_LMHub_Received.shipment_id 
and Return_SOC_LHTransported.destination_station = Return_LMHub_Received.station_name
left join lost_after_Return_SOC_LHTransported
on Return_SOC_LHTransported.shipment_id = lost_after_Return_SOC_LHTransported.shipment_id 
and Return_SOC_LHTransported.destination_station = lost_after_Return_SOC_LHTransported.station_name
left join Return_LMHub_Returned
on Return_SOC_LHTransported.shipment_id = Return_LMHub_Returned.shipment_id 
and Return_SOC_LHTransported.destination_station = Return_LMHub_Returned.station_name
left join Return_LMHub_Onhold_Only
on Return_SOC_LHTransported.shipment_id = Return_LMHub_Onhold_Only.shipment_id 
and Return_SOC_LHTransported.destination_station = Return_LMHub_Onhold_Only.station_name
group by    
    date(Return_SOC_LHTransported.time_stamp)
    ,Return_SOC_LHTransported.destination_station
order by 
    date(Return_SOC_LHTransported.time_stamp) desc