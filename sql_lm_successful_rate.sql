with SOC_LHTransported as 
(
      select 
        fleet_order_LMHub_SOC_LHTransported.shipment_id
        ,staion_table_name.station_name
        ,fleet_order_LMHub_SOC_LHTransported.status
        ,time_stamp
        ,chargeable_weight_in_kg
        ,if(chargeable_weight_in_kg !=0,chargeable_weight_in_kg,null) as weight_not_zero
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status in (36,1)
            and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second)
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )fleet_order_LMHub_SOC_LHTransported
    left join thopsbi_spx.dwd_pub_shipment_info_df_th
    on fleet_order_LMHub_SOC_LHTransported.shipment_id = thopsbi_spx.dwd_pub_shipment_info_df_th.shipment_id
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_LMHub_SOC_LHTransported.destination_station = staion_table_name.id
    where 
        date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and fleet_order_LMHub_SOC_LHTransported.status = 36
        and staion_table_name.station_name not in ('SOCE','CERC','SORC-A','NORC-A','NORC-B','NERC-A','NERC-B')
    group by 
         fleet_order_LMHub_SOC_LHTransported.shipment_id
        ,staion_table_name.station_name
        ,fleet_order_LMHub_SOC_LHTransported.status
        ,time_stamp 
        ,chargeable_weight_in_kg
        ,if(chargeable_weight_in_kg !=0,chargeable_weight_in_kg,null)
    order by 
        time_stamp asc
)
,LMHub_Received as 
(
    select 
        shipment_id
        ,staion_table_name.station_name
        ,time_stamp 
        ,fleet_order_LMHub_Received.status
        
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (1,36) 
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_LMHub_Received
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_LMHub_Received.station_id = staion_table_name.id
    where 
        previous_last_status = 36
        and fleet_order_LMHub_Received.status = 1 
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,station_name
        ,fleet_order_LMHub_Received.status
        ,time_stamp
    order by 
        time_stamp asc
)
,Delivered as
-- end status
(
    select 
        fleet_order_Delivered.shipment_id
        ,staion_table_name.station_name
        ,time_stamp
        ,fleet_order_Delivered.station_name_lead
        ,chargeable_weight_in_kg
        ,if(chargeable_weight_in_kg !=0,chargeable_weight_in_kg,null) as weight_not_zero
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (1,4)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Delivered
    left join thopsbi_spx.dwd_pub_shipment_info_df_th
    on fleet_order_Delivered.shipment_id = thopsbi_spx.dwd_pub_shipment_info_df_th.shipment_id
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Delivered.station_name_lead = staion_table_name.id
    where 
        fleet_order_Delivered.status = 4
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        fleet_order_Delivered.shipment_id
        ,staion_table_name.station_name
        ,time_stamp
        ,fleet_order_Delivered.station_name_lead
        ,chargeable_weight_in_kg
        ,if(chargeable_weight_in_kg !=0,chargeable_weight_in_kg,null) 
    order by 
        time_stamp asc
)
,Cancelled as
-- end status
(
select 
    shipment_id
    ,station_name_lead
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
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status = 3
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Cancelled
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Cancelled.station_name_lead = staion_table_name.id
    where   
        previous_last_status = 1
        and fleet_order_Cancelled.status = 3
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,station_name_lead
        ,time_stamp
        ,staion_table_name.station_name
    order by 
        time_stamp asc
)
,lost as
-- end status
(
  select 
    shipment_id
    ,station_name_lead
    ,time_stamp
    ,staion_table_name.station_name
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (36,1,11)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_lost
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_lost.station_name_lead = staion_table_name.id
    where 
        previous_last_status = 1
        and fleet_order_lost.status = 11
        and date(time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,station_name_lead
        ,time_stamp
        ,staion_table_name.station_name
    order by 
        time_stamp asc
) 
,Disposed as
-- end status
(
  select 
    shipment_id
    ,station_name_lead
    ,time_stamp
    ,staion_table_name.station_name
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (36,1,26,10)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Disposed
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Disposed.station_name_lead = staion_table_name.id
    where 
        previous_last_status = 1
        and fleet_order_Disposed.status = 26
        and date(time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,station_name_lead
        ,time_stamp
        ,staion_table_name.station_name
    order by 
        time_stamp asc
)
,Damaged as
(
select 
    fleet_order_Damaged.shipment_id
    ,fleet_order_Damaged.station_name_lead
    ,fleet_order_Damaged.time_stamp
    ,staion_table_name.station_name
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (36,1,12,10)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Damaged
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Damaged.station_name_lead = staion_table_name.id
    left join lost
    on fleet_order_Damaged.shipment_id = lost.shipment_id 
    and fleet_order_Damaged.station_name_lead = lost.station_name_lead
    left join Cancelled
    on fleet_order_Damaged.shipment_id = Cancelled.shipment_id 
    and fleet_order_Damaged.station_name_lead = Cancelled.station_name_lead
    left join Disposed
    on fleet_order_Damaged.shipment_id = Disposed.shipment_id 
    and fleet_order_Damaged.station_name_lead = Disposed.station_name_lead
    where 
        fleet_order_Damaged.previous_last_status = 1
        and fleet_order_Damaged.status = 12
        and lost.shipment_id  is null
        and Cancelled.shipment_id is null
        and Disposed.shipment_id is null
        and date(fleet_order_Damaged.time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        fleet_order_Damaged.shipment_id
        ,fleet_order_Damaged.station_name_lead
        ,fleet_order_Damaged.time_stamp
        ,staion_table_name.station_name
    order by 
        fleet_order_Damaged.time_stamp asc
)
,Onhold as
(
    select 
        shipment_id
        ,station_name_lead
        ,time_stamp
        ,fleet_order_Onhold.status
        ,staion_table_name.station_name
        ,on_hold_reason
        ,on_hold_reason_name
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,on_hold_reason
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
            ,case 
                when on_hold_reason = 0 then 'No Onhold Reason'
                when on_hold_reason = 17 then 'Buyer Request Reschedule'
                when on_hold_reason = 18 then 'Cannot contact recipient upon arrival'
                when on_hold_reason = 19 then 'Hub cannot contact recipient'
                when on_hold_reason = 20 then 'Number not in service'
                when on_hold_reason = 21 then 'Address is incorrect'
                when on_hold_reason = 22 then 'Location is inaccessible'
                when on_hold_reason = 23 then 'Address is incomplete'
                when on_hold_reason = 24 then 'Insufficient Time for Delivery'
                when on_hold_reason = 25 then 'Rejected By Recipient'
                when on_hold_reason = 26 then 'Recipient Never Placed Order'
                when on_hold_reason = 27 then 'Invalid Conditions'
                when on_hold_reason = 28 then 'Damaged Parcel'
                when on_hold_reason = 29 then 'Parcel Lost'
                when on_hold_reason = 30 then 'Disaster, Heavy Rain, Flooding'
                when on_hold_reason = 31 then 'Traffic Accident'
                when on_hold_reason = 32 then 'Island Delivery Delay'
                when on_hold_reason = 33 then 'Office Order Scheduled For Weekday Delivery'
                when on_hold_reason = 34 then 'Missorted Parcel'
                when on_hold_reason = 54 then 'Order couldn not be delivered successfully'
                when on_hold_reason = 55 then 'Out of serviceable area'
                when on_hold_reason = 79 then 'Shipment is under investigation'
            end as on_hold_reason_name
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (1,5)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Onhold
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Onhold.station_name_lead = staion_table_name.id
    where 
        fleet_order_Onhold.status = 5
        and date(time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,station_name_lead
        ,time_stamp
        ,fleet_order_Onhold.status
        ,staion_table_name.station_name
        ,on_hold_reason
        ,on_hold_reason_name
    order by 
        date(time_stamp) asc
)
,Return_LMHub_Received as 
(
    select 
        fleet_order_Return_LMHub_Received.shipment_id
        ,fleet_order_Return_LMHub_Received.station_id
        ,staion_table_name.station_name
        ,fleet_order_Return_LMHub_Received.time_stamp
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (1,10)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Return_LMHub_Received
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Return_LMHub_Received.station_id = staion_table_name.id
    left join lost
    on fleet_order_Return_LMHub_Received.shipment_id = lost.shipment_id
    and fleet_order_Return_LMHub_Received.station_id = lost.station_name_lead
    left join Delivered
    on fleet_order_Return_LMHub_Received.shipment_id = Delivered.shipment_id
    and fleet_order_Return_LMHub_Received.station_id = Delivered.station_name_lead
    left join Cancelled
    on fleet_order_Return_LMHub_Received.shipment_id = Cancelled.shipment_id
    and fleet_order_Return_LMHub_Received.station_id = Cancelled.station_name_lead
    left join Disposed
    on fleet_order_Return_LMHub_Received.shipment_id = Disposed.shipment_id 
    and fleet_order_Return_LMHub_Received.station_id = Disposed.station_name_lead
    where 
        fleet_order_Return_LMHub_Received.previous_last_status = 1
        and fleet_order_Return_LMHub_Received.status = 10
        and date(fleet_order_Return_LMHub_Received.time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and lost.shipment_id is null
        and Delivered.shipment_id is null
        and Cancelled.shipment_id is null
        and Disposed.shipment_id is null
    group by 
        fleet_order_Return_LMHub_Received.shipment_id
        ,staion_table_name.station_name
        ,fleet_order_Return_LMHub_Received.time_stamp
        ,fleet_order_Return_LMHub_Received.station_id
    order by 
        fleet_order_Return_LMHub_Received.time_stamp asc
)
,Exception_LHPacking as
(
    select 
        fleet_order_Exception_LHPacking.shipment_id
        ,fleet_order_Exception_LHPacking.station_id
        ,staion_table_name.station_name
        ,fleet_order_Exception_LHPacking.time_stamp
    from 
    (
        select 
            shipment_id
            ,station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
            ,lead(status) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as previous_last_status
            ,lag(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as destination_station
            ,lead(station_id) over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as station_name_lead
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            and status in (36,1,84,233)
        order by FROM_UNIXTIME(ctime-3600) desc
    ) fleet_order_Exception_LHPacking
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Exception_LHPacking.station_id = staion_table_name.id
    left join lost
    on fleet_order_Exception_LHPacking.shipment_id = lost.shipment_id
    and fleet_order_Exception_LHPacking.station_id = lost.station_name_lead
    left join Delivered
    on fleet_order_Exception_LHPacking.shipment_id = Delivered.shipment_id
    and fleet_order_Exception_LHPacking.station_id = Delivered.station_name_lead
    left join Cancelled
    on fleet_order_Exception_LHPacking.shipment_id = Cancelled.shipment_id
    and fleet_order_Exception_LHPacking.station_id = Cancelled.station_name_lead
    left join Disposed
    on fleet_order_Exception_LHPacking.shipment_id = Disposed.shipment_id 
    and fleet_order_Exception_LHPacking.station_id = Disposed.station_name_lead
    left join Return_LMHub_Received
    on fleet_order_Exception_LHPacking.shipment_id = Return_LMHub_Received.shipment_id 
    and fleet_order_Exception_LHPacking.station_id = Return_LMHub_Received.station_id
    where 
        fleet_order_Exception_LHPacking.previous_last_status = 1
        and fleet_order_Exception_LHPacking.status in (84,233)
        and date(fleet_order_Exception_LHPacking.time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and lost.shipment_id is null
        and Delivered.shipment_id is null
        and Cancelled.shipment_id is null
        and Disposed.shipment_id is null
        and Return_LMHub_Received.shipment_id is null
    group by 
        fleet_order_Exception_LHPacking.shipment_id
        ,staion_table_name.station_name
        ,fleet_order_Exception_LHPacking.time_stamp
        ,fleet_order_Exception_LHPacking.station_id
    order by 
        fleet_order_Exception_LHPacking.time_stamp asc
)
,Onhold_Only as 
(
select 
    Onhold.shipment_id
    ,Onhold.station_name_lead
    ,Onhold.time_stamp
    ,Onhold.station_name
    ,Onhold.on_hold_reason
    ,Onhold.on_hold_reason_name
from Onhold
left join Return_LMHub_Received
on Onhold.shipment_id = Return_LMHub_Received.shipment_id
and Onhold.station_name_lead = Return_LMHub_Received.station_id
left join Delivered
on Onhold.shipment_id = Delivered.shipment_id
and Onhold.station_name_lead = Delivered.station_name_lead
left join Exception_LHPacking
on Onhold.shipment_id = Exception_LHPacking.shipment_id
and Onhold.station_name_lead = Exception_LHPacking.station_id
left join lost
on Onhold.shipment_id = lost.shipment_id
and Onhold.station_name_lead = lost.station_name_lead
left join Cancelled
on Onhold.shipment_id = Cancelled.shipment_id
and Onhold.station_name_lead = Cancelled.station_name_lead
left join Damaged
on Onhold.shipment_id = Damaged.shipment_id
and Onhold.station_name_lead = Damaged.station_name_lead
left join Disposed
on Onhold.shipment_id = Disposed.shipment_id 
and Onhold.station_name_lead = Disposed.station_name_lead
where 
    Return_LMHub_Received.shipment_id is null
    and Delivered.shipment_id is null
    and Exception_LHPacking.shipment_id is null
    and lost.shipment_id is null
    and Cancelled.shipment_id is null
    and Damaged.shipment_id is null
    and Disposed.shipment_id is null
    and date(Onhold.time_stamp)  between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    Onhold.shipment_id
    ,Onhold.station_name_lead
    ,Onhold.time_stamp
    ,Onhold.station_name
    ,Onhold.on_hold_reason
    ,Onhold.on_hold_reason_name
)
,Return_LMHub_Received_only as
(
  select 
     if(COALESCE(Damaged.time_stamp,Return_LMHub_Received.time_stamp) <= Return_LMHub_Received.time_stamp,Return_LMHub_Received.shipment_id,null) as shipment_id
    ,if(COALESCE(Damaged.time_stamp,Return_LMHub_Received.time_stamp) <= Return_LMHub_Received.time_stamp,Return_LMHub_Received.station_name,null) as station_name
    ,if(COALESCE(Damaged.time_stamp,Return_LMHub_Received.time_stamp) <= Return_LMHub_Received.time_stamp,Return_LMHub_Received.time_stamp,null) as time_stamp
from Return_LMHub_Received
left join Damaged
on Return_LMHub_Received.shipment_id = Damaged.shipment_id 
and Return_LMHub_Received.station_id = Damaged.station_name_lead 
group by 
    if(COALESCE(Damaged.time_stamp,Return_LMHub_Received.time_stamp) <= Return_LMHub_Received.time_stamp,Return_LMHub_Received.shipment_id,null) 
    ,if(COALESCE(Damaged.time_stamp,Return_LMHub_Received.time_stamp) <= Return_LMHub_Received.time_stamp,Return_LMHub_Received.station_name,null)
    ,if(COALESCE(Damaged.time_stamp,Return_LMHub_Received.time_stamp) <= Return_LMHub_Received.time_stamp,Return_LMHub_Received.time_stamp,null) 
)
,Damaged_only as
(
select
    if(COALESCE(Return_LMHub_Received.time_stamp,Damaged.time_stamp) <= Damaged.time_stamp,Damaged.shipment_id,null) as shipment_id
    ,if(COALESCE(Return_LMHub_Received.time_stamp,Damaged.time_stamp) <= Damaged.time_stamp,Damaged.station_name,null) as station_name
    ,if(COALESCE(Return_LMHub_Received.time_stamp,Damaged.time_stamp) <= Damaged.time_stamp,Damaged.time_stamp,null) as time_stamp
from Damaged
left join Return_LMHub_Received
on Damaged.shipment_id = Return_LMHub_Received.shipment_id
and Damaged.station_name_lead = Return_LMHub_Received.station_id
group by 
    if(COALESCE(Return_LMHub_Received.time_stamp,Damaged.time_stamp) <= Damaged.time_stamp,Damaged.shipment_id,null)
    ,if(COALESCE(Return_LMHub_Received.time_stamp,Damaged.time_stamp) <= Damaged.time_stamp,Damaged.station_name,null)
    ,if(COALESCE(Return_LMHub_Received.time_stamp,Damaged.time_stamp) <= Damaged.time_stamp,Damaged.time_stamp,null)
)
-- without cte
select 
    date(SOC_LHTransported.time_stamp) as report_date
    ,SOC_LHTransported.station_name
    ,count(SOC_LHTransported.shipment_id) as count_SOC_LHTransported
    ,count(LMHub_Received.shipment_id) as count_LMHub_Received
    ,count(Delivered.shipment_id) as count_Delivered 
    ,count(Onhold_Only.shipment_id) as count_Onhold_Only 
    ,count(Return_LMHub_Received_only.shipment_id) as count_Return_LMHub_Received
    ,count(Exception_LHPacking.shipment_id) as count_Exception_LHPacking
    ,count(lost.shipment_id) as count_Lost
    ,count(Cancelled.shipment_id) as count_Cancelled
    ,count(Damaged_only.shipment_id) as count_Damaged
    ,count(Disposed.shipment_id) as count_Disposed
    ,count(case when Onhold_Only.on_hold_reason = 0 then 1 end) as "Count No Onhold Reason"
    ,count(case when Onhold_Only.on_hold_reason = 17 then 1 end) as "Count Buyer Request Reschedule"
    ,count(case when Onhold_Only.on_hold_reason = 18 then 1 end) as "Count Cannot contact recipient upon arrival"
    ,count(case when Onhold_Only.on_hold_reason = 19 then 1 end) as "Count Hub cannot contact recipient"
    ,count(case when Onhold_Only.on_hold_reason = 20 then 1 end) as "Count Number not in service"
    ,count(case when Onhold_Only.on_hold_reason = 21 then 1 end) as "Count Address is incorrect"
    ,count(case when Onhold_Only.on_hold_reason = 22 then 1 end) as "Count Location is inaccessible"
    ,count(case when Onhold_Only.on_hold_reason = 23 then 1 end) as "Count Address is incomplete"
    ,count(case when Onhold_Only.on_hold_reason = 24 then 1 end) as "Count Insufficient Time for Delivery"
    ,count(case when Onhold_Only.on_hold_reason = 25 then 1 end) as "Count Rejected by recipient"
    ,count(case when Onhold_Only.on_hold_reason = 26 then 1 end) as "Count Never Placed Order"
    ,count(case when Onhold_Only.on_hold_reason = 27 then 1 end) as "Count Invalid Conditions"
    ,count(case when Onhold_Only.on_hold_reason = 28 then 1 end) as "Count Damaged Parcel"
    ,count(case when Onhold_Only.on_hold_reason = 29 then 1 end) as "Count Parcel lost"
    ,count(case when Onhold_Only.on_hold_reason = 30 then 1 end) as "Count Disaster, Heavy rain, Flooding"
    ,count(case when Onhold_Only.on_hold_reason = 31 then 1 end) as "Count Traffic accident"
    ,count(case when Onhold_Only.on_hold_reason = 32 then 1 end) as "Count Delivery delay "
    ,count(case when Onhold_Only.on_hold_reason = 33 then 1 end) as "Count Office order scheduled for weekday delivery"
    ,count(case when Onhold_Only.on_hold_reason = 34 then 1 end) as "Count Missorted parcel"
    ,count(case when Onhold_Only.on_hold_reason = 54 then 1 end) as "Count Order could'n be delivered successfully"
    ,count(case when Onhold_Only.on_hold_reason = 55 then 1 end) as "Count Out of serviceable area"
    ,count(case when Onhold_Only.on_hold_reason = 79 then 1 end) as "Count Shipment is under investigation"
    ,avg(SOC_LHTransported.weight_not_zero) as avg_SOC_LHTransported
    -- ,avg(Delivered.weight_not_zero) as avg_Delivered
from SOC_LHTransported
left join LMHub_Received
on  SOC_LHTransported.shipment_id = LMHub_Received.shipment_id 
and SOC_LHTransported.station_name = LMHub_Received.station_name
left join Delivered
on SOC_LHTransported.shipment_id = Delivered.shipment_id 
and SOC_LHTransported.station_name = Delivered.station_name
left join Onhold_Only
on SOC_LHTransported.shipment_id = Onhold_Only.shipment_id 
and SOC_LHTransported.station_name = Onhold_Only.station_name
left join Return_LMHub_Received_only
on SOC_LHTransported.shipment_id =  Return_LMHub_Received_only.shipment_id 
and SOC_LHTransported.station_name =  Return_LMHub_Received_only.station_name
left join Exception_LHPacking
on SOC_LHTransported.shipment_id = Exception_LHPacking.shipment_id 
and SOC_LHTransported.station_name = Exception_LHPacking.station_name
left join lost
on SOC_LHTransported.shipment_id = lost.shipment_id 
and SOC_LHTransported.station_name = lost.station_name
left join Cancelled
on SOC_LHTransported.shipment_id = Cancelled.shipment_id 
and SOC_LHTransported.station_name = Cancelled.station_name
left join Damaged_only
on SOC_LHTransported.shipment_id = Damaged_only.shipment_id 
and SOC_LHTransported.station_name = Damaged_only.station_name
left join Disposed
on SOC_LHTransported.shipment_id = Disposed.shipment_id 
and SOC_LHTransported.station_name = Disposed.station_name
group by    
    date(SOC_LHTransported.time_stamp)
    ,SOC_LHTransported.station_name
order by 
    date(SOC_LHTransported.time_stamp) desc