with pickup_arranage_shipment as 
(
    select 
        pickup_order_id
        ,date(FROM_UNIXTIME(seller_order.ctime-3600)) as arrange_pickup_date
        ,staion_table_name.station_name as pickup_station
    from spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_continuous_s0_live as seller_order
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on seller_order.pickup_station_id = staion_table_name.id
    where 
        date(FROM_UNIXTIME(seller_order.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        -- and staion_table_name.station_name = 'FPPIN - พุนพิน (U-412)'
        -- and date(FROM_UNIXTIME(seller_order.ctime-3600)) = date('2022-11-03')
    group by 
        pickup_order_id
        ,date(FROM_UNIXTIME(seller_order.ctime-3600)) 
        ,staion_table_name.station_name 
        -- 2022-11-03
)
,fmhub_lhtransporting_ontime as
(
    select 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
    select
        order_tracking.shipment_id
        ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
        ,order_tracking.status
        ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
    where 
        -- FROM_UNIXTIME(order_tracking.ctime-3600) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '22' hour   
        order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
    )
    where 
        rank_num = 1
        and status = 47
        and time_stamp between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '22' hour
        -- and date(time_stamp) = date('2022-10-21')
        -- and station_name = 'FPPIN - พุนพิน (U-412)'
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '23:00:00.000'
    group by 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,fmhub_lhtransporting_late as
(
    select 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
    select
        order_tracking.shipment_id
        ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
        ,order_tracking.status
        ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
    where 
        -- FROM_UNIXTIME(order_tracking.ctime-3600) between DATE_TRUNC('day', current_timestamp) - interval '45' day + interval '22' hour + interval '01' second and DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second
        order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
    )
    where 
        rank_num = 1
        and status = 47
        and time_stamp between DATE_TRUNC('day', current_timestamp) - interval '45' day + interval '22' hour + interval '01' second and DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second
        and split_part(cast(time_stamp AS varchar),' ',2) between '23:00:01.000' and '23:59:59.000'
    group by 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,FMHub_Received as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status = 42
    group by 
        shipment_id
        ,date_time
)
,order_track_pickup_handover as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status in (32,40)
    group by 
        shipment_id
        ,date_time
)
,order_track_pickup_done as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status in (13,39)
    group by 
        shipment_id
        ,date_time
)
,lost as
(
    select 
        order_track_lost.shipment_id
        ,order_track_lost.date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )order_track_lost
    -- left join fmhub_lhtransporting_ontime
    -- on order_track_lost.shipment_id = fmhub_lhtransporting_ontime.shipment_id
    -- left join fmhub_lhtransporting_late
    -- on order_track_lost.shipment_id = fmhub_lhtransporting_late.shipment_id
    where 
        rank_num = 1
        and order_track_lost.status = 11
    group by 
        order_track_lost.shipment_id
        ,order_track_lost.date_time
)
,created as 
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and  shipment_id = 'SPXTH02997335316B'
    )
    where 
        rank_num = 1
        and status = 0
    group by 
        shipment_id
        ,date_time
)
,Damaged as 
(
    select 
        order_track_damaged.shipment_id
        ,order_track_damaged.date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )order_track_damaged
    -- left join fmhub_lhtransporting_ontime
    -- on order_track_damaged.shipment_id = fmhub_lhtransporting_ontime.shipment_id
    -- left join fmhub_lhtransporting_late
    -- on order_track_damaged.shipment_id = fmhub_lhtransporting_late.shipment_id
    where 
        rank_num = 1
        and order_track_damaged.status = 12
    group by 
        order_track_damaged.shipment_id
        ,order_track_damaged.date_time
)
,Failed_Pickup as 
-- -- SOC_Pickup_Onhold = 30 , FMHub_Pickup_Onhold = 37
(
    select 
        order_track_onhold.shipment_id
        ,order_track_onhold.date_time
        ,pickup_onhold_reason
        ,case 
            when pickup_onhold_reason = '35' then 'No consignment to pick up'
            when pickup_onhold_reason = '36' then 'Postpone pickup date'
            when pickup_onhold_reason = '37' then 'Cannot contact to seller'
            when pickup_onhold_reason = '38' then 'Missed Pickup'
            when pickup_onhold_reason = '56' then 'Booking is cancelled'
            when pickup_onhold_reason = '57' then 'Unacceptable parcel'
            when pickup_onhold_reason = '58' then 'Incorrect pickup information'
            when pickup_onhold_reason = '59' then 'Uncontrollable situation'
            when pickup_onhold_reason = '61' then 'Incorrect AWB'
        end as pickup_onhold_reason_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,try(cast(json_extract(json_parse(content),'$.pickup_onhold_reason') as varchar)) as pickup_onhold_reason
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )order_track_onhold
    -- left join fmhub_lhtransporting_ontime
    -- on order_track_onhold.shipment_id = fmhub_lhtransporting_ontime.shipment_id
    -- left join fmhub_lhtransporting_late
    -- on order_track_onhold.shipment_id = fmhub_lhtransporting_late.shipment_id
    where 
        rank_num = 1
        and order_track_onhold.status in (30,37)
    group by 
         order_track_onhold.shipment_id
        ,order_track_onhold.date_time
        ,pickup_onhold_reason
)
,Identify_onhold_reason as
(
    select 
        arrange_pickup_date
        ,pickup_arranage_shipment.pickup_station
        ,pickup_onhold_reason
        ,cast(count(pickup_onhold_reason) as varchar ) as count_pickup_onhold_reason
        ,array_join(array[pickup_onhold_reason_name,cast(count(pickup_onhold_reason) as varchar )], ' = ') as cause_onhold
    from pickup_arranage_shipment 
    left join Failed_Pickup
    on pickup_arranage_shipment.pickup_order_id = Failed_Pickup.shipment_id
    where 
        pickup_onhold_reason is not null
    group by 
        arrange_pickup_date
        ,pickup_arranage_shipment.pickup_station
        ,pickup_onhold_reason
        ,pickup_onhold_reason_name
    order by 
        pickup_onhold_reason desc
)
,Cancelled as 
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status = 3
    group by 
        shipment_id
        ,date_time
)
,FMHub_LHPacked as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status = 46
    group by 
        shipment_id
        ,date_time
)
,Return_all as 
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (13,32,39,40,42,47,11,12,0,30,37,3,46,10,58,67)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status in (10,58,67)
    group by 
        shipment_id
        ,date_time
)
select 
    pickup_arranage_shipment.arrange_pickup_date
    ,pickup_arranage_shipment.pickup_station
    ,count(pickup_arranage_shipment.pickup_station) as total_arrange
    ,count(fmhub_lhtransporting_ontime.shipment_id) as total_fm_ting_ontime
    ,count(fmhub_lhtransporting_late.shipment_id) as total_fm_ting_late
    ,count(order_track_pickup_done.shipment_id) as total_pickup_done
    ,count(order_track_pickup_handover.shipment_id) as total_pickup_handover
    ,count(created.shipment_id) as total_created
    ,count(FMHub_Received.shipment_id) as total_FMHub_Received
    ,count(FMHub_LHPacked.shipment_id) as total_FMHub_LHPacked
    ,count(Return_all.shipment_id) as total_Return
    ,count(lost.shipment_id) as total_lost
    ,count(Damaged.shipment_id) as total_Damaged
    ,count(Cancelled.shipment_id) as total_Cancelled
    ,count(Failed_Pickup.shipment_id) as total_Onhold
    ,count(case when Failed_Pickup.pickup_onhold_reason = '35' then 1 end) as "No consignment to pick up"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '36' then 1 end) as "Postpone pickup date"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '37' then 1 end) as "Cannot contact to seller"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '38' then 1 end) as "Missed Pickup"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '56' then 1 end) as "Booking is cancelled"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '57' then 1 end) as "Unacceptable parcel"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '58' then 1 end) as "Incorrect pickup information"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '59' then 1 end) as "Uncontrollable situation"
    ,count(case when Failed_Pickup.pickup_onhold_reason = '61' then 1 end) as "Incorrect AWB"
    ,array_join(slice(array_agg(distinct cause_onhold),1,20),',') as count_pickup_onhold_reason_all
from pickup_arranage_shipment
left join fmhub_lhtransporting_ontime
on pickup_arranage_shipment.pickup_order_id = fmhub_lhtransporting_ontime.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = date(fmhub_lhtransporting_ontime.time_stamp)
left join fmhub_lhtransporting_late
on pickup_arranage_shipment.pickup_order_id = fmhub_lhtransporting_late.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = date(fmhub_lhtransporting_late.time_stamp)
left join FMHub_Received
on pickup_arranage_shipment.pickup_order_id = FMHub_Received.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = FMHub_Received.date_time
left join order_track_pickup_handover
on pickup_arranage_shipment.pickup_order_id = order_track_pickup_handover.shipment_id 
and pickup_arranage_shipment.arrange_pickup_date = order_track_pickup_handover.date_time
left join order_track_pickup_done
on pickup_arranage_shipment.pickup_order_id = order_track_pickup_done.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = order_track_pickup_done.date_time
left join lost
on pickup_arranage_shipment.pickup_order_id = lost.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = lost.date_time
left join created
on pickup_arranage_shipment.pickup_order_id = created.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = created.date_time
left join Damaged
on pickup_arranage_shipment.pickup_order_id = Damaged.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = Damaged.date_time
left join Failed_Pickup
on pickup_arranage_shipment.pickup_order_id = Failed_Pickup.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = Failed_Pickup.date_time
left join Cancelled
on pickup_arranage_shipment.pickup_order_id = Cancelled.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = Cancelled.date_time
left join FMHub_LHPacked
on pickup_arranage_shipment.pickup_order_id = FMHub_LHPacked.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = FMHub_LHPacked.date_time
left join Return_all
on pickup_arranage_shipment.pickup_order_id = Return_all.shipment_id
and pickup_arranage_shipment.arrange_pickup_date = Return_all.date_time
left join Identify_onhold_reason
on pickup_arranage_shipment.arrange_pickup_date = Identify_onhold_reason.arrange_pickup_date
and pickup_arranage_shipment.pickup_station  = Identify_onhold_reason.pickup_station
where  
    pickup_arranage_shipment.pickup_station is not null
    and pickup_arranage_shipment.arrange_pickup_date between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    pickup_arranage_shipment.arrange_pickup_date
    ,pickup_arranage_shipment.pickup_station
order by
    pickup_arranage_shipment.arrange_pickup_date desc
-- ได้สูงสุด 11 วัน