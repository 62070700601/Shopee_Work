/*
arrang => fm_recived => soce
จะแบ่งเป็น 2 ก้อน
1.(FM/SOC)Hub_Pickup_Done เป็น Universe pickup_done มาเท่าไหร่ และ FMHub_Received [D0] / SOC_Received [D0] เป็นเท่าไหร่
    (FM/SOC)Hub_Pickup_Done cutoff => 23:59 Universe
    FMHub_Received [D0] / SOC_Received On time [D0] => 21:59
2.fm_recived ไปเท่าไหร่ แล้ว fm_ting ไปเท่าไหร่ ถ้าไม่มี fm_ting ก็จับเหมือนเดิมให้เป็น status อื่นๆ
โดยอันนี้จะจับจาก hub โดยตรง
    Universe FMHub_Received [D0] => cut_off คือ 23.59
    FMHub_LHTransporting_ontime [D0] => 22.59
    FMHub_Received [D0] => 21.59
เวลาทั้งหทด 45 days
case เปลี่ยนมาใช้ rank_num
Run ทุกวัน 24.00
*/
-- case 1 (FM/SOC)Hub_Pickup_Done
with FM_SOCHub_Pickup_Done as
-- SOC_Pickup_Done = 13
-- FMHub_Pickup_Done = 39
-- FMHub_Received [D0] = 42
-- SOC_Received [D0] = 8
-- Cancelled = 3
-- Damaged = 12
-- Lost = 11
-- Retunr_all in ((10,58,67))
-- SOC_Pickup_Handedover = 32
-- FMHub_Pickup_Handedover = 40
(
select 
    shipment_id
        ,date_time
        ,station_name
        -- ,driver_name
    from 
    (
        select  
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,ops_fm_driver_tag
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        left join thopsbi_spx.dim_driver_info_di_th as driver_dict
        on order_tracking.user_id = cast(driver_dict.driver_id as int)
        where 
            order_tracking.status in (13,39)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1
        and status in (13,39)
        and ops_fm_driver_tag LIKE '%2W' or ops_fm_driver_tag LIKE '%4W'
        -- and date_time between date('2022-12-06') and date('2022-12-07')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name
        -- ,driver_name
)
,FMHub_Received_ontime as
(
    select 
        shipment_id
        ,time_stamp
        ,status
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (42,8)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1 
        and status in (42,8)
        -- and date(time_stamp) between date('2022-12-06') and date('2022-12-07')
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
    group by 
        shipment_id
        ,time_stamp
        ,status
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,FMHub_Received_latetime as
(
    select 
        shipment_id
        ,time_stamp
        ,status
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (42,8)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1 
        and status in (42,8)
        -- and date(time_stamp) between date('2022-12-06') and date('2022-12-07')
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
    group by 
        shipment_id
        ,time_stamp
        ,status
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,FM_SOC_Pickup_Handedover as 
(
    select 
        shipment_id
        ,date_time
        ,status
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (42,8,32,40,3,12,11,10,58,67)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1 
        and status in (32,40)
        -- and date_time between date('2022-12-06') and date('2022-12-07')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,status
)
,Cancelled as 
(
    select 
        shipment_id
        ,date_time
        ,status
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (42,8,32,40,3,12,11,10,58,67)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1 
        and status = 3
        -- and date_time between date('2022-12-06') and date('2022-12-07')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,status
)
,Damaged as 
(
    select 
        shipment_id
        ,date_time
        ,status
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (42,8,32,40,3,12,11,10,58,67)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1 
        and status = 12
        -- and date_time between date('2022-12-06') and date('2022-12-07')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,status
)
,Lost as 
(
    select 
        shipment_id
        ,date_time
        ,status
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (42,8,32,40,3,12,11,10,58,67)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1 
        and status = 11
        -- and date_time between date('2022-12-06') and date('2022-12-07')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,status
)
,Return_all as 
(
    select 
        shipment_id
        ,date_time
        ,status
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (42,8,32,40,3,12,11,10,58,67)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-12-06') and date('2022-12-07')
    )
    where 
        rank_num = 1 
        and status in (10,58,67)
        -- and date_time between date('2022-12-06') and date('2022-12-07')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,status
)
select 
    FM_SOCHub_Pickup_Done.date_time
    ,FM_SOCHub_Pickup_Done.station_name
    ,count(FM_SOCHub_Pickup_Done.shipment_id) as total_FM_SOCHub_Pickup_Done
    ,count(FMHub_Received_ontime.shipment_id) as total_FMHub_Received_ontime
    ,count(FMHub_Received_latetime.shipment_id) as total_FMHub_Received_late
    ,count(FM_SOC_Pickup_Handedover.shipment_id) as total_FM_SOC_Pickup_Handedover
    ,count(Cancelled.shipment_id) as total_Cancelled
    ,count(Damaged.shipment_id) as total_Damaged
    ,count(lost.shipment_id) as total_lost
    ,count(Return_all.shipment_id) as total_Return_all
    ,count(FM_SOCHub_Pickup_Done.shipment_id) + count(FM_SOC_Pickup_Handedover.shipment_id) as Pickup_backlog
from FM_SOCHub_Pickup_Done
left join FMHub_Received_ontime
on FM_SOCHub_Pickup_Done.shipment_id = FMHub_Received_ontime.shipment_id
and FM_SOCHub_Pickup_Done.date_time = date(FMHub_Received_ontime.time_stamp)
left join FMHub_Received_latetime
on FM_SOCHub_Pickup_Done.shipment_id = FMHub_Received_latetime.shipment_id
and FM_SOCHub_Pickup_Done.date_time = date(FMHub_Received_latetime.time_stamp)
left join FM_SOC_Pickup_Handedover
on FM_SOCHub_Pickup_Done.shipment_id = FM_SOC_Pickup_Handedover.shipment_id
and FM_SOCHub_Pickup_Done.date_time = date(FM_SOC_Pickup_Handedover.date_time)
left join Cancelled
on FM_SOCHub_Pickup_Done.shipment_id = Cancelled.shipment_id
and FM_SOCHub_Pickup_Done.date_time = Cancelled.date_time
left join Damaged
on FM_SOCHub_Pickup_Done.shipment_id = Damaged.shipment_id
and FM_SOCHub_Pickup_Done.date_time = Damaged.date_time
left join lost
on FM_SOCHub_Pickup_Done.shipment_id = lost.shipment_id
and FM_SOCHub_Pickup_Done.date_time = lost.date_time
left join Return_all
on FM_SOCHub_Pickup_Done.shipment_id = Return_all.shipment_id
and FM_SOCHub_Pickup_Done.date_time = Return_all.date_time
where  
    -- FM_SOCHub_Pickup_Done.date_time between date('2022-12-06') and date('2022-12-07')
    FM_SOCHub_Pickup_Done.date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    FM_SOCHub_Pickup_Done.date_time
    ,FM_SOCHub_Pickup_Done.station_name
order by
    FM_SOCHub_Pickup_Done.date_time desc
