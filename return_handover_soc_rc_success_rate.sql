/* 
โจทย์ใหม่ของ Return_LMHub_Received คือ ตัว Return_LMHub_Received นั้น status ก่อนหน้ามันต้องเป็น status lm_hub_recived
*/
with Return_LMHub_Received as 
-- เพิ่ม Return_FMHub_Received = 67
(
    select 
        order_track_only.shipment_id
        ,time_stamp
        ,order_track_only.date_time
        -- ,order_track_only.station_name
        ,staion_table_name_lag.station_name as station_name_lag
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,lag(order_tracking.status) over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as lag_status
            ,lag(order_tracking.station_id) over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as lag_station
            -- ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
            -- order_tracking.status = 10
            -- shipment_id = 'SPXTH02764162721C'
    ) as order_track_only
    left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name_lag
    on order_track_only.lag_station = staion_table_name_lag.station_id
    where 
        -- rank_num = 1
        order_track_only.status in (67,10)
        and order_track_only.lag_status = 1
        -- and staion_table_name_lag.station_name = 'HSRCH-B - ศรีราชา'
        -- and date_time = date('2022-12-13') 
        and order_track_only.date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        order_track_only.shipment_id
        ,time_stamp
        ,order_track_only.date_time
        -- ,order_track_only.station_name   
        ,staion_table_name_lag.station_name    
)
,Return_LMHub_Received_ontime as 
(
    select
        shipment_id
        ,date_time
        ,station_name_lag
    from Return_LMHub_Received
    where
        split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
    group by 
        shipment_id
        ,date_time
        ,station_name_lag   
)
,Return_LMHub_Received_late as 
(
    select
        shipment_id
        ,date_time
        ,station_name_lag
    from Return_LMHub_Received
    where
        split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
    group by 
        shipment_id
        ,date_time
        ,station_name_lag     
)
-- ,Return_LMHub_Received_ontime as 
-- (
--     select 
--         shipment_id
--         ,date(time_stamp) as date_time
--         ,station_name
--     from 
--     (
--         select 
--             order_tracking.shipment_id
--             ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
--             ,order_tracking.status
--             ,staion_table_name.station_name
--             ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
--         from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
--         left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
--         on order_tracking.station_id = staion_table_name.station_id
--         where 
--             order_tracking.status = 10
--     )
--     where 
--         rank_num = 1
--         and status = 10
--         and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
--         -- and date(time_stamp) between date('2022-12-13') and date('2022-12-14')
--         -- and station_name = 'HSRCH-B - ศรีราชา'
--         and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
--     group by 
--         shipment_id
--         ,date(time_stamp)
--         ,station_name   
-- )

-- ,Return_LMHub_Received_late as 
-- (
--     select 
--         shipment_id
--         ,date(time_stamp) as date_time
--         ,station_name
--     from 
--     (
--         select 
--             order_tracking.shipment_id
--             ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
--             ,order_tracking.status
--             ,staion_table_name.station_name
--             ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
--         from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
--         left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
--         on order_tracking.station_id = staion_table_name.station_id
--         where 
--             order_tracking.status = 10
--     )
--     where 
--         rank_num = 1
--         and status = 10
--         and split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
--         -- and date(time_stamp) between date('2022-12-13') and date('2022-12-14')
--         -- and station_name = 'HSRCH-B - ศรีราชา'
--         and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
--     group by 
--         shipment_id
--         ,date(time_stamp)
--         ,station_name   
-- )
,Return_LMHub_Packing as 
-- เพิ่ม Return_FMHub_Packing = 68 
(
    select 
        shipment_id
        ,date_time
        ,station_name
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
            order_tracking.status in (52,53,54,55,56,11,12,68,69,70,71,235)
    )
    where 
        rank_num = 1
        and status in (52,68)
        -- and station_name = 'FCPON - ชุมพร (U-402)'
        -- and date_time between date('2022-12-13') and date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name   
)
,Return_LMHub_Packed as 
-- เพิ่ม Return_FMHub_Packed = 69
(
    select 
        shipment_id
        ,date_time
        ,station_name
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
            order_tracking.status in (52,53,54,55,56,11,12,68,69,70,71,235)
    )
    where 
        rank_num = 1
        and status in (53,69)
        -- and station_name = 'FCPON - ชุมพร (U-402)'
        -- and date_time between date('2022-12-13') and date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name   
)
,Return_LMHub_LHPacking as 
-- เพิ่ม Return_FMHub_LHPacking = 70
(
    select 
        shipment_id
        ,date_time
        ,station_name
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
            order_tracking.status in (52,53,54,55,56,11,12,68,69,70,71,235)
    )
    where 
        rank_num = 1
        and status in (54,70)
        -- and station_name = 'FCPON - ชุมพร (U-402)'
        -- and date_time between date('2022-12-13') and date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name   
)
,Return_LMHub_LHPacked as 
-- เพิ่ม Return_FMHub_LHPacked = 71
(
    select 
        shipment_id
        ,date_time
        ,station_name
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
            order_tracking.status in (52,53,54,55,56,11,12,68,69,70,71,235)
    )
    where 
        rank_num = 1
        and status in (55,71)
        -- and station_name = 'FCPON - ชุมพร (U-402)'
        -- and date_time between date('2022-12-13') and date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name   
)
-- ,Return_LMHub_LHTransporting as 
-- (
--     select 
--         shipment_id
--         ,date(time_stamp) as date_time
--         ,station_name
--     from 
--     (
--         select 
--             order_tracking.shipment_id
--             ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
--             ,order_tracking.status
--             ,staion_table_name.station_name
--             ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
--         from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
--         left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
--         on order_tracking.station_id = staion_table_name.station_id
--         where 
--             order_tracking.status in (52,53,54,55,56)
--     )
--     where 
--         rank_num = 1
--         and status = 56
--         -- and station_name = 'FCPON - ชุมพร (U-402)'
--         and date(time_stamp) between date('2022-12-13') and date('2022-12-14')
--         and split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
--         -- and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
--     group by 
--         shipment_id
--         ,date_time
--         ,station_name   
-- )
,Return_LMHub_LHTransporting_ontime as 
-- เพิ่ม Return_FMHub_LHTransporting = 235 
(
    select 
        shipment_id
        ,date(time_stamp) as date_time
        ,station_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (52,53,54,55,56,11,12,68,69,70,71,235)
    )
    where 
        rank_num = 1
        and status in (56,235)
        -- and station_name = 'FCPON - ชุมพร (U-402)'
        -- and date(time_stamp) between date('2022-12-13') and date('2022-12-14')
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date(time_stamp)
        ,station_name   
)
,Return_LMHub_LHTransporting_late as 
-- เพิ่ม Return_FMHub_LHTransporting = 235
(
    select 
        shipment_id
        ,date(time_stamp) as date_time
        ,station_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status in (52,53,54,55,56,11,12,68,69,70,71,235)
    )
    where 
        rank_num = 1
        and status in (56,235)
        -- and station_name = 'FCPON - ชุมพร (U-402)'
        -- and date(time_stamp) between date('2022-12-13') and date('2022-12-14')
        and split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date(time_stamp)
        ,station_name   
)
,lost as 
(
    select 
        shipment_id
        ,date_time
        ,station_name
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
            order_tracking.status in (52,53,54,55,56,11,12)
    )
    where 
        rank_num = 1
        and status = 11
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name   
)
,Damaged as 
(
    select 
        shipment_id
        ,date_time
        ,station_name
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
            order_tracking.status in (52,53,54,55,56,11,12)
    )
    where 
        rank_num = 1
        and status = 12
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name   
)
select
    -- REPLACE(CONCAT(CAST(Return_LMHub_Received.date_time AS VARCHAR) ,Return_LMHub_Received.station_name_lag),' ','') as concat_fm
    REPLACE(CONCAT(CAST(Return_LMHub_Received.date_time AS VARCHAR) ,Return_LMHub_Received.station_name_lag),' ','') as concat_fm
    ,Return_LMHub_Received.date_time
    ,Return_LMHub_Received.station_name_lag
    ,substring(split_part(Return_LMHub_Received.station_name_lag,' -',1),2,10) AS hub_name
    ,count(Return_LMHub_Received.shipment_id) as total_Return_LMHub_Received
    ,count(Return_LMHub_Received_ontime.shipment_id) as total_Return_LMHub_Received_ontime_1opm
    ,count(Return_LMHub_Received_late.shipment_id) as total_Return_LMHub_Received_late
    ,count(Return_LMHub_Received_ontime.shipment_id) + count(Return_LMHub_Received_late.shipment_id) as infull_retrun_ting
    ,count(Return_LMHub_Packing.shipment_id) as total_Return_LMHub_Packing
    ,count(Return_LMHub_Packed.shipment_id) as total_Return_LMHub_Packed
    ,count(Return_LMHub_LHPacking.shipment_id) as total_Return_LMHub_LHPacking
    ,count(Return_LMHub_LHPacked.shipment_id) as total_Return_LMHub_LHPacked
    ,count(Return_LMHub_LHTransporting_ontime.shipment_id) as total_Return_LMHub_LHTransporting_ontime_11pm
    ,count(Return_LMHub_LHTransporting_late.shipment_id) as total_Return_LMHub_LHTransporting_late
    ,count(lost.shipment_id) as total_lost
    ,count(Damaged.shipment_id) as total_Damaged
    ,count(Return_LMHub_Received.shipment_id) - (count(Return_LMHub_LHTransporting_ontime.shipment_id) + count(Return_LMHub_LHTransporting_late.shipment_id)) as hub_backlog
    ,(CAST(count(Return_LMHub_LHTransporting_ontime.shipment_id) AS DOUBLE) + CAST(count(Return_LMHub_LHTransporting_late.shipment_id) AS DOUBLE)) / count(Return_LMHub_Received.shipment_id) as "%infull_return_ting"
    ,CAST(count(Return_LMHub_LHTransporting_ontime.shipment_id) AS DOUBLE) / count(Return_LMHub_Received.shipment_id) as "%ontime_lhtransporting"
    ,(CAST(count(Return_LMHub_Received.shipment_id) AS DOUBLE) - (CAST(count(Return_LMHub_LHTransporting_ontime.shipment_id) AS DOUBLE) + CAST(count(Return_LMHub_LHTransporting_late.shipment_id) AS DOUBLE))) / count(Return_LMHub_Received.shipment_id) as "%hub_backlog"
from Return_LMHub_Received
left join Return_LMHub_Packing
on Return_LMHub_Received.shipment_id = Return_LMHub_Packing.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_Packing.date_time
left join Return_LMHub_Received_ontime
on Return_LMHub_Received.shipment_id = Return_LMHub_Received_ontime.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_Received_ontime.date_time
left join Return_LMHub_Received_late
on Return_LMHub_Received.shipment_id = Return_LMHub_Received_late.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_Received_late.date_time
left join Return_LMHub_Packed
on Return_LMHub_Received.shipment_id = Return_LMHub_Packed.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_Packed.date_time
left join Return_LMHub_LHPacking
on Return_LMHub_Received.shipment_id = Return_LMHub_LHPacking.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_LHPacking.date_time
left join Return_LMHub_LHPacked
on Return_LMHub_Received.shipment_id = Return_LMHub_LHPacked.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_LHPacked.date_time
left join Return_LMHub_LHTransporting_ontime
on Return_LMHub_Received.shipment_id = Return_LMHub_LHTransporting_ontime.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_LHTransporting_ontime.date_time
left join Return_LMHub_LHTransporting_late
on Return_LMHub_Received.shipment_id = Return_LMHub_LHTransporting_late.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_LHTransporting_late.date_time
left join lost
on Return_LMHub_Received.shipment_id = lost.shipment_id
and Return_LMHub_Received.date_time = lost.date_time
left join Damaged
on Return_LMHub_Received.shipment_id = Damaged.shipment_id
and Return_LMHub_Received.date_time = Damaged.date_time
where 
    Return_LMHub_Received.date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    Return_LMHub_Received.date_time
    ,Return_LMHub_Received.station_name_lag
order by 
    Return_LMHub_Received.date_time desc









