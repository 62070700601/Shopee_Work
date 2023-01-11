--
with Return_LMHub_Received as
-- Return_LMHub_Received = 10
-- Return_FMHub_Received = 67
(
    select 
        shipment_id
        ,date_time
        -- ,station_name
        ,REPLACE(station_name, SUBSTRING(station_name, 1, 1), 'H') as station_name
        ,time_stamp
        -- ,last_station_order_path_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            -- ,origin_order_path
            ,cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) as last_station_order_path
            ,station_last_order_path.station_name as last_station_order_path_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
            ,updated_order_path_lm_hub_station_name
             ,case 
                 when is_order_path_changed != true then station_last_order_path.station_name
                 else updated_order_path_lm_hub_station_name
            end as order_path_last 
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
        on order_tracking.shipment_id = pub_shipment.shipment_id
        left join spx_mart.dim_spx_station_tab_ri_th_ro as station_last_order_path
        on cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) = cast(station_last_order_path.station_id as varchar)
        where 
            order_tracking.status in (10,67)
            and origin_order_path is not null
            -- and order_tracking.shipment_id = 'SPXTH02093549557C'
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) = date('2022-12-14')
            -- and staion_table_name.station_name != station_last_order_path.station_name
    )
    where 
        rank_num = 1
        and status in (10,67)
        -- and date_time = date('2023-01-08')
        -- and station_name in ('HCMAI-A - เชียงใหม่','HCMAI-B - เชียงใหม่')
        -- and station_name = 'HCMAI-B - เชียงใหม่'
        and station_name != order_path_last
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        -- ,station_name
        ,time_stamp
        ,REPLACE(station_name, SUBSTRING(station_name, 1, 1), 'H')
        -- ,last_station_order_path_name
)
,Return_LMHub_Received_ontime as 
(
    select
        shipment_id
        ,date_time
        ,station_name 
    from Return_LMHub_Received
    where
        split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
)
,Return_LMHub_Received_late as 
(
    select
        shipment_id
        ,date_time
        ,station_name 
    from Return_LMHub_Received
    where
        split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'

)
-- ,LMHub_Received_ontime as
-- (
--     select 
--         shipment_id
--         ,date(time_stamp) as date_time
--         ,station_name
--         ,last_station_order_path_name
--     from 
--     (
--         select 
--             order_tracking.shipment_id
--             ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
--             ,order_tracking.status
--             ,staion_table_name.station_name
--             ,cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) as last_station_order_path
--             ,station_last_order_path.station_name as last_station_order_path_name
--             ,case 
--                  when is_order_path_changed != true then station_last_order_path.station_name
--                  else updated_order_path_lm_hub_station_name
--             end as order_path_last 
--             ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
--         from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
--         left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
--         on order_tracking.station_id = staion_table_name.station_id
--         left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
--         on order_tracking.shipment_id = pub_shipment.shipment_id
--         left join spx_mart.dim_spx_station_tab_ri_th_ro as station_last_order_path
--         on cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) = cast(station_last_order_path.station_id as varchar)
--         where 
--             order_tracking.status = 1
--             and origin_order_path is not null
--     )
--     where 
--         rank_num = 1
--         and status = 1
--         -- and date(time_stamp) = date('2022-12-14')
--         and station_name != order_path_last
--         -- and station_name != last_station_order_path_name
--         and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
--         and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
--     group by 
--         shipment_id
--         ,date(time_stamp)
--         ,station_name
--         ,last_station_order_path_name
-- )
-- ,LMHub_Received_late as
-- (
--     select 
--         shipment_id
--         ,date(time_stamp) as date_time
--         ,station_name
--         ,last_station_order_path_name
--     from 
--     (
--         select 
--             order_tracking.shipment_id
--             ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
--             ,order_tracking.status
--             ,staion_table_name.station_name
--             ,cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) as last_station_order_path
--             ,case 
--                 when is_order_path_changed != true then station_last_order_path.station_name
--                 else updated_order_path_lm_hub_station_name
--             end as order_path_last 
--             ,station_last_order_path.station_name as last_station_order_path_name
--             ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
--         from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
--         left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
--         on order_tracking.station_id = staion_table_name.station_id
--         left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
--         on order_tracking.shipment_id = pub_shipment.shipment_id
--         left join spx_mart.dim_spx_station_tab_ri_th_ro as station_last_order_path
--         on cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) = cast(station_last_order_path.station_id as varchar)
--         where 
--             order_tracking.status = 1
--             and origin_order_path is not null
--     )
--     where 
--         rank_num = 1
--         and status = 1
--         -- and date(time_stamp) = date('2022-12-14')
--         and station_name != order_path_last
--         -- and station_name != last_station_order_path_name
--         and split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
--         and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
--     group by 
--         shipment_id
--         ,date(time_stamp)
--         ,station_name
--         ,last_station_order_path_name
-- )
,Return_LMHub_Packing as
-- เพิ่ม Return_LMHub_Packing = 52
-- เพิ่ม Return_FMHub_LHPacking = 70
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
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status in (210,43)
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time 
)
,Return_LMHub_Packed as
-- เพิ่ม Return_LMHub_Packed = 53
-- เพิ่ม Return_LMHub_Packed = 69
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
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status in (211,44)
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time 
)
,Return_LMHub_LHPacking as
-- เพิ่ม Return_LMHub_LHPacking	= 54
-- เพิ่ม Return_FMHub_LHPacking	= 70
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
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status in (231,45)
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time 
)
,Return_LMHub_LHPacked as
-- เพิ่ม Return_LMHub_LHPacked = 55
-- เพิ่ม Return_FMHub_LHPacked = 71
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
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status in (232,46)
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time 
)
,Return_LMHub_LHTransporting_ontime as 
-- เพิ่ม Return_LMHub_LHTransporting = 56
-- เพิ่ม Return_FMHub_LHTransporting = 235
(
    select 
        shipment_id
        ,date(time_stamp) as date_time
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
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status in (233,47)
        -- and date(time_stamp) = date('2022-12-14')
        -- and date(time_stamp) = date('2023-01-02')
        -- and station_name in ('HCMAI-A - เชียงใหม่','HCMAI-B - เชียงใหม่')
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '22:59:59.000'
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date(time_stamp)
)
,Return_LMHub_LHTransporting_late as
-- เพิ่ม Return_LMHub_LHTransporting = 56
-- เพิ่ม Return_FMHub_LHTransporting = 235
(
    select 
        shipment_id
        ,date(time_stamp) as date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status in (233,47)
        -- and date(time_stamp) = date('2022-12-14')
        and split_part(cast(time_stamp AS varchar),' ',2) between '23:00:00.000' and '23:59:59.000'
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date(time_stamp)
)
,lost as
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
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status = 11
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
)
,Damaged as
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
            order_tracking.status in (52,70,53,69,54,70,55,71,56,235,11,12)
    )
    where 
        rank_num = 1
        and status = 12
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
)
select 
    REPLACE(CONCAT(CAST(Return_LMHub_Received.date_time AS VARCHAR) ,Return_LMHub_Received.station_name),' ','') as concat_fm
    ,Return_LMHub_Received.date_time
    ,Return_LMHub_Received.station_name
    ,substring(split_part(Return_LMHub_Received.station_name,' -',1),2,10) AS hub_name
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
left join Return_LMHub_Received_ontime
on Return_LMHub_Received.shipment_id = Return_LMHub_Received_ontime.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_Received_ontime.date_time
left join Return_LMHub_Received_late
on Return_LMHub_Received.shipment_id = Return_LMHub_Received_late.shipment_id
and Return_LMHub_Received.date_time = Return_LMHub_Received_late.date_time
left join Return_LMHub_Packing
on Return_LMHub_Received.shipment_id = Return_LMHub_Packing.shipment_id
and Return_LMHub_Packing.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = LMHub_Packing.date_time
left join Return_LMHub_Packed
on Return_LMHub_Received.shipment_id = Return_LMHub_Packed.shipment_id
and Return_LMHub_Packed.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = LMHub_Packed.date_time
left join Return_LMHub_LHPacking
on Return_LMHub_Received.shipment_id = Return_LMHub_LHPacking.shipment_id
and Return_LMHub_LHPacking.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = LMHub_LHPacking.date_time
left join Return_LMHub_LHPacked
on Return_LMHub_Received.shipment_id = Return_LMHub_LHPacked.shipment_id
and Return_LMHub_LHPacked.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = LMHub_LHPacked.date_time
left join Return_LMHub_LHTransporting_ontime
on Return_LMHub_Received.shipment_id = Return_LMHub_LHTransporting_ontime.shipment_id
and Return_LMHub_LHTransporting_ontime.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = LMHub_LHTransporting_ontime.date_time
left join Return_LMHub_LHTransporting_late
on Return_LMHub_Received.shipment_id = Return_LMHub_LHTransporting_late.shipment_id
and Return_LMHub_LHTransporting_late.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = LMHub_LHTransporting_late.date_time
left join lost
on Return_LMHub_Received.shipment_id = lost.shipment_id
and lost.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = lost.date_time
left join Damaged
on Return_LMHub_Received.shipment_id = Damaged.shipment_id
and Damaged.date_time between Return_LMHub_Received.date_time and Return_LMHub_Received.date_time + interval '1' day
-- and LMHub_Received.date_time = Damaged.date_time
where  
    Return_LMHub_Received.station_name is not null
    -- and LMHub_Received.date_time = date('2022-12-14')
    and Return_LMHub_Received.date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    Return_LMHub_Received.date_time
    ,Return_LMHub_Received.station_name
order by
    Return_LMHub_Received.date_time desc
