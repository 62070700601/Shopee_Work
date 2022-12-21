/*
-- จับ LMHub_Received เป็น Universe โดยเวลาคือ 23.59 และเงื่อนไขต้องเป็น LMHub_Received ที่เป็น r2 or r3 วิธีจับคือ (destination != current station)
LMHub_LHTransporting_Ontime [D0] 22:59
LMHub_Received_Ontime [D0]	21:59 
*/
with LMHub_Received as
(
    select 
        shipment_id
        ,date_time
        ,station_name
        -- ,last_station_order_path_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            -- ,origin_order_path
            ,cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) as last_station_order_path
            ,station_last_order_path.station_name as last_station_order_path_name
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
        on order_tracking.shipment_id = pub_shipment.shipment_id
        left join spx_mart.dim_spx_station_tab_ri_th_ro as station_last_order_path
        on cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) = cast(station_last_order_path.station_id as varchar)
        where 
            order_tracking.status = 1
            and origin_order_path is not null
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) = date('2022-12-14')
            -- and staion_table_name.station_name != station_last_order_path.station_name
-- select replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')
    )
    where 
        rank_num = 1
        and status = 1
        -- and date_time = date('2022-12-14')
        and station_name != last_station_order_path_name
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name
        -- ,last_station_order_path_name
)
,LMHub_Received_ontime as
(
    select 
        shipment_id
        ,date(time_stamp) as date_time
        ,station_name
        ,last_station_order_path_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) as last_station_order_path
            ,station_last_order_path.station_name as last_station_order_path_name
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
        on order_tracking.shipment_id = pub_shipment.shipment_id
        left join spx_mart.dim_spx_station_tab_ri_th_ro as station_last_order_path
        on cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) = cast(station_last_order_path.station_id as varchar)
        where 
            order_tracking.status = 1
            and origin_order_path is not null
    )
    where 
        rank_num = 1
        and status = 1
        -- and date(time_stamp) = date('2022-12-14')
        and station_name != last_station_order_path_name
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date(time_stamp)
        ,station_name
        ,last_station_order_path_name
)
,LMHub_Received_late as
(
    select 
        shipment_id
        ,date(time_stamp) as date_time
        ,station_name
        ,last_station_order_path_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) as last_station_order_path
            ,station_last_order_path.station_name as last_station_order_path_name
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
        on order_tracking.shipment_id = pub_shipment.shipment_id
        left join spx_mart.dim_spx_station_tab_ri_th_ro as station_last_order_path
        on cast(trim(replace(reverse(split_part(reverse(origin_order_path),',',1)),']','')) as varchar) = cast(station_last_order_path.station_id as varchar)
        where 
            order_tracking.status = 1
            and origin_order_path is not null
    )
    where 
        rank_num = 1
        and status = 1
        -- and date(time_stamp) = date('2022-12-14')
        and station_name != last_station_order_path_name
        and split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date(time_stamp)
        ,station_name
        ,last_station_order_path_name
)
,LMHub_LHPacking as
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
            order_tracking.status in (210,211,233,234,11,12)
    )
    where 
        rank_num = 1
        and status = 210
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time 
)
,LMHub_LHPacked as
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
            order_tracking.status in (210,211,233,234,11,12)
    )
    where 
        rank_num = 1
        and status = 211
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time 
)
,LMHub_LHTransporting as 
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
            order_tracking.status in (210,211,233,234,11,12)
    )
    where 
        rank_num = 1
        and status = 233
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time    
)
,LMHub_LHTransported_ontime as 
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
            order_tracking.status in (210,211,233,234,11,12)
    )
    where 
        rank_num = 1
        and status = 234
        -- and date(time_stamp) = date('2022-12-14')
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '22:59:59.000'
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date(time_stamp)
)
,LMHub_LHTransported_late as
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
            order_tracking.status in (210,211,233,234,11,12)
    )
    where 
        rank_num = 1
        and status = 234
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
            order_tracking.status in (210,211,233,234,11,12)
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
            order_tracking.status in (210,211,233,234,11,12)
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
    REPLACE(CONCAT(CAST(LMHub_Received.date_time AS VARCHAR) ,LMHub_Received.station_name),' ','') as concat_fm
    ,LMHub_Received.date_time
    ,LMHub_Received.station_name
    ,substring(split_part(LMHub_Received.station_name,' -',1),2,10) AS hub_name
    ,count(LMHub_Received.shipment_id) as total_LMHub_Received
    ,count(LMHub_Received_ontime.shipment_id) as ontime_received_10pm
    ,count(LMHub_Received_late.shipment_id) as late_received
    ,count(LMHub_LHTransported_ontime.shipment_id) + count(LMHub_LHTransported_late.shipment_id) as infull_lm_ted
    ,count(LMHub_LHTransported_ontime.shipment_id) as ontime_lhtransported_11pm
    ,count(LMHub_LHTransported_late.shipment_id) as late_lhtransported
    ,count(LMHub_LHPacking.shipment_id) as total_LMHub_LHPacking
    ,count(LMHub_LHPacked.shipment_id) as total_LMHub_LHPacked
    ,count(LMHub_LHTransporting.shipment_id) as total_LMHub_LHTransporting
    ,count(lost.shipment_id) as total_lost
    ,count(Damaged.shipment_id) as total_Damaged
    ,count(LMHub_Received.shipment_id) - (count(LMHub_Received_ontime.shipment_id) + count(LMHub_Received_late.shipment_id)) as hub_backlog
    ,(CAST(count(LMHub_LHTransported_ontime.shipment_id) AS DOUBLE) + CAST(count(LMHub_LHTransported_late.shipment_id) AS DOUBLE)) / count(LMHub_Received.shipment_id) as "%infull_lm_ted"
    ,CAST(count(LMHub_LHTransported_ontime.shipment_id) AS DOUBLE) / count(LMHub_Received.shipment_id) as "%ontime_lhtransported"
    ,(CAST(count(LMHub_Received.shipment_id) AS DOUBLE) - (CAST(count(LMHub_LHTransported_ontime.shipment_id) AS DOUBLE) + CAST(count(LMHub_LHTransported_late.shipment_id) AS DOUBLE))) / count(LMHub_Received.shipment_id) as "%hub_backlog"
from LMHub_Received
left join LMHub_Received_ontime
on LMHub_Received.shipment_id = LMHub_Received_ontime.shipment_id
and LMHub_Received.date_time = LMHub_Received_ontime.date_time
left join LMHub_Received_late
on LMHub_Received.shipment_id = LMHub_Received_late.shipment_id
and LMHub_Received.date_time = LMHub_Received_late.date_time
left join LMHub_LHPacking
on LMHub_Received.shipment_id = LMHub_LHPacking.shipment_id
and LMHub_Received.date_time = LMHub_LHPacking.date_time
left join LMHub_LHPacked
on LMHub_Received.shipment_id = LMHub_LHPacked.shipment_id
and LMHub_Received.date_time = LMHub_LHPacked.date_time
left join LMHub_LHTransporting
on LMHub_Received.shipment_id = LMHub_LHTransporting.shipment_id
and LMHub_Received.date_time = LMHub_LHTransporting.date_time
left join LMHub_LHTransported_ontime
on LMHub_Received.shipment_id = LMHub_LHTransported_ontime.shipment_id
and LMHub_Received.date_time = LMHub_LHTransported_ontime.date_time
left join LMHub_LHTransported_late
on LMHub_Received.shipment_id = LMHub_LHTransported_late.shipment_id
and LMHub_Received.date_time = LMHub_LHTransported_late.date_time
left join lost
on LMHub_Received.shipment_id = lost.shipment_id
and LMHub_Received.date_time = lost.date_time
left join Damaged
on LMHub_Received.shipment_id = Damaged.shipment_id
and LMHub_Received.date_time = Damaged.date_time
where  
    LMHub_Received.station_name is not null
    -- and LMHub_Received.date_time = date('2022-12-14')
    and LMHub_Received.date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    LMHub_Received.date_time
    ,LMHub_Received.station_name
order by
    LMHub_Received.date_time desc