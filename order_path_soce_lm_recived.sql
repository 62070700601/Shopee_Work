/*
โจทย์
1.จับstatus soc_recived and lm_recived แล้วเรียงลำดับว่า status ไหนมาก่อนกัน
2.แล้วนำ station ทีเรียงกันมาอยู่ใน array
3.เอาแค่ min_soc_recived เริ่มต้นด้วย SOCE เท่านั้น
4.ทำเป็น D-1
*/
select 
    min_soc_received.shipment_id
    ,order_path_tracking.order_path
from 
(
select 
    order_track.shipment_id
    ,from_unixtime(order_track.ctime-3600) as time_stamp
    ,order_track.status
    ,staion_table_name.station_name
    ,row_number() over (partition  by order_track.shipment_id order by from_unixtime(order_track.ctime-3600) asc) as rank_num
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
on order_track.station_id = staion_table_name.id
where 
    order_track.status = 8
    -- and date(from_unixtime(order_track.ctime-3600)) = date(DATE_TRUNC('day', current_timestamp) - interval '1' day)
) as min_soc_received
left join 
(
select 
    shipment_id
    ,array_join(slice(array_agg(station_name ORDER BY rank_num asc),1,25),'>') as order_path
from 
    (
    select 
        shipment_id
        ,station_id
        ,staion_table_name.station_name
        ,from_unixtime(order_track.ctime-3600) as time_stamp
        ,row_number() over (partition by shipment_id order by from_unixtime(order_track.ctime-3600) asc) as rank_num
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on order_track.station_id = staion_table_name.id
    where 
        order_track.status in (8,1)
        -- and shipment_id in ('SPXTH02598822609A','SPXTH02626852429A','SPXTH02645713114A')
    )
group by 
    shipment_id
) as order_path_tracking
on min_soc_received.shipment_id = order_path_tracking.shipment_id
where
    min_soc_received.rank_num = 1
    -- and min_soc_received.station_name = 'SOCE'
    and split_part(order_path,'>',1) = 'SOCE'
    -- and min_soc_received.shipment_id = 'SPXTH02872728450B'
    and date(min_soc_received.time_stamp) = date(DATE_TRUNC('day', current_timestamp) - interval '1' day)
group by 
    min_soc_received.shipment_id
    ,order_path_tracking.order_path