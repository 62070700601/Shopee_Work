select 
    shipment_id
    ,destination_hub
    ,delivered_time_stamp
    ,min_delivering_status_time_stamp
    ,min_assigning_status_time_stamp
    ,max_assigning_status_time_stamp
    ,min_assigned_status_time_stamp
    ,max_assigned_status_time_stamp
    ,min_lmhub_received_status_time_stamp
    ,max_rc_lh_transported_status_time_stamp
    ,max_soc_lh_transported_status_time_stamp
    ,max_rc_lh_transporting_status_time_stamp
    ,max_soc_lh_transporting_status_time_stamp
    ,if(max_rc_lh_transported_status_time_stamp > max_soc_lh_transported_status_time_stamp,delivered_time_stamp - max_rc_lh_transported_status_time_stamp,delivered_time_stamp - max_soc_lh_transported_status_time_stamp) as condition_compare_rc_soc
    ,if(max_rc_lh_transported_status_time_stamp > max_soc_lh_transported_status_time_stamp,date_diff('hour',max_rc_lh_transported_status_time_stamp,delivered_time_stamp),date_diff('hour',max_soc_lh_transported_status_time_stamp,delivered_time_stamp)) as condition_compare_rc_soc_hour
    -- ,if(max_rc_lh_transported_status_time_stamp > max_soc_lh_transported_status_time_stamp,date_diff('hour',delivered_time_stamp,max_rc_lh_transported_status_time_stamp),date_diff('hour',delivered_time_stamp,max_soc_lh_transported_status_time_stamp)) as condition_compare_rc_soc_hour
    -- DATEDIFF(year, '2017/08/25', '2011/08/25') AS DateDiff;
from
    (
    select 
        order_track.shipment_id
        ,max(if(order_track.status = 4,staion_table_name.station_name)) as destination_hub
        ,max(if(order_track.status = 4,FROM_UNIXTIME(order_track.ctime-3600),null)) as delivered_time_stamp
        ,min(if(order_track.status = 2,FROM_UNIXTIME(order_track.ctime-3600),null)) as min_delivering_status_time_stamp
        ,min(if(order_track.status = 49,FROM_UNIXTIME(order_track.ctime-3600),null)) as min_assigning_status_time_stamp
        ,max(if(order_track.status = 49,FROM_UNIXTIME(order_track.ctime-3600),null)) as max_assigning_status_time_stamp
        ,min(if(order_track.status = 50,FROM_UNIXTIME(order_track.ctime-3600),null)) as min_assigned_status_time_stamp
        ,max(if(order_track.status = 50,FROM_UNIXTIME(order_track.ctime-3600),null)) as max_assigned_status_time_stamp
        ,min(if(order_track.status = 1,FROM_UNIXTIME(order_track.ctime-3600),null)) as min_lmhub_received_status_time_stamp
        ,max(if(order_track.status = 36 and staion_table_name.id in (1350,82,1479,1480,71,77,983),FROM_UNIXTIME(order_track.ctime-3600),null)) as max_rc_lh_transported_status_time_stamp
        ,max(if(order_track.status = 36 and staion_table_name.id not in (1350,82,1479,1480,71,77,983),FROM_UNIXTIME(order_track.ctime-3600),null)) as max_soc_lh_transported_status_time_stamp
        ,max(if(order_track.status = 15 and staion_table_name.id in (1350,82,1479,1480,71,77,983),FROM_UNIXTIME(order_track.ctime-3600),null)) as max_rc_lh_transporting_status_time_stamp
        ,max(if(order_track.status = 15 and staion_table_name.id not in (1350,82,1479,1480,71,77,983),FROM_UNIXTIME(order_track.ctime-3600),null)) as max_soc_lh_transporting_status_time_stamp
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on try(cast(json_extract(json_parse(content),'$.station_id') as int)) = staion_table_name.id
    where
        order_track.status in (4,2,49,1,36,15,50)
        -- and order_track.shipment_id in ('SPXTH02974040325A','SPXTH02845071030A','SPXTH02493887707A','SPXTH02223575418A','SPXTH02009808825A')
group by 
    order_track.shipment_id
    )