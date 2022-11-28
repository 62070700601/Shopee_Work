with ninja_van_kerry as
(
    select 
        spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live.shipment_id
        ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) as destination_name
        ,status as id_status
        ,from_unixtime(ctime-3600) as time_socpacked
        ,handover_time
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
    left join 
        (
        select 
        shipment_id
        ,from_unixtime(ctime-3600) as handover_time
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 89
        ) handoverdate
    on spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live.shipment_id = handoverdate.shipment_id
    where 
        try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar))  in ('Ninja Van','Kerry','4PL-Kerry (non-int)','SOC Kerry','4PL - Kerry (non-int)','4PL-Kerry (R3)')
        and from_unixtime(ctime-3600)  between DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '6' hour and DATE_TRUNC('day', current_timestamp) + interval '6' hour  
        and status = 33
) 
select 
     date(time_socpacked) as report_date
    ,fleet_order.shipment_id
    ,date(COALESCE(pickup_done_timestamp,created_timestamp)) as pickup_date 
    ,date(handover_time) as handover_date 
    ,destination_name as crossdock_name
    ,'3PL_Received' as current_status
    ,cogs_amount as cogs
    ,date_diff('day',date(COALESCE(pickup_done_timestamp,created_timestamp)),current_date ) as agging_pu_done_time
    ,date_diff('day',date(time_socpacked),current_date) as agging_soc_received
    ,date_diff('day',date(handover_time),current_date ) as agging_handover_date
    ,date_diff('day',date(handover_time),current_date ) as agging_last_status
from ninja_van_kerry
left join thopsbi_spx.dwd_pub_shipment_info_df_th as fleet_order
on ninja_van_kerry.shipment_id = fleet_order.shipment_id
where time_socpacked between DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '6' hour and DATE_TRUNC('day', current_timestamp) + interval '6' hour  
order by report_date ,pickup_date