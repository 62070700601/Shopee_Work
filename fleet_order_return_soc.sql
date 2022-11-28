with return_soc as 
(
select 
    shipment_id
    ,status_time as Return_SOC_LHTransported_time
    ,pickup_station as pickup_return_soc
from
(
    select 
        shipment_id
        ,from_unixtime(ctime-3600) as status_time
        ,row_number() over (partition by shipment_id order by from_unixtime(ctime-3600) asc) as row_number
        ,try(cast(json_extract(json_parse(content),'$.pickup_station_name') as varchar)) as pickup_station
        ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) as destination
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    where 
        status = 65
)
where row_number = 1
)
, destination_SIP_LMHub as 
(
select 
    shipment_id
    ,from_unixtime(ctime-3600) as soc_lhtransportedtime
    ,try(cast(json_extract(json_parse(content),'$.pickup_station_name') as varchar)) as pickup_SIP
    ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) as destination_SIP
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
where 
    status = 36
    and try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) = 'SIP-LMHub'
)
, soc_received_timestamp as
(
select 
    shipment_id
    ,status_time 
from 
    (
        select shipment_id
            ,from_unixtime(ctime-3600) as status_time
            ,row_number() over (partition by shipment_id order by from_unixtime(ctime-3600) asc) as row_number
            ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar))
            ,status
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 8
            and try(cast(json_extract(json_parse(content),'$.station_id') as varchar)) = '3'
     )
where row_number = 1       
) 
select 
    fleet_order.shipment_id
    ,soc_received_timestamp.status_time as soc_received_time
    ,COALESCE(destination_SIP_LMHub.soc_lhtransportedtime,return_soc.Return_SOC_LHTransported_time) as soc_lhtransported_time
    ,destination_SIP_LMHub.pickup_SIP as pickup_station
    ,fleet_order.latest_awb_station_name as destination
from thopsbi_spx.dwd_pub_shipment_info_df_th as fleet_order
left join return_soc
on fleet_order.shipment_id = return_soc.shipment_id
left join destination_SIP_LMHub
on fleet_order.shipment_id = destination_SIP_LMHub.shipment_id 
left join soc_received_timestamp
on fleet_order.shipment_id = soc_received_timestamp.shipment_id
where 
    fleet_order.latest_awb_station_name = 'SIP-LMHub'
    and soc_received_timestamp.status_time between DATE_TRUNC('day', current_timestamp) - interval '6' day + interval '6' hour and DATE_TRUNC('day', current_timestamp) + interval '6' hour - interval '1' minute
order by soc_received_timestamp.status_time 
