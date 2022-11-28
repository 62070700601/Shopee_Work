with lost_damage_track as 
(
select 
    fleet_order_lost.shipment_id
    ,fleet_order_lost.status_id
    ,fleet_order_lost.lost_damage_time as timestamp_status_lost
    ,fleet_order_lost.last_status as status_soc_received
    ,case 
        when fleet_order_lost.status_id = 11 then 'Lost'
        when fleet_order_lost.status_id = 12 then 'Damaged'
    end as "status_type"
from 
    (
    select 
    fleet_order.shipment_id
    ,fleet_order.status AS status_id
    ,FROM_UNIXTIME(ctime-3600) as lost_damage_time
    ,station_id
    ,lead(status) over (partition by fleet_order.shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as last_status
    ,row_number() over (partition by fleet_order.shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as rank_num
    from  spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as fleet_order
    ) fleet_order_lost
where 
    rank_num = 1
    and status_id = 11 and last_status = 8
)
,soc_received_status as
(
    select *
    from 
        (
        select 
            soc_received_fleet.shipment_id
            ,FROM_UNIXTIME(ctime-3600) as soc_recived_timestamp
            ,operator
            ,COALESCE(station_id,try(cast(json_extract(json_parse(content),'$.station_id') as INTEGER))) as station_id_soc_recived
            ,row_number() over (partition by soc_received_fleet.shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as rank_num_soc
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as soc_received_fleet
        where 
            status = 8
            )
        where   
            rank_num_soc = 1
)
, soc_pickup_done as
(
         select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as soc_pickup_done_timestamp
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as soc_received_fleet
        where 
            status = 13
)
select 
    lost_damage_track.shipment_id
    ,lost_damage_track.status_type
    ,lost_damage_track.timestamp_status_lost
    ,case 
        when lost_damage_track.status_soc_received = 8 then 'SOC_Received'
    end previous_last_status
    ,soc_received_status.soc_recived_timestamp
    ,soc_received_status.operator
    ,staion_table_name.station_name
    ,pub_shipment.cogs_amount
    ,COALESCE(pub_shipment.latest_awb_station_name,pub_shipment.updated_order_path_lm_hub_station_name) as Destination
from lost_damage_track
left join thopsbi_spx.dwd_pub_shipment_info_df_th pub_shipment
on lost_damage_track.shipment_id = pub_shipment.shipment_id
left join soc_received_status
on lost_damage_track.shipment_id = soc_received_status.shipment_id
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
on soc_received_status.station_id_soc_recived = staion_table_name.id
left join soc_pickup_done
on lost_damage_track.shipment_id = soc_pickup_done.shipment_id
where
    COALESCE(date(pub_shipment.pickup_done_timestamp),date(soc_pickup_done.soc_pickup_done_timestamp)) between date(DATE_TRUNC('year', current_timestamp) + interval '6' month) and date(DATE_TRUNC('year', current_timestamp) + interval '10' month - interval '1' day)
order by COALESCE(date(pub_shipment.pickup_done_timestamp),date(soc_pickup_done.soc_pickup_done_timestamp))