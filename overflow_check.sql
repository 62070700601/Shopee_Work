with servicable_mapping as
(
select 
    date(from_unixtime(cast(date as int))) as servicable_date 
    ,district
    ,delivery_hub
    ,case 
        when delivery_hub like '4PL' THEN 'nonserviceable'
        else 'serviceable'
    end as serviceable_area
from thopsbi_lof.spx_index_delivery_hub_to_district_temp_v3
)
,pickup_date_cal AS 
(
SELECT
    shipment_id
    ,CASE 
        WHEN (station_name LIKE 'D%' or station_name LIKE 'P%') THEN dropoff_date 
        ELSE pickup_date 
    END AS pickup_done_date
    ,station_name AS dropoff_hub
FROM 
    (
    SELECT
        shipment_id
        ,MIN(CASE 
                WHEN status IN (13,39,42,8) THEN DATE(FROM_UNIXTIME(ctime-3600)) 
                WHEN status = 112 THEN DATE(FROM_UNIXTIME(ctime-3600)+INTERVAL '9' HOUR)  
            END) AS pickup_date
        ,MIN(CASE WHEN status = 42 THEN DATE(FROM_UNIXTIME(ctime-3600)+ INTERVAL '9' HOUR) END) AS dropoff_date
        ,MIN(CASE WHEN status = 42 THEN TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.station_id') AS INTEGER)) END) AS dropoff_hub_id
    FROM spx_mart.shopee_fms_th_db__order_tracking_tab__th_continuous_s0_live
    GROUP BY 
        shipment_id
    ) as order_tracking
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live station
ON order_tracking.dropoff_hub_id = CAST(station.id AS INTEGER)
)
,raw_serviceable as
(
select 
    fleet_order.shipment_id
    ,CASE 
        WHEN station.station_type = 5 OR fleet_order.channel_id != 1 OR station.station_name LIKE '4PL%' OR timestamp_4pl is not null THEN 1 ELSE 0 END AS is_4pl
    ,date(order_track.delivered_time) as delivered_date 
    ,buyer_info.buyer_addr_district as buyer_district 
    ,pickup_date_cal.pickup_done_date
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order 
left join 
    (
    select 
        shipment_id 
        ,min(case when status in (4,81) then from_unixtime(ctime-3600) end) as delivered_time 
        ,min(case when status in (18,89) then from_unixtime(ctime-3600) end) as timestamp_4pl 
    from spx_mart.shopee_fms_th_db__order_tracking_tab__th_continuous_s0_live 
    group by
        shipment_id 
    ) as order_track
on order_track.shipment_id = fleet_order.shipment_id
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live AS station 
ON fleet_order.station_id = station.id
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live AS buyer_info
ON fleet_order.shipment_id = buyer_info.shipment_id   
LEFT JOIN pickup_date_cal
on pickup_date_cal.shipment_id = fleet_order.shipment_id
),
servicable_map as
(
select 
    pickup_done_date
    ,buyer_district
    ,shipment_id
    ,is_4pl
    ,serviceable_area
from raw_serviceable 
left join servicable_mapping 
on servicable_mapping.servicable_date = raw_serviceable.pickup_done_date
and servicable_mapping.district = raw_serviceable.buyer_district 
)
select 
    pickup_done_date as report_date
    ,buyer_district
    ,count(*) as total_pickup_volume 
    --,sum(case when serviceable_area =  'serviceable' then 1 end) as total_spx_serviceable_volume
    --,sum(case when serviceable_area =  'serviceable' and is_4pl = 0 then 1 end) as spx_e2e_servicable_volume  
    --,sum(case when serviceable_area =  'serviceable' and is_4pl = 1 then 1 end) as servicable_volume_4pl
    --,sum(case when serviceable_area =  'nonserviceable' then 1 end) as total_spx_non_serviceable_volume
from servicable_map
where 
    serviceable_area = 'serviceable' 
    and is_4pl = 1
    and pickup_done_date between current_date - interval '15' day and current_date - interval '1' day
group by 
    pickup_done_date
    ,buyer_district
order by 
    pickup_done_date desc
    ,buyer_district