with pickup_date_cal AS 
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
        ,MIN(CASE 
                WHEN status = 42 THEN DATE(FROM_UNIXTIME(ctime-3600)+ INTERVAL '9' HOUR) 
            END) AS dropoff_date
        ,MIN(CASE 
                WHEN status = 42 THEN TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.station_id') AS INTEGER)) 
            END) AS dropoff_hub_id
    FROM spx_mart.shopee_fms_th_db__order_tracking_tab__th_continuous_s0_live
    GROUP BY 
        shipment_id
    ) as order_tracking
    LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live station
    ON order_tracking.dropoff_hub_id = CAST(station.id AS INTEGER)
)
,fleet_raw as
(
select 
    fleet_order.shipment_id
    ,pickup_date_cal.pickup_done_date as report_date
    ,origin_path.station_name as orgin_last_station
    ,CASE 
        WHEN fleet_order.station_id = 37 THEN 'flash'
        WHEN fleet_order.channel_id = 50002 THEN 'flash'
        WHEN fleet_order.station_id IN (121,122) THEN 'cj'
        WHEN fleet_order.channel_id = 50003 THEN 'cj'
        WHEN fleet_order.station_id = 188 THEN 'njv'
        WHEN fleet_order.station_id = 25 THEN 'jnt'
        WHEN fleet_order.channel_id = 50001 THEN 'jnt'
        WHEN fleet_order.station_id = 5 or fleet_order.station_id = 170 THEN 'kerry'
        --WHEN station.station_type = 5 THEN 'non-integrated'
        else 'spx' 
    END as delivery_station
    ,CASE 
        WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 THEN 1 
        ELSE 0 
    END AS is_4pl
    ,buyer_info.buyer_addr_district as buyer_district
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join 
    ( 
    select 
        shipment_id
        ,last_staton_id
        ,station.station_name
    from     
        (
        select 
            shipment_id 
            ,origin_order_path
            ,try(cast(replace(ltrim(split_part(origin_order_path,',',cardinality(CAST(JSON_PARSE(origin_order_path) AS ARRAY<INT>)))),']','') as int)) as last_staton_id 
        from spx_mart.shopee_fleet_order_th_db__fleet_order_extension_tab__reg_daily_s0_live
        where 
            origin_order_path != '[]'
        ) as odp
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station
    on odp.last_staton_id = station.id
    ) as origin_path
on origin_path.shipment_id = fleet_order.shipment_id

left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station
on fleet_order.station_id = station.id

left join pickup_date_cal
on pickup_date_cal.shipment_id = fleet_order.shipment_id

left join spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live  AS buyer_info
ON  fleet_order.shipment_id = buyer_info.shipment_id
)
select 
    report_date
    ,delivery_station
    ,shipment_id
    ,is_4pl
    ,orgin_last_station
    ,buyer_district
    ,case when orgin_last_station like 'H%' and is_4pl = 1 then 1 else 0 end as is_overflow 
from fleet_raw
where 
    report_date = Current_date - interval '1' day
    and orgin_last_station like 'H%' and is_4pl = 1
order by 1 desc,2,5,6
