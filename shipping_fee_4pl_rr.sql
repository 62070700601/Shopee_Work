WITH fleet_raw AS 
(
    SELECT
        fleet_order.shipment_id
        ,fleet_order.order_type
        ,CASE
            WHEN fleet_order.station_id = 37 OR fleet_order.channel_id = 50002 THEN 'Flash Express'
            WHEN fleet_order.station_id IN (121,122) OR fleet_order.channel_id = 50003 THEN 'CJ Logistics'
            WHEN fleet_order.station_id = 188 THEN 'Ninja Van'
            WHEN fleet_order.station_id IN (5,170) THEN 'Kerry Express'
            WHEN fleet_order.station_id = 25 OR fleet_order.channel_id = 50001 THEN 'J&T Express'
            ELSE 'SPX' 
        END AS delivery_station
        ,CASE WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 THEN 1 ELSE 0 END AS is_4pl
        ,CASE 
            WHEN pickup.seller_province IS NOT NULL THEN pickup.seller_province
            ELSE dropoff.seller_province 
        END AS seller_province
        ,buyer_info.buyer_addr_state AS buyer_province
        ,buyer_info.buyer_addr_district AS buyer_district
       /*  CASE
            WHEN fleet_order.chargeable_weight IS NULL THEN fleet_order.sc_Weight
            ELSE fleet_order.chargeable_weight
        END AS chargeable_weight,*/
        ,case 
            when (fleet_order.station_id = 37 OR fleet_order.channel_id = 50002) then fleet_order.sc_weight 
            when (fleet_order.station_id != 37 and fleet_order.channel_id != 50002) and fleet_order.chargeable_weight is not null then fleet_order.chargeable_weight
            else fleet_order.sc_weight  
        end as chargeable_weight
        ,delivered_date
    FROM
        spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live fleet_order
    LEFT JOIN 
    (
        SELECT
            shipment_id
            ,MIN(DATE(FROM_UNIXTIME(ctime-3600))) AS delivered_date
        FROM
            spx_mart.shopee_fms_th_db__order_tracking_tab__th_continuous_s0_live
        WHERE
            status IN (81)
        GROUP BY
            shipment_id
    ) as del_track
    ON fleet_order.shipment_id = del_track.shipment_id
    LEFT JOIN 
    (
        SELECT 
            pickup1.pickup_order_id
            ,seller_addr_state AS seller_province
            ,seller_addr_district AS seller_district
        FROM 
            spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_daily_s0_live pickup1
        INNER JOIN 
        (
            SELECT
                pickup_order_id
                ,MAX(ctime) AS latest_ctime 
            FROM 
                spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_daily_s0_live
            GROUP BY
                pickup_order_id
        ) AS pickup2
        ON pickup1.pickup_order_id = pickup2.pickup_order_id
        AND pickup1.ctime = pickup2.latest_ctime   
    ) as pickup
    ON pickup.pickup_order_id = fleet_order.shipment_id
    LEFT JOIN 
    (
        SELECT 
            shipment_id
            ,seller_state AS seller_province
            ,seller_city AS seller_district
        FROM spx_mart.shopee_fms_th_db__dropoff_order_tab__th_daily_s0_live
    ) as dropoff
    ON dropoff.shipment_id = fleet_order.shipment_id
    LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live  AS buyer_info
    ON fleet_order.shipment_id = buyer_info.shipment_id
)
,order_map AS 
(
    SELECT
        DISTINCT shipment_id
        ,delivery_station
        ,proin.tier
        ,CASE
            WHEN order_type = 1 THEN 'CB'
            WHEN seller_province IN ('????????????????????????????????????????????????????????????', '??????????????????????????????????????????????????????', '??????????????????????????????????????????', '?????????????????????????????????????????????') THEN 'GBKK'
            ELSE 'UPC'
        END AS origin_region
        ,CASE
            WHEN buyer_province IN ('????????????????????????????????????????????????????????????', '??????????????????????????????????????????????????????', '??????????????????????????????????????????', '?????????????????????????????????????????????') THEN 'GBKK'
            ELSE 'UPC'
        END AS destination_region
        ,CASE
            WHEN chargeable_weight <= 500 THEN '0.5'
            WHEN chargeable_weight <= 1000 THEN '1'
            WHEN chargeable_weight <= 2000 THEN '2'
            WHEN chargeable_weight <= 3000 THEN '3'
            WHEN chargeable_weight <= 4000 THEN '4'
            WHEN chargeable_weight <= 5000 THEN '5'
            WHEN chargeable_weight <= 6000 THEN '6'
            WHEN chargeable_weight <= 7000 THEN '7'
            WHEN chargeable_weight <= 8000 THEN '8'
            WHEN chargeable_weight <= 9000 THEN '9'
            WHEN chargeable_weight <= 10000 THEN '10'
            WHEN chargeable_weight <= 11000 THEN '11'
            WHEN chargeable_weight <= 12000 THEN '12'
            WHEN chargeable_weight <= 13000 THEN '13'
            WHEN chargeable_weight <= 14000 THEN '14'
            WHEN chargeable_weight <= 15000 THEN '15'
            WHEN chargeable_weight <= 16000 THEN '16'
            WHEN chargeable_weight <= 17000 THEN '17'
            WHEN chargeable_weight <= 18000 THEN '18'
            WHEN chargeable_weight <= 19000 THEN '19'
            WHEN chargeable_weight <= 20000 THEN '20'
            else '21'
        END AS weight_tier
  		,chargeable_weight
        ,CASE
            WHEN area.spx_district IS NOT NULL THEN 'Serviceable'
            ELSE 'Non-serviceable'
        END spx_serviceable
        ,delivered_date
    FROM
        fleet_raw
    LEFT JOIN
        thopsbi_lof.spx_province_index_tab proin
    ON  fleet_raw.buyer_district = proin.district
    LEFT JOIN 
    (
        SELECT 
            district_name AS spx_district
        FROM 
            shopee_th.shopee_th_op_team__spx_service_area_v1
        WHERE 
            last_mile_delivery = 'Shopee Express'
    )   area
    ON  fleet_raw.buyer_district = area.spx_district
    WHERE
        delivery_station != 'SPX'
        --AND delivered_date BETWEEN date('2022-01-01') and  date('2022-01-31')
        AND delivered_date between current_date - interval '30' day and current_date - interval '1' day
)
SELECT
    --delivered_date,
    delivery_station
    ,tier
    ,origin_region
    ,destination_region
    ,weight_tier
    ,spx_serviceable
    ,COUNT(DISTINCT(shipment_id)) AS volume
    ,SUM(CASE 
            WHEN delivery_station = 'Kerry Express' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight/1000 AS DOUBLE) - 20)*30) as int) 
            else cast(rate.rate_card as int) 
        end) as cost
    --SUM(CAST(rate.rate_card AS INTEGER)) AS cost,
FROM
    order_map
LEFT JOIN
    thopsbi_lof.spx_card_tier_tab rate
ON  order_map.origin_region = rate.origin
AND order_map.destination_region = rate.destination
AND order_map.weight_tier = rate.interval
AND order_map.delivery_station = rate.shipment_provider
GROUP BY 
    delivery_station
    ,tier
    ,origin_region
    ,destination_region
    ,weight_tier
    ,spx_serviceable
ORDER BY 
    delivery_station desc
    ,tier
    ,origin_region
    ,destination_region
    ,weight_tier
    ,spx_serviceable