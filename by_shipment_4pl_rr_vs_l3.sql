with kerry_apr as
(
select 
    fleet_4pl.shipment_id
    ,case 
        when "4pl_name" = 'FLASH' then 'Flash Express'
        when "4pl_name" = 'KERRY' then 'Kerry Express'
        when "4pl_name" = 'NINJA VAN' then 'Ninja Van'
        when "4pl_name" = 'CJ' then 'CJ Logistics'
        else null 
    end as delivery_station_4pl
    ,case
        when warehouse_flag = true then 'GBKK'
        when cross_border_flag = true then 'GBKK'
        when pickup_region is not null then pickup_region 
        else dop_region end as seller_region_name
    ,buyer_region_name
    ,seller_province_name as dtm_seller_province
    ,buyer_province_name as dtm_buyer_province
    ,case 
        when "4pl_name" = 'FLASH' then sc_weight_in_kg
        when chargeable_weight_in_kg is not null then chargeable_weight_in_kg
        else sc_weight_in_kg
    end as chargeable_weight_in_kg
    ,fleet_order.cod_amount as cod_amount_dtm 
from thopsbi_lof.dwd_thspx_4pl_shipment_info_di_th as fleet_4pl 
left join 
(
select 
    shipment_id
    ,cod_amount
    ,chargeable_weight_in_kg
    ,sc_weight_in_kg
    ,buyer_region_name
    ,seller_region_name
    ,seller_province_name
    ,seller_district_name
    ,buyer_district_name
    ,buyer_province_name
    ,delivered_timestamp
    ,cross_border_flag
    ,warehouse_flag
    ,case 
        when latest_pickup_area_name = 'GBKK' then 'GBKK' 
        else 'UPC' 
    end as pickup_region 
    ,case 
        when dop_station_province_name in ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 'GBKK' 
        ELSE 'UPC' 
    end as dop_region 
from thopsbi_lof.dwd_thspx_pub_shipment_info_di_th 
) as fleet_order 
on fleet_4pl.shipment_id = fleet_order.shipment_id 
where 
    "4pl_name" = 'KERRY' 
    and date("4pl_delivered_timestamp") between date('2022-04-01') and date('2022-04-30') 
)
,raw_4pl as 
(
select 
    fleet_order.shipment_id
    ,kerry_apr.shipment_id as dtm_shipment_id
    ,fleet_order.order_type
   ,CASE
        WHEN fleet_order.station_id = 37 OR fleet_order.channel_id = 50002 THEN 'Flash Express'
        WHEN fleet_order.station_id IN (121,122) OR fleet_order.channel_id = 50003 THEN 'CJ Logistics'
        WHEN fleet_order.station_id = 188 THEN 'Ninja Van'
        WHEN fleet_order.station_id IN (5,170) THEN 'Kerry Express'
        WHEN fleet_order.station_id = 25 OR fleet_order.channel_id = 50001 THEN 'jnt'
        ELSE 'SPX' 
    END AS delivery_station
    ,delivery_station_4pl as delivery_station_dtm 
    ,seller_region_name as dtm_seller_region
    ,buyer_region_name as dtm_buyer_region
    ,CASE 
        WHEN pickup.seller_province IS NOT NULL THEN pickup.seller_province
        ELSE dropoff.seller_province 
    END AS seller_province
    ,buyer_info.buyer_addr_state AS buyer_province
    ,delivered_date
    ,dtm_buyer_province
    ,dtm_seller_province
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
    ,cast(chargeable_weight as double)/1000 as chargeable_weight
    ,chargeable_weight_in_kg as chargeable_weight_dtm 
    ,CASE
        WHEN chargeable_weight_in_kg <= 0.5 THEN '0.5'
        WHEN chargeable_weight_in_kg <= 1 THEN '1'
        WHEN chargeable_weight_in_kg <= 2 THEN '2'
        WHEN chargeable_weight_in_kg <= 3 THEN '3'
        WHEN chargeable_weight_in_kg <= 4 THEN '4'
        WHEN chargeable_weight_in_kg <= 5 THEN '5'
        WHEN chargeable_weight_in_kg <= 6 THEN '6'
        WHEN chargeable_weight_in_kg <= 7 THEN '7'
        WHEN chargeable_weight_in_kg <= 8 THEN '8'
        WHEN chargeable_weight_in_kg <= 9 THEN '9'
        WHEN chargeable_weight_in_kg <= 10 THEN '10'
        WHEN chargeable_weight_in_kg <= 11 THEN '11'
        WHEN chargeable_weight_in_kg <= 12 THEN '12'
        WHEN chargeable_weight_in_kg <= 13 THEN '13'
        WHEN chargeable_weight_in_kg <= 14 THEN '14'
        WHEN chargeable_weight_in_kg <= 15 THEN '15'
        WHEN chargeable_weight_in_kg <= 16 THEN '16'
        WHEN chargeable_weight_in_kg <= 17 THEN '17'
        WHEN chargeable_weight_in_kg <= 18 THEN '18'
        WHEN chargeable_weight_in_kg <= 19 THEN '19'
        WHEN chargeable_weight_in_kg <= 20 THEN '20'
        else '21'
    END AS weight_tier_dtm
    ,fleet_order.cod_amount
    ,cod_amount_dtm
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live fleet_order
left join 
(
select
    shipment_id
    ,status_date as delivered_date
from 
    ( 
    SELECT 
        shipment_id
        ,DATE(FROM_UNIXTIME(ctime-3600)) AS status_date
        ,status
        ,ROW_NUMBER() OVER(PARTITION BY shipment_id/* , DATE(FROM_UNIXTIME(ctime-3600)), status */ ORDER BY ctime DESC) AS rank_num
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    WHERE 
        status IN (4,81) 
        AND operator NOT LIKE '%SPX%' 
        AND DATE(FROM_UNIXTIME(ctime-3600)) >= DATE('2021-09-01')
    )
where rank_num = 1 
    )  as deliver_status_track 
ON fleet_order.shipment_id = deliver_status_track.shipment_id
full outer join kerry_apr
on kerry_apr.shipment_id = fleet_order.shipment_id
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live AS station 
ON fleet_order.station_id = station.id
LEFT JOIN 
    (
    SELECT 
        pickup1.pickup_order_id
        ,seller_addr_state AS seller_province
        ,seller_addr_district AS seller_district
    FROM spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_daily_s0_live pickup1
    INNER JOIN 
        (
        SELECT
            pickup_order_id
            ,MAX(ctime) AS latest_ctime 
        FROM spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_daily_s0_live
        GROUP BY pickup_order_id
        ) AS pickup2
    ON pickup1.pickup_order_id = pickup2.pickup_order_id
    AND pickup1.ctime = pickup2.latest_ctime   
    )   pickup
ON pickup.pickup_order_id = fleet_order.shipment_id
LEFT JOIN 
(
    SELECT 
        shipment_id,
        seller_state AS seller_province,
        seller_city AS seller_district
    FROM spx_mart.shopee_fms_th_db__dropoff_order_tab__th_daily_s0_live
) as dropoff
ON dropoff.shipment_id = fleet_order.shipment_id
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live  AS buyer_info
ON fleet_order.shipment_id = buyer_info.shipment_id
)
,map_region as 
(
select 
    shipment_id
    ,dtm_shipment_id
    ,delivery_station
    ,delivery_station_dtm
    ,cod_amount_dtm
    ,cod_amount
    ,dtm_seller_province
    ,seller_province
    ,dtm_buyer_province
    ,buyer_province
    ,chargeable_weight_dtm
    ,chargeable_weight
    ,weight_tier
    ,weight_tier_dtm
    ,case 
        when dtm_seller_region != 'GBKK' then 'UPC' 
        else 'GBKK' 
    end as dtm_seller_region
    ,CASE
        WHEN order_type IN (1,0) THEN 'GBKK'  
        WHEN seller_province IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 'GBKK'
        ELSE 'UPC' 
    END AS seller_region 
    ,dtm_buyer_region
    ,CASE 
        WHEN buyer_province IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 'GBKK'
        ELSE 'UPC' 
    END as buyer_region
from raw_4pl
where (delivery_station = 'Kerry Express' and delivered_date between date('2022-04-01') and date('2022-04-30')) or delivery_station_dtm = 'Kerry Express' 
)
select 
    shipment_id
    ,dtm_shipment_id
    ,delivery_station_dtm
    ,delivery_station
    /* ,cod_amount_dtm
    ,cod_amount */
    ,weight_tier_dtm
    ,weight_tier
    ,rate_dtm.rate_card as rate_dtm
    ,rate_fms.rate_card as rate_fms 
   /*  ,dtm_seller_province
    ,seller_province
    ,dtm_buyer_province
    ,buyer_province */
    ,dtm_seller_region
    ,seller_region
    ,dtm_buyer_region
    ,buyer_region
from map_region
LEFT JOIN thopsbi_lof.spx_card_tier_tab as rate_fms 
ON  map_region.seller_region = rate_fms.origin
AND map_region.buyer_region = rate_fms.destination
AND map_region.weight_tier = rate_fms.interval
AND map_region.delivery_station = rate_fms.shipment_provider
LEFT JOIN thopsbi_lof.spx_card_tier_tab as rate_dtm 
ON  map_region.dtm_seller_region = rate_dtm.origin
AND map_region.dtm_buyer_region = rate_dtm.destination
AND map_region.weight_tier_dtm = rate_dtm.interval
AND map_region.delivery_station_dtm = rate_dtm.shipment_provider
where 
    (rate_fms.rate_card != rate_dtm.rate_card) 
--where /* (cod_amount_dtm != cod_amount) or (cod_amount_dtm is null and cod_amount is not null) and */ delivery_station = 'Ninja Van'

