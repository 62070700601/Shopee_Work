WITH overflow_check as
(
select 
    fleet_order.shipment_id
    ,case 
        when origin_path.station_name is not null then origin_path.station_name 
        else station.station_name 
    end as last_station_name
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
        where origin_order_path != '[]'
        ) as odp
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station
    on odp.last_staton_id = station.id
    ) as origin_path
on origin_path.shipment_id = fleet_order.shipment_id
left join 
    (
    select 
        shipment_id 
        ,order_path
        ,try(cast(replace(ltrim(split_part(order_path,',',cardinality(CAST(JSON_PARSE(order_path) AS ARRAY<INT>)))),']','') as int)) as last_staton_id
    from 
        (
        select 
            shipment_id 
            ,order_path
            ,ROW_NUMBER() over(partition  by shipment_id order by ctime) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 42 
            and date(from_unixtime(ctime - 3600)) >= current_date - interval '40' day 
        )
    where rank_num = 1 
    ) as fm_path 
on fm_path.shipment_id = fleet_order.shipment_id
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station
on fm_path.last_staton_id = station.id
)
,fleet_raw AS 
(
SELECT
    fleet_order.shipment_id
    ,fleet_order.order_type
    ,CASE
        WHEN fleet_order.station_id = 37 OR fleet_order.channel_id = 50002 THEN 'Flash Express'
        WHEN fleet_order.station_id IN (121,122) OR fleet_order.channel_id = 50003 THEN 'CJ Logistics'
        WHEN fleet_order.station_id = 188 THEN 'Ninja Van'
        WHEN fleet_order.station_id IN (5,170) THEN 'Kerry Express'
        WHEN fleet_order.station_id = 25 OR fleet_order.channel_id = 50001 THEN 'jnt'
        ELSE 'SPX' 
    END AS delivery_station
    ,CASE 
        WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 THEN 1 ELSE 0 END AS is_4pl
    ,CASE 
        WHEN pickup.seller_province IS NOT NULL THEN pickup.seller_province
        ELSE dropoff.seller_province 
    END AS seller_province
    ,buyer_info.buyer_addr_state AS buyer_province
    ,buyer_info.buyer_addr_district AS buyer_district
    /*CASE
        WHEN fleet_order.chargeable_weight IS NULL THEN fleet_order.sc_Weight
        ELSE fleet_order.chargeable_weight
    END AS chargeable_weight,
    */
    ,case 
        when (fleet_order.station_id = 37 OR fleet_order.channel_id = 50002) then fleet_order.sc_weight 
        when (fleet_order.station_id != 37 and fleet_order.channel_id != 50002) and fleet_order.chargeable_weight is not null then fleet_order.chargeable_weight
        else fleet_order.sc_weight  
    end as chargeable_weight
    ----
    ,remote_4pl_map.is_flash_remote
    ,remote_4pl_map.is_kerry_remote
    ,remote_4pl_map.is_ninjavan_remote
    ----
    ,case 
        when fleet_order.payment_method = 'COD' then 1 
        else 0 
    end as is_cod
    ,CAST(fleet_order.cod_amount AS DOUBLE) AS cod_amount
    ---
    ,deliver_status_track.delivered_date
    ,return_status_track.returned_date
FROM spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live fleet_order
LEFT JOIN 
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
            ,ROW_NUMBER() OVER(PARTITION BY shipment_id, DATE(FROM_UNIXTIME(ctime-3600)), status ORDER BY ctime DESC) AS rank_num
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        WHERE 
            status IN (4,81) 
            AND operator NOT LIKE '%SPX%' 
            AND DATE(FROM_UNIXTIME(ctime-3600)) >= DATE('2021-09-01')
        )
    where rank_num = 1 
    )  as deliver_status_track 
ON fleet_order.shipment_id = deliver_status_track.shipment_id
LEFT JOIN 
    (
    select 
        shipment_id
        ,status_date as returned_date
    from 
        ( 
        SELECT 
            shipment_id
            ,DATE(FROM_UNIXTIME(ctime-3600)) AS status_date
            ,status
            ,ROW_NUMBER() OVER(PARTITION BY shipment_id, DATE(FROM_UNIXTIME(ctime-3600)), status ORDER BY ctime ) AS rank_num
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        WHERE 
            status IN (58,95) 
            AND DATE(FROM_UNIXTIME(ctime-3600)) >= DATE('2021-09-01')
        )
    where rank_num = 1 
    ) as return_status_track 
ON fleet_order.shipment_id = return_status_track.shipment_id
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
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live AS buyer_info
ON fleet_order.shipment_id = buyer_info.shipment_id
left join thopsbi_lof.spx_4pl_remote_district_index as remote_4pl_map
on remote_4pl_map.zip_code = buyer_info.buyer_zipcode
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live AS station 
ON fleet_order.station_id = station.id
)
,order_map AS 
(
SELECT
    fleet_raw.shipment_id
    ,delivery_station
    ,proin.tier
    ,CASE
        WHEN order_type = 1 THEN 'CB'
        WHEN seller_province IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 'GBKK'
        ELSE 'UPC'
    END AS origin_region
    ,CASE
        WHEN buyer_province IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 'GBKK'
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
  	,cast(chargeable_weight/1000 as double) as chargeable_weight
    ,case when overflow_check.last_station_name like 'H%' then 1 else 0 end as is_spx_serviceable
    ,delivered_date
    ,returned_date
    ,case 
        WHEN delivery_station = 'Flash Express' and is_flash_remote = 'TRUE' then 1 
        else 0 
    end as is_flash_remote
    ,CASE WHEN delivery_station = 'Kerry Express' AND is_kerry_remote = 'TRUE' then 1 else 0 end as is_kerry_remote
    ,CASE WHEN delivery_station = 'Ninja Van' AND is_ninjavan_remote = 'TRUE' then 1 else 0 end as is_njv_remote
    ,is_cod
    ,cod_amount
FROM fleet_raw
LEFT JOIN thopsbi_lof.spx_province_index_tab proin
ON fleet_raw.buyer_district = proin.district
and proin.province = fleet_raw.buyer_province
left join overflow_check
on overflow_check.shipment_id = fleet_raw.shipment_id 
WHERE 
    delivery_station != 'SPX'
    and is_4pl = 1        
)
select
    shipment_id
    ,delivery_station
    ,destination_region
from order_map 
where 
    destination_region = 'GBKK' 
    and delivered_date between date('2022-01-01') and date('2022-01-31')