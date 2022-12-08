WITH 
overflow_check as
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
        where 
            origin_order_path != '[]'
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
    where 
        rank_num = 1 
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
        WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 
        THEN 1 
        ELSE 0 
    END AS is_4pl
    ,CASE 
        WHEN pickup.seller_province IS NOT NULL THEN pickup.seller_province
        ELSE dropoff.seller_province 
    END AS seller_province
    ,buyer_info.buyer_addr_state AS buyer_province
    ,buyer_info.buyer_addr_district AS buyer_district
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
        ,ROW_NUMBER() OVER(PARTITION BY shipment_id/* , DATE(FROM_UNIXTIME(ctime-3600)), status */ ORDER BY ctime DESC) AS rank_num
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    WHERE 
        status IN (4,81) 
        AND operator NOT LIKE '%SPX%' 
        AND DATE(FROM_UNIXTIME(ctime-3600)) >= DATE('2021-09-01')
        )
where 
    rank_num = 1 
) as deliver_status_track 
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
        ,ROW_NUMBER() OVER(PARTITION BY shipment_id/* , DATE(FROM_UNIXTIME(ctime-3600)), status */ ORDER BY ctime ) AS rank_num
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    WHERE 
        status IN (58,95) 
        AND DATE(FROM_UNIXTIME(ctime-3600)) >= DATE('2021-09-01')
    )
where 
    rank_num = 1 
)  as return_status_track 
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
GROUP BY
    pickup_order_id
) AS pickup2
ON pickup1.pickup_order_id = pickup2.pickup_order_id
AND pickup1.ctime = pickup2.latest_ctime   
) as pickup
ON  pickup.pickup_order_id = fleet_order.shipment_id
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
        WHEN order_type IN (1,0) THEN 'GBKK'
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
        WHEN chargeable_weight > 20000 THEN '21'
    END AS weight_tier
  	,cast(chargeable_weight/1000 as double) as chargeable_weight
    ,case 
        when overflow_check.last_station_name like 'H%' then 1 
        else 0 
    end as is_spx_serviceable
    ,delivered_date
    ,returned_date
    ,case 
        WHEN delivery_station = 'Flash Express' and is_flash_remote = 'TRUE' then 1 
        else 0 
    end as is_flash_remote
    ,CASE 
        WHEN delivery_station = 'Kerry Express' AND is_kerry_remote = 'TRUE' then 1 
        else 0 
    end as is_kerry_remote
    ,CASE 
        WHEN delivery_station = 'Ninja Van' AND is_ninjavan_remote = 'TRUE' then 1 
        else 0 
    end as is_njv_remote
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
,reverse_4pl_aggregate as
(
select 
    returned_date as report_date
    -------- volume 
    ,sum(case when delivery_station = 'Flash Express' then 1 else 0 end) as flash_reverse
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier A' then 1 else 0 end) as flash_Ta_reverse
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier B' then 1 else 0 end) as flash_Tb_reverse
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier C' then 1 else 0 end) as flash_Tc_reverse
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier D' then 1 else 0 end) as flash_Td_reverse
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier E' then 1 else 0 end) as flash_Te_reverse
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier F' then 1 else 0 end) as flash_Tf_reverse
    ,sum(case when delivery_station = 'Kerry Express' then 1 else 0 end) as kerry_reverse
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier A' then 1 else 0 end) as kerry_Ta_reverse
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier B' then 1 else 0 end) as kerry_Tb_reverse
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier C' then 1 else 0 end) as kerry_Tc_reverse
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier D' then 1 else 0 end) as kerry_Td_reverse
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier E' then 1 else 0 end) as kerry_Te_reverse
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier F' then 1 else 0 end) as kerry_Tf_reverse
    ,sum(case when delivery_station = 'Ninja Van' then 1 else 0 end) as njv_reverse
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier A' then 1 else 0 end) as njv_Ta_reverse
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier B' then 1 else 0 end) as njv_Tb_reverse
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier C' then 1 else 0 end) as njv_Tc_reverse
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier D' then 1 else 0 end) as njv_Td_reverse
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier E' then 1 else 0 end) as njv_Te_reverse
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier F' then 1 else 0 end) as njv_Tf_reverse
    ---------- cost 
    ,0.5*SUM(CASE WHEN delivery_station = 'Flash Express' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier A' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier A' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_ta_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier B' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier B' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_tb_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier C' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier C' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_tc_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier D' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier D' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_td_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier E' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier E' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_te_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier F' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier F' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_tf_rev_fee

    ,0.5*SUM(CASE WHEN delivery_station = 'Ninja Van' then cast(rate.rate_card as int) else 0 end) as njv_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier A' then cast(rate.rate_card as int) else 0 end) as njv_ta_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier B' then cast(rate.rate_card as int) else 0 end) as njv_tb_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier C' then cast(rate.rate_card as int) else 0 end) as njv_tc_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier D' then cast(rate.rate_card as int) else 0 end) as njv_td_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier E' then cast(rate.rate_card as int) else 0 end) as njv_te_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier F' then cast(rate.rate_card as int) else 0 end) as njv_tf_rev_fee

    ,0.5*SUM(CASE WHEN delivery_station = 'Kerry Express' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier A' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier A' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_ta_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier B' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier B' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_tb_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier C' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier C' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_tc_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier D' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier D' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_td_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier E' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier E' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_te_rev_fee
    ,0.5*SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier F' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier F' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_tf_rev_fee

     -------- reverse 
    ,avg(cast(weight_tier as double) ) as reverse_weight
    ,avg(case when delivery_station = 'Flash Express' then cast(weight_tier as double) else 0 end) as flash_reverse_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier A' then cast(weight_tier as double) end) as flash_Ta_reverse_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier B' then cast(weight_tier as double) end) as flash_Tb_reverse_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier C' then cast(weight_tier as double) end) as flash_Tc_reverse_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier D' then cast(weight_tier as double) end) as flash_Td_reverse_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier E' then cast(weight_tier as double) end) as flash_Te_reverse_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier F' then cast(weight_tier as double) end) as flash_Tf_reverse_weight
    ,avg(case when delivery_station = 'Kerry Express' then cast(weight_tier as double)  end) as kerry_reverse_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier A' then cast(weight_tier as double) end) as kerry_Ta_reverse_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier B' then cast(weight_tier as double) end) as kerry_Tb_reverse_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier C' then cast(weight_tier as double) end) as kerry_Tc_reverse_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier D' then cast(weight_tier as double) end) as kerry_Td_reverse_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier E' then cast(weight_tier as double) end) as kerry_Te_reverse_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier F' then cast(weight_tier as double) end) as kerry_Tf_reverse_weight
    ,avg(case when delivery_station = 'Ninja Van' then cast(weight_tier as double)  end) as njv_reverse_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier A' then cast(weight_tier as double) end) as njv_Ta_reverse_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier B' then cast(weight_tier as double) end) as njv_Tb_reverse_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier C' then cast(weight_tier as double) end) as njv_Tc_reverse_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier D' then cast(weight_tier as double) end) as njv_Td_reverse_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier E' then cast(weight_tier as double) end) as njv_Te_reverse_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier F' then cast(weight_tier as double) end) as njv_Tf_reverse_weight

FROM order_map
LEFT JOIN thopsbi_lof.spx_card_tier_tab as rate
ON order_map.origin_region = rate.origin
AND order_map.destination_region = rate.destination
AND order_map.weight_tier = rate.interval
AND order_map.delivery_station = rate.shipment_provider
where 
    returned_date between current_date - interval '35' day and current_date - interval '1' day 
group by 
    returned_date
)
,forward_4pl_aggregate as
(
SELECT
    delivered_date as report_date
    ,count(*) as  total_volume 
    -------------------- Delivered Volume 
    ,sum(case when delivery_station = 'Kerry Express' and origin_region = 'GBKK' and destination_region = 'GBKK' THEN 1 ELSE 0 END) as kerry_gbkk 
    ,sum(case when delivery_station = 'Kerry Express' and (origin_region = 'UPC' or destination_region = 'UPC') THEN 1 ELSE 0 END) as kerry_upc
    ,sum(case when delivery_station = 'Flash Express' then 1 else 0 end) as flash_del
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier A' then 1 else 0 end) as flash_Ta_del
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier A' and is_spx_serviceable = 1 then 1 else 0 end) as flash_Ta_del_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier A' and is_spx_serviceable = 0 then 1 else 0 end) as flash_Ta_del_non_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier B' then 1 else 0 end) as flash_Tb_del
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier B' and is_spx_serviceable = 1 then 1 else 0 end) as flash_Tb_del_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier B' and is_spx_serviceable = 0 then 1 else 0 end) as flash_Tb_del_non_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier C' then 1 else 0 end) as flash_Tc_del
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier C' and is_spx_serviceable = 1 then 1 else 0 end) as flash_Tc_del_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier C' and is_spx_serviceable = 0 then 1 else 0 end) as flash_Tc_del_non_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier D' then 1 else 0 end) as flash_Td_del
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier D' and is_spx_serviceable = 1 then 1 else 0 end) as flash_Td_del_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier D' and is_spx_serviceable = 0 then 1 else 0 end) as flash_Td_del_non_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier E' then 1 else 0 end) as flash_Te_del
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier E' and is_spx_serviceable = 1 then 1 else 0 end) as flash_Te_del_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier E' and is_spx_serviceable = 0 then 1 else 0 end) as flash_Te_del_non_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier F' then 1 else 0 end) as flash_Tf_del
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier F' and is_spx_serviceable = 1 then 1 else 0 end) as flash_Tf_del_ser
    ,sum(case when delivery_station = 'Flash Express' and tier = 'Tier F' and is_spx_serviceable = 0 then 1 else 0 end) as flash_Tf_del_non_ser
    ,sum(case when delivery_station = 'Kerry Express' then 1 else 0 end) as kerry_del
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier A' then 1 else 0 end) as kerry_Ta_del
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier A' and is_spx_serviceable = 1 then 1 else 0 end) as kerry_Ta_del_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier A' and is_spx_serviceable = 0 then 1 else 0 end) as kerry_Ta_del_non_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier B' then 1 else 0 end) as kerry_Tb_del
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier B' and is_spx_serviceable = 1 then 1 else 0 end) as kerry_Tb_del_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier B' and is_spx_serviceable = 0 then 1 else 0 end) as kerry_Tb_del_non_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier C' then 1 else 0 end) as kerry_Tc_del
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier C' and is_spx_serviceable = 1 then 1 else 0 end) as kerry_Tc_del_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier C' and is_spx_serviceable = 0 then 1 else 0 end) as kerry_Tc_del_non_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier D' then 1 else 0 end) as kerry_Td_del
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier D' and is_spx_serviceable = 1 then 1 else 0 end) as kerry_Td_del_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier D' and is_spx_serviceable = 0 then 1 else 0 end) as kerry_Td_del_non_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier E' then 1 else 0 end) as kerry_Te_del
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier E' and is_spx_serviceable = 1 then 1 else 0 end) as kerry_Te_del_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier E' and is_spx_serviceable = 0 then 1 else 0 end) as kerry_Te_del_non_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier F' then 1 else 0 end) as kerry_Tf_del
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier F' and is_spx_serviceable = 1 then 1 else 0 end) as kerry_Tf_del_ser 
    ,sum(case when delivery_station = 'Kerry Express' and tier = 'Tier F' and is_spx_serviceable = 0 then 1 else 0 end) as kerry_Tf_del_non_ser 
    ,sum(case when delivery_station = 'Ninja Van' then 1 else 0 end) as njv_del
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier A' then 1 else 0 end) as njv_Ta_del
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier A' and is_spx_serviceable = 1 then 1 else 0 end) as njv_Ta_del_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier A' and is_spx_serviceable = 0 then 1 else 0 end) as njv_Ta_del_non_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier B' then 1 else 0 end) as njv_Tb_del
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier B' and is_spx_serviceable = 1 then 1 else 0 end) as njv_Tb_del_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier B' and is_spx_serviceable = 0 then 1 else 0 end) as njv_Tb_del_non_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier C' then 1 else 0 end) as njv_Tc_del
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier C' and is_spx_serviceable = 1 then 1 else 0 end) as njv_Tc_del_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier C' and is_spx_serviceable = 0 then 1 else 0 end) as njv_Tc_del_non_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier D' then 1 else 0 end) as njv_Td_del
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier D' and is_spx_serviceable = 1 then 1 else 0 end) as njv_Td_del_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier D' and is_spx_serviceable = 0 then 1 else 0 end) as njv_Td_del_non_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier E' then 1 else 0 end) as njv_Te_del
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier E' and is_spx_serviceable = 1 then 1 else 0 end) as njv_Te_del_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier E' and is_spx_serviceable = 0 then 1 else 0 end) as njv_Te_del_non_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier F' then 1 else 0 end) as njv_Tf_del
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier F' and is_spx_serviceable = 1 then 1 else 0 end) as njv_Tf_del_ser
    ,sum(case when delivery_station = 'Ninja Van' and tier = 'Tier F' and is_spx_serviceable = 0 then 1 else 0 end) as njv_Tf_del_non_ser
    ---------------------- remote volume 
    ,sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 then 1 else 0 end) as flash_remote
    ,sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier A' then 1 else 0 end) as flash_Ta_remote
    ,sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier B' then 1 else 0 end) as flash_Tb_remote
    ,sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier C' then 1 else 0 end) as flash_Tc_remote
    ,sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier D' then 1 else 0 end) as flash_Td_remote
    ,sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier E' then 1 else 0 end) as flash_Te_remote
    ,sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier F' then 1 else 0 end) as flash_Tf_remote
    ,sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 then 1 else 0 end) as kerry_remote
    ,sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier A' then 1 else 0 end) as kerry_Ta_remote
    ,sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier B' then 1 else 0 end) as kerry_Tb_remote
    ,sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier C' then 1 else 0 end) as kerry_Tc_remote
    ,sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier D' then 1 else 0 end) as kerry_Td_remote
    ,sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier E' then 1 else 0 end) as kerry_Te_remote
    ,sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier F' then 1 else 0 end) as kerry_Tf_remote
    ,sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 then 1 else 0 end) as njv_remote
    ,sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier A' then 1 else 0 end) as njv_Ta_remote
    ,sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier B' then 1 else 0 end) as njv_Tb_remote
    ,sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier C' then 1 else 0 end) as njv_Tc_remote
    ,sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier D' then 1 else 0 end) as njv_Td_remote
    ,sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier E' then 1 else 0 end) as njv_Te_remote
    ,sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier F' then 1 else 0 end) as njv_Tf_remote
    -------------------------- COD Volume 
    ,sum(case when delivery_station = 'Flash Express' and is_cod = 1 then 1 else 0 end) as flash_cod
    ,sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier A' then 1 else 0 end) as flash_ta_cod
    ,sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier B' then 1 else 0 end) as flash_tb_cod
    ,sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier C' then 1 else 0 end) as flash_tc_cod
    ,sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier D' then 1 else 0 end) as flash_td_cod
    ,sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier E' then 1 else 0 end) as flash_te_cod
    ,sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier F' then 1 else 0 end) as flash_tf_cod
    ,sum(case when delivery_station = 'Kerry Express' and is_cod = 1 then 1 else 0 end) as kerry_cod
    ,sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier A' then 1 else 0 end) as kerry_Tt_cod
    ,sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier B' then 1 else 0 end) as kerry_tb_cod
    ,sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier C' then 1 else 0 end) as kerry_tc_cod
    ,sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier D' then 1 else 0 end) as kerry_td_cod
    ,sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier E' then 1 else 0 end) as kerry_te_cod
    ,sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier F' then 1 else 0 end) as kerry_tf_cod
    ,sum(case when delivery_station = 'Ninja Van' and is_cod = 1 then 1 else 0 end) as njv_cod
    ,sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier A' then 1 else 0 end) as njv_ta_cod
    ,sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier B' then 1 else 0 end) as njv_tb_cod
    ,sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier C' then 1 else 0 end) as njv_tc_cod
    ,sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier D' then 1 else 0 end) as njv_td_cod
    ,sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier E' then 1 else 0 end) as njv_te_cod
    ,sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier F' then 1 else 0 end) as njv_tf_cod

    --- weight overall
    ,avg(cast(weight_tier as double) ) as total_avg_weight 
    ,avg(case when delivery_station = 'Flash Express' then cast(weight_tier as double)  end) as flash_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier A' then cast(weight_tier as double) end) as flash_Ta_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier B' then cast(weight_tier as double) end) as flash_Tb_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier C' then cast(weight_tier as double) end) as flash_Tc_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier D' then cast(weight_tier as double) end) as flash_Td_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier E' then cast(weight_tier as double) end) as flash_Te_weight
    ,avg(case when delivery_station = 'Flash Express' and tier = 'Tier F' then cast(weight_tier as double) end) as flash_Tf_weight
    ,avg(case when delivery_station = 'Kerry Express' then cast(weight_tier as double)  end) as kerry_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier A' then cast(weight_tier as double) end) as kerry_Ta_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier B' then cast(weight_tier as double) end) as kerry_Tb_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier C' then cast(weight_tier as double) end) as kerry_Tc_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier D' then cast(weight_tier as double) end) as kerry_Td_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier E' then cast(weight_tier as double) end) as kerry_Te_weight
    ,avg(case when delivery_station = 'Kerry Express' and tier = 'Tier F' then cast(weight_tier as double) end) as kerry_Tf_weight
    ,avg(case when delivery_station = 'Ninja Van' then cast(weight_tier as double)  end) as njv_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier A' then cast(weight_tier as double) end) as njv_Ta_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier B' then cast(weight_tier as double) end) as njv_Tb_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier C' then cast(weight_tier as double) end) as njv_Tc_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier D' then cast(weight_tier as double) end) as njv_Td_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier E' then cast(weight_tier as double) end) as njv_Te_weight
    ,avg(case when delivery_station = 'Ninja Van' and tier = 'Tier F' then cast(weight_tier as double) end) as njv_Tf_weight 
    --- remote weight
    ,avg(case when is_flash_remote = 1 then cast(weight_tier as double)  end) as remote_weight 
    ,avg(case when delivery_station = 'Flash Express' and is_flash_remote = 1 then cast(weight_tier as double)  end) as flash_remote_weight
    ,avg(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier A' then cast(weight_tier as double) end) as flash_Ta_remote_weight
    ,avg(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier B' then cast(weight_tier as double) end) as flash_Tb_remote_weight
    ,avg(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier C' then cast(weight_tier as double) end) as flash_Tc_remote_weight
    ,avg(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier D' then cast(weight_tier as double) end) as flash_Td_remote_weight
    ,avg(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier E' then cast(weight_tier as double) end) as flash_Te_remote_weight
    ,avg(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier F' then cast(weight_tier as double) end) as flash_Tf_remote_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 then cast(weight_tier as double)  end) as kerry_remote_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier A' then cast(weight_tier as double) end) as kerry_Ta_remote_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier B' then cast(weight_tier as double) end) as kerry_Tb_remote_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier C' then cast(weight_tier as double) end) as kerry_Tc_remote_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier D' then cast(weight_tier as double) end) as kerry_Td_remote_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier E' then cast(weight_tier as double) end) as kerry_Te_remote_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier F' then cast(weight_tier as double) end) as kerry_Tf_remote_weight 
    ,avg(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 then cast(weight_tier as double)  end) as njv_remote_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier A' then cast(weight_tier as double) end) as njv_Ta_remote_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier B' then cast(weight_tier as double) end) as njv_Tb_remote_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier C' then cast(weight_tier as double) end) as njv_Tc_remote_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier D' then cast(weight_tier as double) end) as njv_Td_remote_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier E' then cast(weight_tier as double) end) as njv_Te_remote_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier F' then cast(weight_tier as double) end) as njv_Tf_remote_weight
    --- COD weight 
    ,avg(case when is_cod = 1 then cast(weight_tier as double)  end) as cod_weight
    ,avg(case when delivery_station = 'Flash Express' and is_cod = 1 then cast(weight_tier as double)  end) as flash_cod_weight
    ,avg(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier A' then cast(weight_tier as double) end) as flash_ta_cod_weight
    ,avg(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier B' then cast(weight_tier as double) end) as flash_tb_cod_weight
    ,avg(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier C' then cast(weight_tier as double) end) as flash_tc_cod_weight
    ,avg(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier D' then cast(weight_tier as double) end) as flash_td_cod_weight
    ,avg(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier E' then cast(weight_tier as double) end) as flash_te_cod_weight
    ,avg(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier F' then cast(weight_tier as double) end) as flash_tf_cod_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_cod = 1 then cast(weight_tier as double)  end) as kerry_cod_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier A' then cast(weight_tier as double) end) as kerry_Tt_cod_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier B' then cast(weight_tier as double) end) as kerry_tb_cod_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier C' then cast(weight_tier as double) end) as kerry_tc_cod_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier D' then cast(weight_tier as double) end) as kerry_td_cod_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier E' then cast(weight_tier as double) end) as kerry_te_cod_weight
    ,avg(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier F' then cast(weight_tier as double) end) as kerry_tf_cod_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_cod = 1 then cast(weight_tier as double)  end) as njv_cod_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier A' then cast(weight_tier as double) end) as njv_ta_cod_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier B' then cast(weight_tier as double) end) as njv_tb_cod_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier C' then cast(weight_tier as double) end) as njv_tc_cod_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier D' then cast(weight_tier as double) end) as njv_td_cod_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier E' then cast(weight_tier as double) end) as njv_te_cod_weight
    ,avg(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier F' then cast(weight_tier as double) end) as njv_tf_cod_weight

    ---------------------------- Cost 
    ,SUM(CASE WHEN delivery_station = 'Flash Express' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_cost
    ,SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier A' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier A' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_ta_cost
    ,SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier B' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier B' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_tb_cost
    ,SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier C' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier C' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_tc_cost
    ,SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier D' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier D' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_td_cost
    ,SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier E' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier E' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_te_cost
    ,SUM(CASE WHEN delivery_station = 'Flash Express' and tier = 'Tier F' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*15) as int) 
              WHEN delivery_station = 'Flash Express' and tier = 'Tier F' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as flash_tf_cost

    ,SUM(CASE WHEN delivery_station = 'Ninja Van' then cast(rate.rate_card as int) else 0 end) as njv_cost
    ,SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier A' then cast(rate.rate_card as int) else 0 end) as njv_ta_cost
    ,SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier B' then cast(rate.rate_card as int) else 0 end) as njv_tb_cost
    ,SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier C' then cast(rate.rate_card as int) else 0 end) as njv_tc_cost
    ,SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier D' then cast(rate.rate_card as int) else 0 end) as njv_td_cost
    ,SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier E' then cast(rate.rate_card as int) else 0 end) as njv_te_cost
    ,SUM(CASE WHEN delivery_station = 'Ninja Van' and tier = 'Tier F' then cast(rate.rate_card as int) else 0 end) as njv_tf_cost

    ,SUM(CASE WHEN delivery_station = 'Kerry Express' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_cost
    ,SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier A' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier A' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_ta_cost
    ,SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier B' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier B' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_tb_cost
    ,SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier C' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier C' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_tc_cost
    ,SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier D' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier D' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_td_cost
    ,SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier E' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier E' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_te_cost
    ,SUM(CASE WHEN delivery_station = 'Kerry Express' and tier = 'Tier F' and weight_tier = '21' THEN cast(rate.rate_card as int) + cast(((CAST(chargeable_weight AS DOUBLE) - 20)*30) as int) 
              WHEN delivery_station = 'Kerry Express' and tier = 'Tier F' and weight_tier != '21' THEN cast(rate.rate_card as int) else 0 end) as kerry_tf_cost
    ---------------------------- Remote Cost    
    ,50*sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 then 1 else 0 end) as flash_remote_fee
    ,50*sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier A' then 1 else 0 end) as flash_Ta_remote_fee
    ,50*sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier B' then 1 else 0 end) as flash_Tb_remote_fee
    ,50*sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier C' then 1 else 0 end) as flash_Tc_remote_fee
    ,50*sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier D' then 1 else 0 end) as flash_Td_remote_fee
    ,50*sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier E' then 1 else 0 end) as flash_Te_remote_fee
    ,50*sum(case when delivery_station = 'Flash Express' and is_flash_remote = 1 and tier = 'Tier F' then 1 else 0 end) as flash_Tf_remote_fee
    ,50*sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 then 1 else 0 end) as kerry_remote_fee
    ,50*sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier A' then 1 else 0 end) as kerry_Ta_remote_fee
    ,50*sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier B' then 1 else 0 end) as kerry_Tb_remote_fee
    ,50*sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier C' then 1 else 0 end) as kerry_Tc_remote_fee
    ,50*sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier D' then 1 else 0 end) as kerry_Td_remote_fee
    ,50*sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier E' then 1 else 0 end) as kerry_Te_remote_fee
    ,50*sum(case when delivery_station = 'Kerry Express' and is_kerry_remote = 1 and tier = 'Tier F' then 1 else 0 end) as kerry_Tf_remote_fee
    ,50*sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 then 1 else 0 end) as njv_remote_fee
    ,50*sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier A' then 1 else 0 end) as njv_Ta_remote_fee
    ,50*sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier B' then 1 else 0 end) as njv_Tb_remote_fee
    ,50*sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier C' then 1 else 0 end) as njv_Tc_remote_fee
    ,50*sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier D' then 1 else 0 end) as njv_Td_remote_fee
    ,50*sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier E' then 1 else 0 end) as njv_Te_remote_fee
    ,50*sum(case when delivery_station = 'Ninja Van' and is_njv_remote = 1 and tier = 'Tier F' then 1 else 0 end) as njv_Tf_remote_fee
    ------------------------------ COD cost 
    ,0.01*sum(case when delivery_station = 'Flash Express' and is_cod = 1 then cod_amount else 0 end) as flash_cod_fee
    ,0.01*sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier A' then cod_amount else 0 end) as flash_ta_cod_fee
    ,0.01*sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier B' then cod_amount else 0 end) as flash_tb_cod_fee
    ,0.01*sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier C' then cod_amount else 0 end) as flash_tc_cod_fee
    ,0.01*sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier D' then cod_amount else 0 end) as flash_td_cod_fee
    ,0.01*sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier E' then cod_amount else 0 end) as flash_te_cod_fee
    ,0.01*sum(case when delivery_station = 'Flash Express' and is_cod = 1 and tier = 'Tier F' then cod_amount else 0 end) as flash_tf_cod_fee
    ,0.01*sum(case when delivery_station = 'Kerry Express' and is_cod = 1 then cod_amount else 0 end) as kerry_cod_fee
    ,0.01*sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier A' then cod_amount else 0 end) as kerry_Tt_cod_fee
    ,0.01*sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier B' then cod_amount else 0 end) as kerry_tb_cod_fee
    ,0.01*sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier C' then cod_amount else 0 end) as kerry_tc_cod_fee
    ,0.01*sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier D' then cod_amount else 0 end) as kerry_td_cod_fee
    ,0.01*sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier E' then cod_amount else 0 end) as kerry_te_cod_fee
    ,0.01*sum(case when delivery_station = 'Kerry Express' and is_cod = 1 and tier = 'Tier F' then cod_amount else 0 end) as kerry_tf_cod_fee
    ,0.005*sum(case when delivery_station = 'Ninja Van' and is_cod = 1 then cod_amount else 0 end) as njv_cod_fee
    ,0.005*sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier A' then cod_amount else 0 end) as njv_ta_cod_fee
    ,0.005*sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier B' then cod_amount else 0 end) as njv_tb_cod_fee
    ,0.005*sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier C' then cod_amount else 0 end) as njv_tc_cod_fee
    ,0.005*sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier D' then cod_amount else 0 end) as njv_td_cod_fee
    ,0.005*sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier E' then cod_amount else 0 end) as njv_te_cod_fee
    ,0.005*sum(case when delivery_station = 'Ninja Van' and is_cod = 1 and tier = 'Tier F' then cod_amount else 0 end) as njv_tf_cod_fee
FROM order_map
LEFT JOIN thopsbi_lof.spx_card_tier_tab as rate
ON order_map.origin_region = rate.origin
AND order_map.destination_region = rate.destination
AND order_map.weight_tier = rate.interval
AND order_map.delivery_station = rate.shipment_provider
where delivered_date between current_date - interval '35' day and current_date - interval '1' day
group by
    delivered_date
)
select 
    --delivered volume 
    sum(flash_del+kerry_del+njv_del) as total_delivered
    ,sum(flash_del) as flash_del
    ,sum(flash_Ta_del) as flash_Ta_del
    ,sum(flash_Ta_del_ser) as flash_Ta_del_ser
    ,sum(flash_Ta_del_non_ser) as flash_Ta_del_non_ser
    ,sum(flash_Tb_del) as flash_Tb_del
    ,sum(flash_Tb_del_ser) as flash_Tb_del_ser
    ,sum(flash_Tb_del_non_ser) as flash_Tb_del_non_ser
    ,sum(flash_Tc_del) as flash_Tc_del
    ,sum(flash_Tc_del_ser) as flash_Tc_del_ser
    ,sum(flash_Tc_del_non_ser) as flash_Tc_del_non_ser
    ,sum(flash_Td_del) as flash_Td_del
    ,sum(flash_Td_del_ser) as flash_Td_del_ser
    ,sum(flash_Td_del_non_ser) as flash_Td_del_non_ser 
    ,sum(flash_Te_del) as flash_Te_del
    ,sum(flash_Te_del_ser) as flash_Te_del_ser
    ,sum(flash_Te_del_non_ser) as flash_Te_del_non_ser
    ,sum(flash_Tf_del) as flash_Tf_del
    ,sum(flash_Tf_del_ser) as flash_Tf_del_ser
    ,sum(flash_Tf_del_non_ser) as flash_Tf_del_non_ser
    ,sum(kerry_del) as kerry_del
    ,sum(kerry_Ta_del) as kerry_Ta_del
    ,sum(kerry_Ta_del_ser) as kerry_Ta_del_ser
    ,sum(kerry_Ta_del_non_ser) as  kerry_Ta_del_non_ser
    ,sum(kerry_Tb_del) as kerry_Tb_del
    ,sum(kerry_Tb_del_ser) as kerry_Tb_del_ser 
    ,sum(kerry_Tb_del_non_ser) as kerry_Tb_del_non_ser
    ,sum(kerry_Tc_del) as kerry_Tc_del
    ,sum(kerry_Tc_del_ser) as kerry_Tc_del_ser
    ,sum(kerry_Tc_del_non_ser) as  kerry_Tc_del_non_ser
    ,sum(kerry_Td_del) as kerry_Td_del
    ,sum(kerry_Td_del_ser) as kerry_Td_del_ser
    ,sum(kerry_Td_del_non_ser) as kerry_Td_del_non_ser
    ,sum(kerry_Te_del) as kerry_Te_del
    ,sum(kerry_Te_del_ser) as kerry_Te_del_ser
    ,sum(kerry_Te_del_non_ser) as kerry_Te_del_non_ser
    ,sum(kerry_Tf_del) as kerry_Tf_del
    ,sum(kerry_Tf_del_ser) as kerry_Tf_del_ser
    ,sum(kerry_Tf_del_non_ser) as kerry_Tf_del_non_ser  
    ,sum(njv_del) as njv_del
    ,sum(njv_Ta_del) as njv_Ta_del 
    ,sum(njv_Ta_del_ser) as njv_Ta_del_ser
    ,sum(njv_Ta_del_non_ser) as njv_Ta_del_non_ser
    ,sum(njv_Tb_del) as njv_Tb_del
    ,sum(njv_Tb_del_ser) as njv_Tb_del_ser 
    ,sum(njv_Tb_del_non_ser) as njv_Tb_del_non_ser
    ,sum(njv_Tc_del) as njv_Tc_del
    ,sum(njv_Tc_del_ser) as njv_Tc_del_ser
    ,sum(njv_Tc_del_non_ser) as njv_Tc_del_non_ser
    ,sum(njv_Td_del) as njv_Td_del
    ,sum(njv_Td_del_ser) as njv_Td_del_ser
    ,sum(njv_Td_del_non_ser) as njv_Td_del_non_ser
    ,sum(njv_Te_del) as njv_Te_del
    ,sum(njv_Te_del_ser) as njv_Te_del_ser
    ,sum(njv_Te_del_non_ser) as njv_Te_del_non_ser
    ,sum(njv_Tf_del) as njv_Tf_del
    ,sum(njv_Tf_del_ser) as njv_Tf_del_ser
    ,sum(njv_Tf_del_non_ser) as njv_Tf_del_non_ser
    --- remote volume 
    ,sum(flash_remote+kerry_remote+njv_remote) as total_remote_volume 
    ,sum(flash_remote) as flash_remote
    ,sum(flash_Ta_remote) as flash_Ta_remote
    ,sum(flash_Tb_remote) as flash_Tb_remote
    ,sum(flash_Tc_remote) as flash_Tc_remote
    ,sum(flash_Td_remote) as flash_Td_remote
    ,sum(flash_Te_remote) as flash_Te_remote
    ,sum(flash_Tf_remote) as flash_Tf_remote
    ,sum(kerry_remote) as kerry_remote
    ,sum(kerry_Ta_remote) as kerry_Ta_remote
    ,sum(kerry_Tb_remote) as kerry_Tb_remote
    ,sum(kerry_Tc_remote) as kerry_Tc_remote
    ,sum(kerry_Td_remote) as kerry_Td_remote
    ,sum(kerry_Te_remote) as kerry_Te_remote
    ,sum(kerry_Tf_remote) as kerry_Tf_remote
    ,sum(njv_remote) as njv_remote
    ,sum(njv_Ta_remote) as njv_Ta_remote
    ,sum(njv_Tb_remote) as njv_Tb_remote
    ,sum(njv_Tc_remote) as njv_Tc_remote
    ,sum(njv_Td_remote) as njv_Td_remote
    ,sum(njv_Te_remote) as njv_Te_remote
    ,sum(njv_Tf_remote) as njv_Tf_remote
    --- COD Volume 
    ,sum(flash_cod+kerry_cod+njv_cod) as total_cod_volume
    ,sum(flash_cod) as flash_cod
    ,sum(flash_ta_cod) as flash_ta_cod
    ,sum(flash_tb_cod) as flash_tb_cod
    ,sum(flash_tc_cod) as flash_tc_cod
    ,sum(flash_td_cod) as flash_td_cod
    ,sum(flash_te_cod) as flash_te_cod
    ,sum(flash_tf_cod) as flash_tf_cod
    ,sum(kerry_cod) as kerry_cod
    ,sum(kerry_Tt_cod) as kerry_Tt_cod
    ,sum(kerry_tb_cod) as kerry_tb_cod
    ,sum(kerry_tc_cod) as kerry_tc_cod
    ,sum(kerry_td_cod) as kerry_td_cod
    ,sum(kerry_te_cod) as kerry_te_cod
    ,sum(kerry_tf_cod) as kerry_tf_cod
    ,sum(njv_cod) as njv_cod
    ,sum(njv_ta_cod) as njv_ta_cod
    ,sum(njv_tb_cod) as njv_tb_cod
    ,sum(njv_tc_cod) as njv_tc_cod
    ,sum(njv_td_cod) as njv_td_cod
    ,sum(njv_te_cod) as njv_te_cod 
    ,sum(njv_tf_cod) as njv_tf_cod
    ---- reverse volume 
    ,sum(flash_reverse+njv_reverse+kerry_reverse) as total_reverse_volume 
    ,sum(flash_reverse) as flash_reverse
    ,sum(flash_Ta_reverse) as flash_Ta_reverse
    ,sum(flash_Tb_reverse) as flash_Tb_reverse
    ,sum(flash_Tc_reverse) as flash_Tc_reverse
    ,sum(flash_Td_reverse) as flash_Td_reverse
    ,sum(flash_Te_reverse) as flash_Te_reverse
    ,sum(flash_Tf_reverse) as flash_Tf_reverse
    ,sum(kerry_reverse) as kerry_reverse
    ,sum(kerry_Ta_reverse) as kerry_Ta_reverse
    ,sum(kerry_Tb_reverse) as kerry_Tb_reverse
    ,sum(kerry_Tc_reverse) as kerry_Tc_reverse
    ,sum(kerry_Td_reverse) as kerry_Td_reverse
    ,sum(kerry_Te_reverse) as kerry_Te_reverse
    ,sum(kerry_Tf_reverse) as kerry_Tf_reverse
    ,sum(njv_reverse) as njv_reverse
    ,sum(njv_Ta_reverse) as njv_Ta_reverse
    ,sum(njv_Tb_reverse) as njv_Tb_reverse
    ,sum(njv_Tc_reverse) as njv_Tc_reverse
    ,sum(njv_Td_reverse) as njv_Td_reverse 
    ,sum(njv_Te_reverse) as njv_Te_reverse
    ,sum(njv_Tf_reverse) as njv_Tf_reverse
    
    --- Cost 
    ,sum(flash_cost+njv_cost+kerry_cost) as total_cost 
    ,SUM(flash_cost) as flash_cost
    ,SUM(flash_ta_cost) as flash_ta_cost
    ,SUM(flash_tb_cost) as flash_tb_cost
    ,SUM(flash_tc_cost) as flash_tc_cost
    ,SUM(flash_td_cost) as flash_td_cost
    ,SUM(flash_te_cost) as flash_te_cost
    ,SUM(flash_tf_cost) as flash_tf_cost
    ,SUM(kerry_cost) as kerry_cost
    ,SUM(kerry_ta_cost) as kerry_ta_cost
    ,SUM(kerry_tb_cost) as kerry_tb_cost 
    ,SUM(kerry_tc_cost) as kerry_tc_cost
    ,SUM(kerry_td_cost) as kerry_td_cost
    ,SUM(kerry_te_cost) as kerry_te_cost
    ,SUM(kerry_tf_cost) as kerry_tf_cost
    ,SUM(njv_cost) as njv_cost
    ,SUM(njv_ta_cost) as njv_ta_cost
    ,SUM(njv_tb_cost) as njv_tb_cost
    ,SUM(njv_tc_cost) as njv_tc_cost
    ,SUM(njv_td_cost) as njv_td_cost
    ,SUM(njv_te_cost) as njv_te_cost
    ,SUM(njv_tf_cost) as njv_tf_cost
    --- remote cost 
    ,sum(flash_remote_fee+kerry_remote_fee+njv_remote_fee) as total_remote_fee 
    ,sum(flash_remote_fee) as flash_remote_fee
    ,sum(flash_Ta_remote_fee) as flash_Ta_remote_fee
    ,sum(flash_Tb_remote_fee) as flash_Tb_remote_fee
    ,sum(flash_Tc_remote_fee) as flash_Tc_remote_fee
    ,sum(flash_Td_remote_fee) as flash_Td_remote_fee
    ,sum(flash_Te_remote_fee) as flash_Te_remote_fee
    ,sum(flash_Tf_remote_fee) as flash_Tf_remote_fee
    ,sum(kerry_remote_fee) as kerry_remote_fee
    ,sum(kerry_Ta_remote_fee) as kerry_Ta_remote_fee
    ,sum(kerry_Tb_remote_fee) as kerry_Tb_remote_fee
    ,sum(kerry_Tc_remote_fee) as kerry_Tc_remote_fee
    ,sum(kerry_Td_remote_fee) as kerry_Td_remote_fee
    ,sum(kerry_Te_remote_fee) as kerry_Te_remote_fee
    ,sum(kerry_Tf_remote_fee) as kerry_Tf_remote_fee
    ,sum(njv_remote_fee) as njv_remote_fee
    ,sum(njv_Ta_remote_fee) as njv_Ta_remote_fee
    ,sum(njv_Tb_remote_fee) as njv_Tb_remote_fee
    ,sum(njv_Tc_remote_fee) as njv_Tc_remote_fee
    ,sum(njv_Td_remote_fee) as njv_Td_remote_fee
    ,sum(njv_Te_remote_fee) as njv_Te_remote_fee
    ,sum(njv_Tf_remote_fee) as njv_Tf_remote_fee
    --- COD fee 
    ,sum(flash_cod_fee+kerry_cod_fee+njv_cod_fee) as total_cod_fee 
    ,sum(flash_cod_fee) as flash_cod
    ,sum(flash_ta_cod_fee) as flash_ta_cod
    ,sum(flash_tb_cod_fee) as flash_tb_cod
    ,sum(flash_tc_cod_fee) as flash_tc_cod
    ,sum(flash_td_cod_fee) as flash_td_cod
    ,sum(flash_te_cod_fee) as flash_te_cod
    ,sum(flash_tf_cod_fee) as flash_tf_cod
    ,sum(kerry_cod_fee) as kerry_cod
    ,sum(kerry_Tt_cod_fee) as kerry_Tt_cod
    ,sum(kerry_tb_cod_fee) as kerry_tb_cod
    ,sum(kerry_tc_cod_fee) as kerry_tc_cod
    ,sum(kerry_td_cod_fee) as kerry_td_cod
    ,sum(kerry_te_cod_fee) as kerry_te_cod
    ,sum(kerry_tf_cod_fee) as kerry_tf_cod
    ,sum(njv_cod_fee) as njv_cod
    ,sum(njv_ta_cod_fee) as njv_ta_cod
    ,sum(njv_tb_cod_fee) as njv_tb_cod
    ,sum(njv_tc_cod_fee) as njv_tc_cod
    ,sum(njv_td_cod_fee) as njv_td_cod
    ,sum(njv_te_cod_fee) as njv_te_cod
    ,sum(njv_tf_cod_fee) as njv_tf_cod
    --- reverse fee 
    ,sum(flash_rev_fee+njv_rev_fee+kerry_rev_fee) as total_rev_fee 
    ,SUM(flash_rev_fee) as flash_rev_fee
    ,SUM(flash_ta_rev_fee) as flash_ta_rev_fee
    ,SUM(flash_tb_rev_fee) as flash_tb_rev_fee
    ,SUM(flash_tc_rev_fee) as flash_tc_rev_fee
    ,SUM(flash_td_rev_fee) as flash_td_rev_fee
    ,SUM(flash_te_rev_fee) as flash_te_rev_fee
    ,SUM(flash_tf_rev_fee) as flash_tf_rev_fee
    ,SUM(kerry_rev_fee) as kerry_rev_fee
    ,SUM(kerry_ta_rev_fee) as kerry_ta_rev_fee
    ,SUM(kerry_tb_rev_fee) as kerry_tb_rev_fee
    ,SUM(kerry_tc_rev_fee) as kerry_tc_rev_fee
    ,SUM(kerry_td_rev_fee) as kerry_td_rev_fee
    ,SUM(kerry_te_rev_fee) as kerry_te_rev_fee
    ,SUM(kerry_tf_rev_fee) as kerry_tf_rev_fee
    ,SUM(njv_rev_fee) as njv_rev_fee
    ,SUM(njv_ta_rev_fee) as njv_ta_rev_fee
    ,SUM(njv_tb_rev_fee) as njv_tb_rev_fee
    ,SUM(njv_tc_rev_fee) as njv_tc_rev_fee
    ,SUM(njv_td_rev_fee) as njv_td_rev_fee
    ,SUM(njv_te_rev_fee) as njv_te_rev_fee
    ,SUM(njv_tf_rev_fee) as njv_tf_rev_fee

     --- weight overall
    ,avg(total_avg_weight) as total_avg_weight
    ,avg(flash_weight) as flash_weight
    ,avg(flash_Ta_weight) as flash_Ta_weight
    ,avg(flash_Tb_weight) as flash_Tb_weight
    ,avg(flash_Tc_weight) as flash_Tc_weight
    ,avg(flash_Td_weight) as flash_Td_weight
    ,avg(flash_Te_weight) as flash_Te_weight
    ,avg(flash_Tf_weight) as flash_Tf_weight
    ,avg(kerry_weight) as kerry_weight
    ,avg(kerry_Ta_weight) as kerry_Ta_weight
    ,avg(kerry_Tb_weight) as kerry_Tb_weight
    ,avg(kerry_Tc_weight) as kerry_Tc_weight
    ,avg(kerry_Td_weight) as kerry_Td_weight
    ,avg(kerry_Te_weight) as kerry_Te_weight
    ,avg(kerry_Tf_weight) as kerry_Tf_weight
    ,avg(njv_weight) as njv_weight
    ,avg(njv_Ta_weight) as njv_Ta_weight
    ,avg(njv_Tb_weight) as njv_Tb_weight
    ,avg(njv_Tc_weight) as njv_Tc_weight
    ,avg(njv_Td_weight) as njv_Td_weight
    ,avg(njv_Te_weight) as njv_Te_weight
    ,avg(njv_Tf_weight) as njv_Tf_weight 
    --- remote weight
    ,avg(remote_weight) as remote_weight
    ,avg(flash_weight) as flash_weight
    ,avg(flash_Ta_remote_weight) as flash_Ta_remote_weight
    ,avg(flash_Tb_remote_weight) as flash_Tb_remote_weight
    ,avg(flash_Tc_remote_weight) as flash_Tc_remote_weight
    ,avg(flash_Td_remote_weight) as flash_Td_remote_weight
    ,avg(flash_Te_remote_weight) as flash_Te_remote_weight
    ,avg(flash_Tf_remote_weight) as flash_Tf_remote_weight
    ,avg(kerry_remote_weight) as kerry_remote_weight
    ,avg(kerry_Ta_remote_weight) as kerry_Ta_remote_weight
    ,avg(kerry_Tb_remote_weight) as kerry_Tb_remote_weight
    ,avg(kerry_Tc_remote_weight) as kerry_Tc_remote_weight
    ,avg(kerry_Td_remote_weight) as kerry_Td_remote_weight
    ,avg(kerry_Te_remote_weight) as kerry_Te_remote_weight
    ,avg(kerry_Tf_remote_weight) as kerry_Tf_remote_weight
    ,avg(njv_remote_weight) as njv_remote_weight
    ,avg(njv_Ta_remote_weight) as njv_Ta_remote_weight
    ,avg(njv_Tb_remote_weight) as njv_Tb_remote_weight
    ,avg(njv_Tc_remote_weight) as njv_Tc_remote_weight
    ,avg(njv_Td_remote_weight) as njv_Td_remote_weight
    ,avg(njv_Te_remote_weight) as njv_Te_remote_weight
    ,avg(njv_Tf_remote_weight) as njv_Tf_remote_weight
    --- COD weight 
  /*   ,avg(cod_weight) as cod_weight
    ,avg(flash_cod_weight) as flash_cod_weight
    ,avg(flash_ta_cod_weight) as flash_ta_cod_weight
    ,avg(flash_tb_cod_weight) as flash_tb_cod_weight
    ,avg(flash_tc_cod_weight) as flash_tc_cod_weight
    ,avg(flash_td_cod_weight) as flash_td_cod_weight
    ,avg(flash_te_cod_weight) as flash_te_cod_weight
    ,avg(flash_tf_cod_weight) as flash_tf_cod_weight
    ,avg(kerry_cod_weight) as kerry_cod_weight 
    ,avg(kerry_Tt_cod_weight) as kerry_Tt_cod_weight
    ,avg(kerry_tb_cod_weight) as kerry_tb_cod_weight
    ,avg(kerry_tc_cod_weight) as kerry_tc_cod_weight
    ,avg(kerry_td_cod_weight) as kerry_td_cod_weight
    ,avg(kerry_te_cod_weight) as kerry_te_cod_weight
    ,avg(kerry_tf_cod_weight) as kerry_tf_cod_weight
    ,avg(njv_cod_weight) as njv_cod_weight
    ,avg(njv_ta_cod_weight) as njv_ta_cod_weight 
    ,avg(njv_tb_cod_weight) as njv_tb_cod_weight
    ,avg(njv_tc_cod_weight) as njv_tc_cod_weight
    ,avg(njv_td_cod_weight) as njv_td_cod_weight
    ,avg(njv_te_cod_weight) as njv_te_cod_weight
    ,avg(njv_tf_cod_weight) as njv_tf_cod_weight */
    --- reverse weight
    ,avg(reverse_weight) as reverse_weight
    ,avg(flash_reverse_weight) as flash_reverse_weight
    ,avg(flash_Ta_reverse_weight) as flash_Ta_reverse_weight
    ,avg(flash_Tb_reverse_weight) as flash_Tb_reverse_weight
    ,avg(flash_Tc_reverse_weight) as flash_Tc_reverse_weight
    ,avg(flash_Td_reverse_weight) as flash_Td_reverse_weight
    ,avg(flash_Te_reverse_weight) as flash_Te_reverse_weight
    ,avg(flash_Tf_reverse_weight) as flash_Tf_reverse_weight
    ,avg(kerry_reverse_weight) as kerry_reverse_weight
    ,avg(kerry_Ta_reverse_weight) as kerry_Ta_reverse_weight
    ,avg(kerry_Tb_reverse_weight) as kerry_Tb_reverse_weight
    ,avg(kerry_Tc_reverse_weight) as kerry_Tc_reverse_weight
    ,avg(kerry_Td_reverse_weight) as kerry_Td_reverse_weight
    ,avg(kerry_Te_reverse_weight) as kerry_Te_reverse_weight
    ,avg(kerry_Tf_reverse_weight) as kerry_Tf_reverse_weight
    ,avg(njv_reverse_weight) as njv_reverse_weight
    ,avg(njv_Ta_reverse_weight) as njv_Ta_reverse_weight
    ,avg(njv_Tb_reverse_weight) as njv_Tb_reverse_weight
    ,avg(njv_Tc_reverse_weight) as njv_Tc_reverse_weight
    ,avg(njv_Td_reverse_weight) as njv_Td_reverse_weight
    ,avg(njv_Te_reverse_weight) as njv_Te_reverse_weight
    ,avg(njv_Tf_reverse_weight) as njv_Tf_reverse_weight

    --
    --,sum(kerry_gbkk) as kerry_gbkk
    --,sum(kerry_upc) as kerry_upc
from forward_4pl_aggregate
left join reverse_4pl_aggregate
on reverse_4pl_aggregate.report_date = forward_4pl_aggregate.report_date





