with raw_4pl as
(
select 
    fleet_4pl.shipment_id 
    ,delivered_timestamp
    ,case 
        when "4pl_name" = 'FLASH' then sc_weight_in_kg
        when chargeable_weight_in_kg is not null then chargeable_weight_in_kg
        else sc_weight_in_kg
    end as chargeable_weight_in_kg
    ,seller_province_name
    ,seller_district_name
    ,fleet_order.buyer_district_name
    ,fleet_order.buyer_province_name
    ,"4pl_remoted_area_flag" as is_remote
    ,"4pl_delivered_timestamp" as delivered_4pl 
    ,"4pl_first_retern_timestamp" as return_4pl 
    ,case 
        when "4pl_name" = 'FLASH' then 'Flash Express'
        when "4pl_name" = 'KERRY' then 'Kerry Express'
        when "4pl_name" = 'NINJA VAN' then 'Ninja Van'
        when "4pl_name" = 'CJ' then 'CJ Logistics'
        else null 
    end as delivery_station_4pl
    ,"4pl_flag" as is_4pl 
    ,fleet_order.cod_amount
    ,buyer_region_name
    ,case
        when warehouse_flag = true then 'GBKK'
        when cross_border_flag = true then 'GBKK'
        when pickup_region is not null then pickup_region 
        else dop_region 
    end as seller_region_name
from  thopsbi_lof.dwd_thspx_4pl_shipment_info_di_th as fleet_4pl 
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
and "4pl_flag" = true 
)
,region_map as 
(
select 
    shipment_id
    --,case when seller_region.sla_region = 'GBKK' then 'GBKK' else 'UPC' end as seller_region 
    --,case when buyer_region.sla_region = 'GBKK' then 'GBKK' else 'UPC' end as buyer_region 
    ,case when seller_region_name = 'GBKK' then 'GBKK' else 'UPC' end as seller_region 
    ,case when buyer_region_name = 'GBKK' then 'GBKK' else 'UPC' end as buyer_region 
    ,is_4pl
    ,is_remote
    ,delivery_station_4pl
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
        WHEN chargeable_weight_in_kg > 20 THEN '21'
    END AS weight_tier
,cod_amount
,return_4pl
,chargeable_weight_in_kg
,date(delivered_4pl) as delivered_date 
,date(return_4pl) as return_date 
from raw_4pl 
)
,rate_card_map as 
(
select  
    shipment_id
    ,delivery_station_4pl
    ,rate.rate_card as rate_card
    ,is_4pl
    ,is_remote
    ,cod_amount
    ,return_4pl
    ,weight_tier
    ,buyer_region
    ,seller_region
    ,delivered_date
    ,return_date
    ,chargeable_weight_in_kg
from region_map 
LEFT JOIN thopsbi_lof.spx_card_tier_tab as rate
ON region_map.seller_region = rate.origin
AND region_map.buyer_region = rate.destination
AND region_map.weight_tier = rate.interval
AND region_map.delivery_station_4pl = rate.shipment_provider
)
select 
    shipment_id
    ,delivery_station_4pl
    ,case 
        when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + (CAST(chargeable_weight_in_kg AS DOUBLE) - 20)
        when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int)
        else 0 
    end as total_kerry_shipping_fee  
from rate_card_map
where 
    delivered_date = date('2022-04-01') 
    and delivery_station_4pl = 'Kerry Express'
