/*
--DROP TABLE dev_thopsbi_lof.spx_analytics_cost_4pl_cpo_v1
CREATE TABLE dev_thopsbi_lof.spx_analytics_cost_4pl_cpo_v1
(
     shipment_id varchar
    ,district_tier varchar
    ,order_type varchar
    ,delivery_station_4pl varchar
    ,report_month_4pl DATE
    ,report_day_4pl DATE
    ,is_4pl  BOOLEAN
    ,is_remote  BOOLEAN
    ,is_reverse BOOLEAN
    ,is_cod BOOLEAN
    ,rate_card_4pl DECIMAL(16,7)
    ,total_4pl_cost DECIMAL(16,7)
    ,shippping_fee_cost DECIMAL(16,7)
    ,remote_cost DECIMAL(16,7) 
    ,cod_cost DECIMAL(16,7)
    ,return_cost DECIMAL(16,7)
    ,chargeable_weight_in_kg  DECIMAL(16,7)
    ,ingestion_timestamp TIMESTAMP	
    ,partition_date	DATE	
)
     WITH
    (
      FORMAT = 'Parquet',
      PARTITIONED_BY = array['partition_date']

    );
 */
/*  delete from dev_thopsbi_lof.spx_analytics_cost_4pl_cpo_v1 
 where partition_date = date('2022-08-01')  */
insert into dev_thopsbi_lof.spx_analytics_cost_4pl_cpo_v1
with raw_4pl as
(
select 
    fleet_4pl.shipment_id 
    ,delivered_timestamp
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
    END AS weight_tier
    ,seller_province_name
    ,seller_district_name
    ,chargeable_weight_in_kg
    ,fleet_order.buyer_district_name
    ,fleet_order.buyer_province_name
    ,is_4pl_remoted_area as is_remote
    ,_4pl_delivered_timestamp as delivered_4pl 
    ,_4pl_return_timestamp as return_4pl 
    ,case 
        when _4pl_name = 'FLASH' then 'Flash Express'
        when _4pl_name = 'KERRY' then 'Kerry Express'
        when _4pl_name = 'NINJA VAN' then 'Ninja Van'
        when _4pl_name = 'CJ' then 'CJ Logistics'
        else null 
    end as delivery_station_4pl
    ,is_4pl as is_4pl 
    ,fleet_order.cod_amount
    ,district_tier.tier as district_tier
    ,case 
        when is_bulky = true  then 'BULKY'
        when is_open_service = true  then 'OSV'
        when is_marketplace = true  then 'MKP'
        when is_cross_border = true  then 'CB'
        when is_warehouse = true  then 'WH'
    end as order_type
from  thopsbi_spx.dwd_4pl_shipment_info_df_th as fleet_4pl --thopsbi_lof.dwd_thspx_4pl_shipment_info_di_th as fleet_4pl 
left join 
    (
    select 
        shipment_id
        ,cod_amount
        ,chargeable_weight_in_kg
        ,seller_province_name
        ,seller_district_name
        ,buyer_district_name
        ,buyer_province_name
        ,delivered_timestamp
        ,is_marketplace
        ,is_bulky
        ,is_open_service
        ,is_cross_border
        ,is_warehouse
    from thopsbi_spx.dwd_pub_shipment_info_df_th  --thopsbi_lof.dwd_thspx_pub_shipment_info_di_th 
    ) as fleet_order 
on fleet_4pl.shipment_id = fleet_order.shipment_id 
left join thopsbi_lof.spx_index_region_temp as district_tier 
on district_tier.district = fleet_order.buyer_district_name
and district_tier.province = fleet_order.buyer_province_name
)
,region_map as 
(
select 
    shipment_id
    ,order_type
    ,district_tier
    ,case 
        when date(return_4pl) between date('2022-08-01') and date('2022-08-31') then date('2022-08-01')
        when date(delivered_4pl) between date('2022-08-01') and date('2022-08-31') then date('2022-08-01')
    end as report_month_4pl 
    ,case 
        when date(return_4pl) between date('2022-08-01') and date('2022-08-31') then date(return_4pl)
        when date(delivered_4pl) between date('2022-08-01') and date('2022-08-31') then date(delivered_4pl)
    end as report_day_4pl 
    ,chargeable_weight_in_kg
    ,case when seller_region.sla_region = 'GBKK' then 'GBKK' else 'UPC' end as seller_region 
    ,case when buyer_region.sla_region = 'GBKK' then 'GBKK' else 'UPC' end as buyer_region 
    ,is_4pl
    ,is_remote
    ,delivery_station_4pl
    ,weight_tier
    ,return_4pl 
    ,cod_amount
    ,delivered_4pl
from raw_4pl 
left join thopsbi_lof.spx_index_region_temp  as seller_region 
on raw_4pl.seller_district_name = seller_region.district
and raw_4pl.seller_province_name = seller_region.province
left join thopsbi_lof.spx_index_region_temp  as buyer_region  
on raw_4pl.buyer_district_name = buyer_region.district
and raw_4pl.buyer_province_name = buyer_region.province
)
,rate_card_map as 
(
select  
    shipment_id
    ,district_tier
    ,order_type
    ,report_month_4pl
    ,report_day_4pl
    ,weight_tier
    ,delivery_station_4pl
    ,chargeable_weight_in_kg
    ,rate.rate_card as rate_card
    ,is_remote
    ,return_4pl
    ,is_4pl
    ,cod_amount
    ,delivered_4pl
from region_map 
LEFT JOIN thopsbi_lof.spx_card_tier_tab as rate
ON region_map.seller_region = rate.origin
AND region_map.buyer_region = rate.destination
AND region_map.weight_tier = rate.interval
AND region_map.delivery_station_4pl = rate.shipment_provider
)
,pre_table as 
(
select 
    shipment_id
    ,district_tier
    ,order_type
    ,delivery_station_4pl
    ,is_4pl
    ,report_month_4pl
    ,report_day_4pl
    ,case when (date(delivered_4pl) between date('2022-08-01') and date('2022-08-31')) and is_remote = true and is_4pl  = true then true else false end as is_remote
    ,case when return_4pl is not null and is_4pl  = true then true else false  end as is_reverse 
    ,case when (date(delivered_4pl) between date('2022-08-01') and date('2022-08-31')) and cast(cod_amount as double) > 0 and is_4pl  = true then true else false end as is_cod
    ,cast(rate_card as int) as rate_card_4pl 
    ,case 
        when (date(delivered_4pl) between date('2022-08-01') and date('2022-08-31')) and rate_card is not null then 
            case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                else cast(rate_card as int)
            end 
        else 0 
    end as shippping_fee_cost
    ,case 
        when (date(delivered_4pl) between date('2022-08-01') and date('2022-08-31')) and is_remote = true and is_4pl  = true then 50 
        else 0 
    end as remote_cost 
    ,case 
        when (date(delivered_4pl) between date('2022-08-01') and date('2022-08-31')) and cast(cod_amount as double) > 0 and is_4pl = true then 
            case 
                when delivery_station_4pl = 'Ninja Van' then 0.005*cod_amount
                else 0.01*cod_amount
            end 
        else 0 
    end as cod_cost 
    ,case 
        when (date(return_4pl) between date('2022-08-01') and date('2022-08-31')) and is_4pl  = true and return_4pl is not null then 0.5*cast(rate_card as int) 
        else 0 
    end as return_cost 
    ,chargeable_weight_in_kg
from rate_card_map
where 
    report_month_4pl is not null 
)
,cpo_4pl_table as 
(
select 
    shipment_id 
    ,district_tier
    ,order_type
    ,report_month_4pl
    ,report_day_4pl
    ,delivery_station_4pl
    ,is_4pl
    ,is_remote
    ,is_reverse
    ,is_cod
    ,rate_card_4pl
    ,shippping_fee_cost + remote_cost + cod_cost + return_cost as total_4pl_cost 
    ,shippping_fee_cost
    ,remote_cost
    ,cod_cost
    ,return_cost
    ,chargeable_weight_in_kg
from pre_table
)
SELECT  
    CAST(shipment_id as varchar)
    ,cast(district_tier as varchar )
    ,cast(order_type as varchar)
    ,CAST(delivery_station_4pl as varchar)
    ,CAST(report_month_4pl as DATE)
    ,CAST(report_day_4pl as DATE)
    ,CAST(is_4pl as BOOLEAN)
    ,CAST(is_remote as BOOLEAN)
    ,CAST(is_reverse as BOOLEAN)
    ,CAST(is_cod as BOOLEAN)
    ,CAST(rate_card_4pl AS DECIMAL(16,7))
    ,CAST(total_4pl_cost as DECIMAL(16,7))
    ,CAST(shippping_fee_cost AS DECIMAL(16,7))
    ,CAST(remote_cost AS DECIMAL(16,7)) 
    ,CAST(cod_cost AS DECIMAL(16,7))
    ,CAST(return_cost AS DECIMAL(16,7))
    ,CAST(chargeable_weight_in_kg as DECIMAL(16,7))
    ,CAST(CURRENT_TIMESTAMP + INTERVAL '-1' HOUR AS TIMESTAMP) AS ingestion_timestamp
    ,DATE('2022-08-01') AS partition_date
FROM cpo_4pl_table
