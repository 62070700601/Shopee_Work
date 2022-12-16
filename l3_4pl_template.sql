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
from thopsbi_spx.dwd_thspx_4pl_shipment_info_di_th as fleet_4pl 
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
    ,case 
        when seller_region_name = 'GBKK' then 'GBKK' else 'UPC' 
        end as seller_region 
    ,case 
        when buyer_region_name = 'GBKK' then 'GBKK' else 'UPC' 
        end as buyer_region 
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
    ,buyer_district_name
    ,buyer_province_name
from raw_4pl 

/* left join thopsbi_lof.spx_index_region_temp  as seller_region 
on raw_4pl.seller_district_name = seller_region.district
and raw_4pl.seller_province_name = seller_region.province

left join thopsbi_lof.spx_index_region_temp  as buyer_region  
on raw_4pl.buyer_district_name = buyer_region.district
and raw_4pl.buyer_province_name = buyer_region.province */

/* where date(delivered_4pl) between current_date - interval '30' day and current_date - interval '1' day  or 
date(return_4pl) between current_date - interval '30' day and current_date - interval '1' day */
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
    ,proin.tier 
from region_map 
LEFT JOIN thopsbi_lof.spx_card_tier_tab as rate
ON region_map.seller_region = rate.origin
AND region_map.buyer_region = rate.destination
AND region_map.weight_tier = rate.interval
AND region_map.delivery_station_4pl = rate.shipment_provider
LEFT JOIN thopsbi_lof.spx_province_index_tab proin
ON region_map.buyer_district_name = proin.district
and region_map.buyer_province_name = proin.province 
)
,pre_reverse_agg as 
(
select 
    case 
        when return_date between date('2021-11-01') and date('2021-11-30') then '2021-11'
        when return_date between date('2021-12-01') and date('2021-12-31') then '2021-12'
        when return_date between date('2022-01-01') and date('2022-01-31') then '2022-01'
        when return_date between date('2022-02-01') and date('2022-02-28') then '2022-02'
        when return_date between date('2022-03-01') and date('2022-03-31') then '2022-03'  
        when return_date between date('2022-04-01') and date('2022-04-30') then '2022-04'
    end as report_month
    ,count(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null  then shipment_id else null end) as total_flash_reverse_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier A' then shipment_id else null end) as total_flash_reverse_tier_a_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier B' then shipment_id else null end) as total_flash_reverse_tier_b_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier C' then shipment_id else null end) as total_flash_reverse_tier_c_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier D' then shipment_id else null end) as total_flash_reverse_tier_d_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier E' then shipment_id else null end) as total_flash_reverse_tier_e_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier F' then shipment_id else null end) as total_flash_reverse_tier_f_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and return_4pl is not null then shipment_id else null end) as total_kerry_reverse_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier A'   then shipment_id else null end) as total_kerry_reverse_tier_a_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier B'   then shipment_id else null end) as total_kerry_reverse_tier_b_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier C'   then shipment_id else null end) as total_kerry_reverse_tier_c_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier D'   then shipment_id else null end) as total_kerry_reverse_tier_d_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier E'   then shipment_id else null end) as total_kerry_reverse_tier_e_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier F'   then shipment_id else null end) as total_kerry_reverse_tier_f_volume
    ,count(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null then shipment_id else null end) as total_njv_reverse_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier A' then shipment_id else null end) as total_njv_reverse_tier_a_volume
    ,count(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier B' then shipment_id else null end) as total_njv_reverse_tier_b_volume
    ,count(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier C' then shipment_id else null end) as total_njv_reverse_tier_c_volume
    ,count(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier D' then shipment_id else null end) as total_njv_reverse_tier_d_volume
    ,count(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier E' then shipment_id else null end) as total_njv_reverse_tier_e_volume
    ,count(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier F' then shipment_id else null end) as total_njv_reverse_tier_f_volume
    ,count(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null  then shipment_id else null end) as total_cj_reverse_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null  and tier = 'Tier A' then shipment_id else null end) as total_cj_reverse_tier_a_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null  and tier = 'Tier B' then shipment_id else null end) as total_cj_reverse_tier_b_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null  and tier = 'Tier C' then shipment_id else null end) as total_cj_reverse_tier_c_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null  and tier = 'Tier D' then shipment_id else null end) as total_cj_reverse_tier_d_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null  and tier = 'Tier E' then shipment_id else null end) as total_cj_reverse_tier_e_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null  and tier = 'Tier F' then shipment_id else null end) as total_cj_reverse_tier_f_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null  then shipment_id else null end) as total_jnt_reverse_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null  and tier = 'Tier A' then shipment_id else null end) as total_jnt_reverse_tier_a_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null  and tier = 'Tier B' then shipment_id else null end) as total_cj_reverse_tier_b_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null  and tier = 'Tier C' then shipment_id else null end) as total_cj_reverse_tier_c_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null  and tier = 'Tier D' then shipment_id else null end) as total_cj_reverse_tier_d_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null  and tier = 'Tier E' then shipment_id else null end) as total_cj_reverse_tier_e_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null  and tier = 'Tier F' then shipment_id else null end) as total_cj_reverse_tier_f_volume 
    ,0.5*sum(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null then cast(rate_card as int) else 0 end) as total_flash_reverse_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_flash_reverse_tier_a_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_flash_reverse_tier_b_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_flash_reverse_tier_c_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_flash_reverse_tier_d_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_flash_reverse_tier_e_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Flash Express' and return_4pl is not null and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_flash_reverse_tier_f_fee  
    ,0.5*sum(case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null then cast(rate_card as int)
                else 0 
            end) as total_kerry_reverse_fee  
    ,0.5*sum(case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier A' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier A' then cast(rate_card as int)
                else 0 
            end) as total_kerry_reverse_tier_a_fee  
    ,0.5*sum(case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier B' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier B' then cast(rate_card as int)
                else 0 
            end) as total_kerry_reverse_tier_b_fee  
    ,0.5*sum(case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier C' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier C' then cast(rate_card as int)
                else 0 
            end) as total_kerry_reverse_tier_c_fee 
    ,0.5*sum(case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier D' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier D' then cast(rate_card as int)
                else 0 
            end) as total_kerry_reverse_tier_d_fee 
    ,0.5*sum(case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier E' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier E' then cast(rate_card as int)
                else 0 
            end) as total_kerry_reverse_tier_e_fee 
    ,0.5*sum(case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier F' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and return_4pl is not null and tier = 'Tier F' then cast(rate_card as int)
                else 0 
            end) as total_kerry_reverse_tier_f_fee 
    ,0.5*sum(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null then cast(rate_card as int) else 0 end) as total_njv_reverse_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_njv_reverse_tier_a_fee
    ,0.5*sum(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_njv_reverse_tier_b_fee
    ,0.5*sum(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_njv_reverse_tier_c_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_njv_reverse_tier_d_fee  
    ,0.5*sum(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_njv_reverse_tier_e_fee 
    ,0.5*sum(case when delivery_station_4pl = 'Ninja Van' and return_4pl is not null and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_njv_reverse_tier_f_fee    
    ,0.5*sum(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null then cast(rate_card as int) else 0 end) as total_cj_reverse_fee  
    ,0.5*sum(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_cj_reverse_tier_a_fee  
    ,0.5*sum(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_cj_reverse_tier_b_fee 
    ,0.5*sum(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_cj_reverse_tier_c_fee 
    ,0.5*sum(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_cj_reverse_tier_d_fee 
    ,0.5*sum(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_cj_reverse_tier_e_fee 
    ,0.5*sum(case when delivery_station_4pl = 'CJ Logistics' and return_4pl is not null and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_cj_reverse_tier_f_fee 
    ,0.5*sum(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null then cast(rate_card as int) else 0 end) as total_jnt_reverse_fee  
    ,0.5*sum(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_jnt_reverse_tier_a_fee  
    ,0.5*sum(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_jnt_reverse_tier_b_fee 
    ,0.5*sum(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_jnt_reverse_tier_c_fee 
    ,0.5*sum(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_jnt_reverse_tier_d_fee 
    ,0.5*sum(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_jnt_reverse_tier_e_fee 
    ,0.5*sum(case when delivery_station_4pl = 'J&T Express' and return_4pl is not null and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_jnt_reverse_tier_f_fee 
from rate_card_map
group by 1 
)
,pre_delivered_agg as (
select 
    --delivered_date
    case 
        when delivered_date between date('2021-11-01') and date('2021-11-30') then '2021-11'
        when delivered_date between date('2021-12-01') and date('2021-12-31') then '2021-12'
        when delivered_date between date('2022-01-01') and date('2022-01-31') then '2022-01'
        when delivered_date between date('2022-02-01') and date('2022-02-28') then '2022-02'
        when delivered_date between date('2022-03-01') and date('2022-03-31') then '2022-03'  
        when delivered_date between date('2022-04-01') and date('2022-04-30') then '2022-04'
    end as report_month
    ,count(case when delivery_station_4pl is not null then shipment_id else null end) as total_4pl_volume
    ,count(case when delivery_station_4pl = 'Flash Express' then shipment_id else null end) as total_flash_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier A' then shipment_id else null end) as total_flash_tier_a_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier B' then shipment_id else null end) as total_flash_tier_b_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier C' then shipment_id else null end) as total_flash_tier_c_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier D' then shipment_id else null end) as total_flash_tier_d_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier E' then shipment_id else null end) as total_flash_tier_e_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier F' then shipment_id else null end) as total_flash_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' then shipment_id else null end) as total_kerry_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier A' then shipment_id else null end) as total_kerry_tier_a_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier B' then shipment_id else null end) as total_kerry_tier_b_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier C' then shipment_id else null end) as total_kerry_tier_c_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier D' then shipment_id else null end) as total_kerry_tier_d_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier E' then shipment_id else null end) as total_kerry_tier_e_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier F' then shipment_id else null end) as total_kerry_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' then shipment_id else null end) as total_njv_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier A' then shipment_id else null end) as total_njv_tier_a_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier B' then shipment_id else null end) as total_njv_tier_b_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier C' then shipment_id else null end) as total_njv_tier_c_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier D' then shipment_id else null end) as total_njv_tier_d_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier E' then shipment_id else null end) as total_njv_tier_e_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier F' then shipment_id else null end) as total_njv_tier_f_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as total_cj_volume
    ,count(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier A' then shipment_id else null end) as total_cj_tier_a_volume
    ,count(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier B' then shipment_id else null end) as total_cj_tier_b_volume
    ,count(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier C' then shipment_id else null end) as total_cj_tier_c_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier D' then shipment_id else null end) as total_cj_tier_d_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier E' then shipment_id else null end) as total_cj_tier_e_volume   
    ,count(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier F' then shipment_id else null end) as total_cj_tier_f_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as total_jnt_volume
    ,count(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier A' then shipment_id else null end) as total_jnt_tier_a_volume
    ,count(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier B' then shipment_id else null end) as total_jnt_tier_b_volume
    ,count(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier C' then shipment_id else null end) as total_jnt_tier_c_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier D' then shipment_id else null end) as total_jnt_tier_d_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier E' then shipment_id else null end) as total_jnt_tier_e_volume   
    ,count(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier F' then shipment_id else null end) as total_jnt_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and cod_amount > 0 then shipment_id else null end) as total_flash_cod_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and cod_amount > 0 and tier = 'Tier A' then shipment_id else null end) as total_flash_cod_tier_a_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and cod_amount > 0 and tier = 'Tier B' then shipment_id else null end) as total_flash_cod_tier_b_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and cod_amount > 0 and tier = 'Tier C' then shipment_id else null end) as total_flash_cod_tier_c_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and cod_amount > 0 and tier = 'Tier D' then shipment_id else null end) as total_flash_cod_tier_d_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and cod_amount > 0 and tier = 'Tier E' then shipment_id else null end) as total_flash_cod_tier_e_volume
    ,count(case when delivery_station_4pl = 'Flash Express' and cod_amount > 0 and tier = 'Tier F' then shipment_id else null end) as total_flash_cod_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and cod_amount > 0  then shipment_id else null end) as total_kerry_cod_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and cod_amount > 0 and tier = 'Tier A' then shipment_id else null end) as total_kerry_cod_tier_a_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and cod_amount > 0 and tier = 'Tier B' then shipment_id else null end) as total_kerry_cod_tier_b_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and cod_amount > 0 and tier = 'Tier C' then shipment_id else null end) as total_kerry_cod_tier_c_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and cod_amount > 0 and tier = 'Tier D' then shipment_id else null end) as total_kerry_cod_tier_d_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and cod_amount > 0 and tier = 'Tier E' then shipment_id else null end) as total_kerry_cod_tier_e_volume
    ,count(case when delivery_station_4pl = 'Kerry Express' and cod_amount > 0 and tier = 'Tier F' then shipment_id else null end) as total_kerry_cod_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and cod_amount > 0  then shipment_id else null end) as total_njv_cod_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and cod_amount > 0 and tier = 'Tier A' then shipment_id else null end) as total_njv_cod_tier_a_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and cod_amount > 0 and tier = 'Tier B' then shipment_id else null end) as total_njv_cod_tier_b_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and cod_amount > 0 and tier = 'Tier C' then shipment_id else null end) as total_njv_cod_tier_c_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and cod_amount > 0 and tier = 'Tier D' then shipment_id else null end) as total_njv_cod_tier_d_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and cod_amount > 0 and tier = 'Tier E' then shipment_id else null end) as total_njv_cod_tier_e_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and cod_amount > 0 and tier = 'Tier F' then shipment_id else null end) as total_njv_cod_tier_f_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and cod_amount > 0  then shipment_id else null end) as total_cj_cod_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and cod_amount > 0  and tier = 'Tier A' then shipment_id else null end) as total_cj_cod_tier_a_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and cod_amount > 0  and tier = 'Tier B' then shipment_id else null end) as total_cj_cod_tier_b_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and cod_amount > 0  and tier = 'Tier C' then shipment_id else null end) as total_cj_cod_tier_c_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and cod_amount > 0  and tier = 'Tier D' then shipment_id else null end) as total_cj_cod_tier_d_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and cod_amount > 0  and tier = 'Tier E' then shipment_id else null end) as total_cj_cod_tier_e_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and cod_amount > 0  and tier = 'Tier F' then shipment_id else null end) as total_cj_cod_tier_f_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and cod_amount > 0  then shipment_id else null end) as total_jnt_cod_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and cod_amount > 0  and tier = 'Tier A' then shipment_id else null end) as total_jnt_cod_tier_a_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and cod_amount > 0  and tier = 'Tier B' then shipment_id else null end) as total_jnt_cod_tier_b_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and cod_amount > 0  and tier = 'Tier C' then shipment_id else null end) as total_jnt_cod_tier_c_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and cod_amount > 0  and tier = 'Tier D' then shipment_id else null end) as total_jnt_cod_tier_d_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and cod_amount > 0  and tier = 'Tier E' then shipment_id else null end) as total_jnt_cod_tier_e_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and cod_amount > 0  and tier = 'Tier F' then shipment_id else null end) as total_jnt_cod_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and is_remote = true then shipment_id else null end) as total_flash_remote_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_flash_remote_tier_a_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_flash_remote_tier_b_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_flash_remote_tier_c_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_flash_remote_tier_d_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_flash_remote_tier_e_volume 
    ,count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_flash_remote_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true then shipment_id else null end) as total_kerry_remote_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_kerry_remote_tier_a_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_kerry_remote_tier_b_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_kerry_remote_tier_c_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_kerry_remote_tier_d_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_kerry_remote_tier_e_volume 
    ,count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_kerry_remote_tier_f_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true  then shipment_id else null end) as total_njv_remote_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_njv_remote_tier_a_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_njv_remote_tier_b_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_njv_remote_tier_c_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_njv_remote_tier_d_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_njv_remote_tier_e_volume 
    ,count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_njv_remote_tier_f_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true then shipment_id else null end) as total_cj_remote_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_cj_remote_tier_a_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_cj_remote_tier_b_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_cj_remote_tier_c_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_cj_remote_tier_d_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_cj_remote_tier_e_volume 
    ,count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_cj_remote_tier_f_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and is_remote = true then shipment_id else null end) as total_jnt_remote_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_jnt_remote_tier_a_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_jnt_remote_tier_b_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_jnt_remote_tier_c_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_jnt_remote_tier_d_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_jnt_remote_tier_e_volume 
    ,count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_jnt_remote_tier_f_volume 
    ,sum(case when delivery_station_4pl = 'Flash Express' then cast(rate_card as int) else 0 end) as total_flash_shipping_fee  
    ,sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_flash_tier_a_shipping_fee
    ,sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_flash_tier_b_shipping_fee
    ,sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_flash_tier_c_shipping_fee 
    ,sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_flash_tier_d_shipping_fee
    ,sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_flash_tier_e_shipping_fee 
    ,sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_flash_tier_f_shipping_fee 
    ,sum(case 
            when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
            when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int)
            else 0 
        end) as total_kerry_shipping_fee  
    ,sum(case 
            when weight_tier = '21' and delivery_station_4pl = 'Kerry Express'  and tier = 'Tier A' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
            when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and tier = 'Tier A' then cast(rate_card as int)
            else 0 
        end) as total_kerry_tier_a_shipping_fee  
    ,sum(case 
            when weight_tier = '21' and delivery_station_4pl = 'Kerry Express'  and tier = 'Tier B' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
            when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and tier = 'Tier B' then cast(rate_card as int)
            else 0 
        end) as total_kerry_tier_b_shipping_fee
    ,sum(case 
            when weight_tier = '21' and delivery_station_4pl = 'Kerry Express'  and tier = 'Tier C' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
            when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and tier = 'Tier C' then cast(rate_card as int)
            else 0 
        end) as total_kerry_tier_c_shipping_fee
    ,sum(case 
            when weight_tier = '21' and delivery_station_4pl = 'Kerry Express'  and tier = 'Tier D' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
            when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and tier = 'Tier D' then cast(rate_card as int)
            else 0 
        end) as total_kerry_tier_d_shipping_fee
    ,sum(case 
            when weight_tier = '21' and delivery_station_4pl = 'Kerry Express'  and tier = 'Tier E' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
            when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and tier = 'Tier E' then cast(rate_card as int)
            else 0 
        end) as total_kerry_tier_e_shipping_fee    
    ,sum(case 
            when weight_tier = '21' and delivery_station_4pl = 'Kerry Express'  and tier = 'Tier F' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
            when weight_tier != '21' and delivery_station_4pl = 'Kerry Express' and tier = 'Tier F' then cast(rate_card as int)
            else 0 
        end) as total_kerry_tier_f_shipping_fee    
    ,sum(case when delivery_station_4pl = 'Ninja Van' then cast(rate_card as int) else 0 end) as total_njv_shipping_fee  
    ,sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_njv_tier_a_shipping_fee
    ,sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_njv_tier_b_shipping_fee
    ,sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_njv_tier_c_shipping_fee
    ,sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_njv_tier_d_shipping_fee
    ,sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_njv_tier_e_shipping_fee
    ,sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_njv_tier_f_shipping_fee
    ,sum(case when delivery_station_4pl = 'CJ Logistics' then cast(rate_card as int) else 0 end) as total_cj_shipping_fee  
    ,sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_cj_tier_a_shipping_fee 
    ,sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_cj_tier_b_shipping_fee 
    ,sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_cj_tier_c_shipping_fee 
    ,sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_cj_tier_d_shipping_fee 
    ,sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_cj_tier_e_shipping_fee 
    ,sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_cj_tier_f_shipping_fee 
    ,sum(case when delivery_station_4pl = 'J&T Express' then cast(rate_card as int) else 0 end) as total_jnt_shipping_fee  
    ,sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier A' then cast(rate_card as int) else 0 end) as total_jnt_tier_a_shipping_fee 
    ,sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier B' then cast(rate_card as int) else 0 end) as total_jnt_tier_b_shipping_fee 
    ,sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier C' then cast(rate_card as int) else 0 end) as total_jnt_tier_c_shipping_fee 
    ,sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier D' then cast(rate_card as int) else 0 end) as total_jnt_tier_d_shipping_fee 
    ,sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier E' then cast(rate_card as int) else 0 end) as total_jnt_tier_e_shipping_fee 
    ,sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier F' then cast(rate_card as int) else 0 end) as total_jnt_tier_f_shipping_fee 
    ,0.01*sum(case when delivery_station_4pl = 'Flash Express' then cod_amount  else 0 end) as total_flash_cod_fee
    ,0.01*sum(case when delivery_station_4pl = 'Flash Express'  and tier = 'Tier A' then cod_amount  else 0 end) as total_flash_tier_a_cod_fee
    ,0.01*sum(case when delivery_station_4pl = 'Flash Express'  and tier = 'Tier B' then cod_amount  else 0 end) as total_flash_tier_b_cod_fee   
    ,0.01*sum(case when delivery_station_4pl = 'Flash Express'  and tier = 'Tier C' then cod_amount  else 0 end) as total_flash_tier_c_cod_fee   
    ,0.01*sum(case when delivery_station_4pl = 'Flash Express'  and tier = 'Tier D' then cod_amount  else 0 end) as total_flash_tier_d_cod_fee   
    ,0.01*sum(case when delivery_station_4pl = 'Flash Express'  and tier = 'Tier E' then cod_amount  else 0 end) as total_flash_tier_e_cod_fee   
    ,0.01*sum(case when delivery_station_4pl = 'Flash Express'  and tier = 'Tier F' then cod_amount  else 0 end) as total_flash_tier_f_cod_fee       
    ,0.01*sum(case when delivery_station_4pl = 'Kerry Express' then cod_amount else 0 end) as total_kerry_cod_fee  
    ,0.01*sum(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier A' then cod_amount else 0 end) as total_kerry_tier_a_cod_fee
    ,0.01*sum(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier B' then cod_amount else 0 end) as total_kerry_tier_b_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier C' then cod_amount else 0 end) as total_kerry_tier_c_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier D' then cod_amount else 0 end) as total_kerry_tier_d_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier E' then cod_amount else 0 end) as total_kerry_tier_e_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier F' then cod_amount else 0 end) as total_kerry_tier_f_cod_fee  
    ,0.005*sum(case when delivery_station_4pl = 'Ninja Van' then cod_amount else 0 end) as total_njv_cod_fee
    ,0.005*sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier A' then cod_amount else 0 end) as total_njv_tier_a_cod_fee
    ,0.005*sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier B' then cod_amount else 0 end) as total_njv_tier_b_cod_fee
    ,0.005*sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier C' then cod_amount else 0 end) as total_njv_tier_c_cod_fee
    ,0.005*sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier D' then cod_amount else 0 end) as total_njv_tier_d_cod_fee
    ,0.005*sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier E' then cod_amount else 0 end) as total_njv_tier_e_cod_fee
    ,0.005*sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier F' then cod_amount else 0 end) as total_njv_tier_f_cod_fee
    ,0.01*sum(case when delivery_station_4pl = 'CJ Logistics' then cod_amount else 0 end) as total_cj_cod_fee  
    ,0.01*sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier A' then cod_amount else 0 end) as total_cj_tier_a_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier B' then cod_amount else 0 end) as total_cj_tier_b_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier C' then cod_amount else 0 end) as total_cj_tier_c_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier D' then cod_amount else 0 end) as total_cj_tier_d_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier E' then cod_amount else 0 end) as total_cj_tier_e_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'CJ Logistics'  and tier = 'Tier F' then cod_amount else 0 end) as total_cj_tier_f_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'J&T Express' then cod_amount else 0 end) as total_cj_cod_fee  
    ,0.01*sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier A' then cod_amount else 0 end) as total_jnt_tier_a_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier B' then cod_amount else 0 end) as total_jnt_tier_b_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier C' then cod_amount else 0 end) as total_jnt_tier_c_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier D' then cod_amount else 0 end) as total_jnt_tier_d_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier E' then cod_amount else 0 end) as total_jnt_tier_e_cod_fee 
    ,0.01*sum(case when delivery_station_4pl = 'J&T Express'  and tier = 'Tier F' then cod_amount else 0 end) as total_jnt_tier_f_cod_fee 
    ,50*count(case when delivery_station_4pl = 'Flash Express' and is_remote = true  then shipment_id else null end) as total_flash_remote_cost
    ,50*count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_flash_tier_a_remote_cost
    ,50*count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_flash_tier_b_remote_cost
    ,50*count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_flash_tier_c_remote_cost
    ,50*count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_flash_tier_d_remote_cost
    ,50*count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_flash_tier_e_remote_cost
    ,50*count(case when delivery_station_4pl = 'Flash Express' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_flash_tier_f_remote_cost
    ,50*count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true then shipment_id else null end) as total_kerry_remote_cost
    ,50*count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_kerry_tier_a_remote_cost
    ,50*count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_kerry_tier_b_remote_cost
    ,50*count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_kerry_tier_c_remote_cost
    ,50*count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_kerry_tier_d_remote_cost
    ,50*count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_kerry_tier_e_remote_cost
    ,50*count(case when delivery_station_4pl = 'Kerry Express' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_kerry_tier_f_remote_cost
    ,50*count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true  then shipment_id else null end) as total_njv_remote_cost
    ,50*count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier A'  then shipment_id else null end) as total_njv_tier_a_remote_cost
    ,50*count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier B'  then shipment_id else null end) as total_njv_tier_b_remote_cost
    ,50*count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier C'  then shipment_id else null end) as total_njv_tier_c_remote_cost
    ,50*count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier D'  then shipment_id else null end) as total_njv_tier_d_remote_cost
    ,50*count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier E'  then shipment_id else null end) as total_njv_tier_e_remote_cost
    ,50*count(case when delivery_station_4pl = 'Ninja Van' and is_remote = true and tier = 'Tier F'  then shipment_id else null end) as total_njv_tier_f_remote_cost
    ,50*count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true then shipment_id else null end) as total_cj_remote_cost 
    ,50*count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_cj_tier_a_remote_cost 
    ,50*count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_cj_tier_b_remote_cost 
    ,50*count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_cj_tier_c_remote_cost 
    ,50*count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_cj_tier_d_remote_cost 
    ,50*count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_cj_tier_e_remote_cost 
    ,50*count(case when delivery_station_4pl = 'CJ Logistics' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_cj_tier_f_remote_cost 
    ,50*count(case when delivery_station_4pl = 'J&T Express' and is_remote = true then shipment_id else null end) as total_cj_remote_cost 
    ,50*count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier A' then shipment_id else null end) as total_jnt_tier_a_remote_cost 
    ,50*count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier B' then shipment_id else null end) as total_jnt_tier_b_remote_cost 
    ,50*count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier C' then shipment_id else null end) as total_jnt_tier_c_remote_cost 
    ,50*count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier D' then shipment_id else null end) as total_jnt_tier_d_remote_cost 
    ,50*count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier E' then shipment_id else null end) as total_jnt_tier_e_remote_cost 
    ,50*count(case when delivery_station_4pl = 'J&T Express' and is_remote = true and tier = 'Tier F' then shipment_id else null end) as total_jnt_tier_f_remote_cost 


    ,cast(sum(case when delivery_station_4pl = 'Flash Express' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Flash Express' then shipment_id else null end) as flash_shipping_fee_cpo 
    ,cast(sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier A' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier A' then shipment_id else null end) as flash_shipping_fee_cpo_tier_a
    ,cast(sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier B' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier B' then shipment_id else null end) as flash_shipping_fee_cpo_tier_b
    ,cast(sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier C' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier C' then shipment_id else null end) as flash_shipping_fee_cpo_tier_c
    ,cast(sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier D' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier D' then shipment_id else null end) as flash_shipping_fee_cpo_tier_d
    ,cast(sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier E' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier E' then shipment_id else null end) as flash_shipping_fee_cpo_tier_e
    ,cast(sum(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier F' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Flash Express' and tier = 'Tier F' then shipment_id else null end) as flash_shipping_fee_cpo_tier_f 



    ,cast
        (
        sum(
            case 
                when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21'  and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) 
                else 0 
            end
            ) as double
        )/count(case when delivery_station_4pl = 'Kerry Express' then shipment_id else null end) as kerry_shipping_fee_cpo
    ,cast
        (
        sum(
            case 
                when weight_tier = '21' and tier = 'Tier A' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and tier = 'Tier A' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) 
                else 0 
            end
            ) as double
        )/count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier A' then shipment_id else null end) as kerry_shipping_fee_cpo_tier_a
    ,cast
        (
        sum(
            case 
                when weight_tier = '21' and tier = 'Tier B' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and tier = 'Tier B' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) 
                else 0 
            end
            ) as double
        )/count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier B' then shipment_id else null end) as kerry_shipping_fee_cpo_tier_b
    ,cast
        (
        sum(
            case 
                when weight_tier = '21' and tier = 'Tier C' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and tier = 'Tier C' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) 
                else 0 
            end
            ) as double
        )/count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier C' then shipment_id else null end) as kerry_shipping_fee_cpo_tier_c
    ,cast
        (
        sum(
            case 
                when weight_tier = '21' and tier = 'Tier D' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and tier = 'Tier D' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) 
                else 0 
            end
            ) as double
        )/count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier D' then shipment_id else null end) as kerry_shipping_fee_cpo_tier_d        
    ,cast
        (
        sum(
            case 
                when weight_tier = '21' and tier = 'Tier E' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and tier = 'Tier E' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) 
                else 0 
            end
            ) as double
        )/count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier E' then shipment_id else null end) as kerry_shipping_fee_cpo_tier_e
    ,cast
        (
        sum(
            case 
                when weight_tier = '21' and tier = 'Tier F' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) + cast(((CAST(chargeable_weight_in_kg AS DOUBLE) - 20)*30) as int)
                when weight_tier != '21' and tier = 'Tier F' and delivery_station_4pl = 'Kerry Express' then cast(rate_card as int) 
                else 0 
            end
            ) as double
        )/count(case when delivery_station_4pl = 'Kerry Express' and tier = 'Tier F' then shipment_id else null end) as kerry_shipping_fee_cpo_tier_f


    ,cast(sum(case when delivery_station_4pl = 'Ninja Van' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Ninja Van' then shipment_id else null end) as njv_shipping_fee_cpo  
    ,cast(sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier A' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier A' then shipment_id else null end) as njv_shipping_fee_cpo_tier_a
    ,cast(sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier B' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier B' then shipment_id else null end) as njv_shipping_fee_cpo_tier_b
    ,cast(sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier C' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier C' then shipment_id else null end) as njv_shipping_fee_cpo_tier_c
    ,cast(sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier D' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier D' then shipment_id else null end) as njv_shipping_fee_cpo_tier_d
    ,cast(sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier E' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier E' then shipment_id else null end) as njv_shipping_fee_cpo_tier_e
    ,cast(sum(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier F' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'Ninja Van' and tier = 'Tier F' then shipment_id else null end) as njv_shipping_fee_cpo_tier_f



    ,cast(sum(case when delivery_station_4pl = 'CJ Logistics' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as cj_shipping_fee_cpo  
    ,cast(sum(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier A' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as cj_shipping_fee_cpo_tier_a
    ,cast(sum(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier B' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as cj_shipping_fee_cpo_tier_b
    ,cast(sum(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier C' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as cj_shipping_fee_cpo_tier_c
    ,cast(sum(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier D' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as cj_shipping_fee_cpo_tier_d
    ,cast(sum(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier E' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as cj_shipping_fee_cpo_tier_e
    ,cast(sum(case when delivery_station_4pl = 'CJ Logistics' and tier = 'Tier F' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'CJ Logistics' then shipment_id else null end) as cj_shipping_fee_cpo_tier_f

    ,cast(sum(case when delivery_station_4pl = 'J&T Express'then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as jnt_shipping_fee_cpo  
    ,cast(sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier A' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as jnt_shipping_fee_cpo_tier_a
    ,cast(sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier B' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as jnt_shipping_fee_cpo_tier_b
    ,cast(sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier C' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as jnt_shipping_fee_cpo_tier_c
    ,cast(sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier D' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as jnt_shipping_fee_cpo_tier_d
    ,cast(sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier E' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as jnt_shipping_fee_cpo_tier_e
    ,cast(sum(case when delivery_station_4pl = 'J&T Express' and tier = 'Tier F' then cast(rate_card as int) else 0 end) as double)/count(case when delivery_station_4pl = 'J&T Express' then shipment_id else null end) as jnt_shipping_fee_cpo_tier_f

    ---- 
    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'Flash Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as flash_GBKK_21

    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' or buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'Flash Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as flash_UPC_21


    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as kerry_GBKK_21

    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' or buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'Kerry Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as kerry_UPC_21


    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'Ninja Van' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as njv_GBKK_21

    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' or buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'Ninja Van' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as njv_UPC_21    

    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'CJ Logistics'and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'CJ Logistics'and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'CJ Logistics'and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'CJ Logistics' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as cj_GBKK_21

    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' or buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'CJ Logistics'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'CJ Logistics'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'CJ Logistics'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'CJ Logistics'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'CJ Logistics' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as cj_UPC_21   


    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'J&T Express' and seller_region = 'GBKK' and buyer_region = 'GBKK' then shipment_id end ) as jnt_GBKK_21

    ,count(case when weight_tier = '0.5' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' or buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_05
    ,count(case when weight_tier = '1' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_1
    ,count(case when weight_tier = '2' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_2
    ,count(case when weight_tier = '3' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_3
    ,count(case when weight_tier = '4' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_4
    ,count(case when weight_tier = '5' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_5
    ,count(case when weight_tier = '6' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_6
    ,count(case when weight_tier = '7' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_7
    ,count(case when weight_tier = '8' and delivery_station_4pl = 'J&T Express'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_8
    ,count(case when weight_tier = '9' and delivery_station_4pl = 'J&T Express'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_9
    ,count(case when weight_tier = '10' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_10
    ,count(case when weight_tier = '11' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_11
    ,count(case when weight_tier = '12' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_12
    ,count(case when weight_tier = '13' and delivery_station_4pl = 'J&T Express'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_13
    ,count(case when weight_tier = '14' and delivery_station_4pl = 'J&T Express'and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_14
    ,count(case when weight_tier = '15' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_15
    ,count(case when weight_tier = '16' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_16
    ,count(case when weight_tier = '17' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_17
    ,count(case when weight_tier = '18' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_18
    ,count(case when weight_tier = '19' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_19
    ,count(case when weight_tier = '20' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_20
    ,count(case when weight_tier = '21' and delivery_station_4pl = 'J&T Express' and (seller_region != 'GBKK' and buyer_region != 'GBKK') then shipment_id end ) as jnt_UPC_21   

from rate_card_map
group by 1 

)
select 
    pre_delivered_agg.report_month
    --allocation 
    ,cast(total_flash_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_flash 
    ,cast(total_flash_tier_a_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_flash_tier_a
    ,cast(total_flash_tier_b_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_flash_tier_b
    ,cast(total_flash_tier_c_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_flash_tier_c
    ,cast(total_flash_tier_d_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_flash_tier_d
    ,cast(total_flash_tier_e_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_flash_tier_e
    ,cast(total_flash_tier_f_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_flash_tier_f
    
    ,cast(total_kerry_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_kerry 
    ,cast(total_kerry_tier_a_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_kerry_tier_a
    ,cast(total_kerry_tier_b_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_kerry_tier_b
    ,cast(total_kerry_tier_c_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_kerry_tier_c
    ,cast(total_kerry_tier_d_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_kerry_tier_d
    ,cast(total_kerry_tier_e_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_kerry_tier_e
    ,cast(total_kerry_tier_f_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_kerry_tier_f
    
    ,cast(total_njv_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_njv 
    ,cast(total_njv_tier_a_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_njv_tier_a
    ,cast(total_njv_tier_b_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_njv_tier_b
    ,cast(total_njv_tier_c_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_njv_tier_c
    ,cast(total_njv_tier_d_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_njv_tier_d
    ,cast(total_njv_tier_e_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_njv_tier_e
    ,cast(total_njv_tier_f_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_njv_tier_f
    
    ,cast(total_cj_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_cj 
    ,cast(total_cj_tier_a_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_cj_tier_a
    ,cast(total_cj_tier_b_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_cj_tier_b
    ,cast(total_cj_tier_c_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_cj_tier_c
    ,cast(total_cj_tier_d_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_cj_tier_d
    ,cast(total_cj_tier_e_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_cj_tier_e
    ,cast(total_cj_tier_f_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as allocation_cj_tier_f

     ,cast(total_jnt_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume + total_jnt_volume) as allocation_jnt
    ,cast(total_jnt_tier_a_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume + total_jnt_volume) as allocation_jnt_tier_a
    ,cast(total_jnt_tier_b_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume + total_jnt_volume) as allocation_jnt_tier_b
    ,cast(total_jnt_tier_c_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume + total_jnt_volume) as allocation_jnt_tier_c
    ,cast(total_jnt_tier_d_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume + total_jnt_volume) as allocation_jnt_tier_d
    ,cast(total_jnt_tier_e_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume + total_jnt_volume) as allocation_jnt_tier_e
    ,cast(total_jnt_tier_f_volume as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume + total_jnt_volume) as allocation_jnt_tier_f

    -- overall volume 
    ,total_flash_volume
    ,total_flash_tier_a_volume
    ,total_flash_tier_b_volume
    ,total_flash_tier_c_volume
    ,total_flash_tier_d_volume
    ,total_flash_tier_e_volume
    ,total_flash_tier_f_volume
    ,total_kerry_volume
    ,total_kerry_tier_a_volume
    ,total_kerry_tier_b_volume
    ,total_kerry_tier_c_volume
    ,total_kerry_tier_d_volume
    ,total_kerry_tier_e_volume
    ,total_kerry_tier_f_volume
    ,total_njv_volume
    ,total_njv_tier_a_volume
    ,total_njv_tier_b_volume
    ,total_njv_tier_c_volume
    ,total_njv_tier_d_volume
    ,total_njv_tier_e_volume
    ,total_njv_tier_f_volume
    ,total_cj_volume
    ,total_cj_tier_a_volume
    ,total_cj_tier_b_volume
    ,total_cj_tier_c_volume
    ,total_cj_tier_d_volume
    ,total_cj_tier_e_volume
    ,total_cj_tier_f_volume
    ,total_jnt_volume
    ,total_jnt_tier_a_volume
    ,total_jnt_tier_b_volume
    ,total_jnt_tier_c_volume
    ,total_jnt_tier_d_volume
    ,total_jnt_tier_e_volume
    ,total_jnt_tier_f_volume
   

    -- cod volume 
    ,total_flash_cod_volume
    ,total_flash_cod_tier_a_volume
    ,total_flash_cod_tier_b_volume
    ,total_flash_cod_tier_c_volume
    ,total_flash_cod_tier_d_volume
    ,total_flash_cod_tier_e_volume
    ,total_flash_cod_tier_f_volume
    
    ,total_kerry_cod_volume
    ,total_kerry_cod_tier_a_volume
    ,total_kerry_cod_tier_b_volume
    ,total_kerry_cod_tier_c_volume
    ,total_kerry_cod_tier_d_volume
    ,total_kerry_cod_tier_e_volume
    ,total_kerry_cod_tier_f_volume

    ,total_njv_cod_volume
    ,total_njv_cod_tier_a_volume
    ,total_njv_cod_tier_b_volume
    ,total_njv_cod_tier_c_volume
    ,total_njv_cod_tier_d_volume
    ,total_njv_cod_tier_e_volume
    ,total_njv_cod_tier_f_volume

    ,total_cj_cod_volume
    ,total_cj_cod_tier_a_volume
    ,total_cj_cod_tier_b_volume
    ,total_cj_cod_tier_c_volume
    ,total_cj_cod_tier_d_volume
    ,total_cj_cod_tier_e_volume
    ,total_cj_cod_tier_f_volume

    ,total_jnt_cod_volume
    ,total_jnt_cod_tier_a_volume
    ,total_jnt_cod_tier_b_volume
    ,total_jnt_cod_tier_c_volume
    ,total_jnt_cod_tier_d_volume
    ,total_jnt_cod_tier_e_volume
    ,total_jnt_cod_tier_f_volume

    -- reverse volume 
    ,total_flash_reverse_volume
    ,total_flash_reverse_tier_a_volume
    ,total_flash_reverse_tier_b_volume
    ,total_flash_reverse_tier_c_volume
    ,total_flash_reverse_tier_d_volume
    ,total_flash_reverse_tier_e_volume
    ,total_flash_reverse_tier_f_volume
    
    
    ,total_kerry_reverse_volume
    ,total_kerry_reverse_tier_a_volume
    ,total_kerry_reverse_tier_b_volume
    ,total_kerry_reverse_tier_c_volume
    ,total_kerry_reverse_tier_d_volume
    ,total_kerry_reverse_tier_e_volume
    ,total_kerry_reverse_tier_f_volume
    
    ,total_njv_reverse_volume
    ,total_njv_reverse_tier_a_volume
    ,total_njv_reverse_tier_b_volume
    ,total_njv_reverse_tier_c_volume
    ,total_njv_reverse_tier_d_volume
    ,total_njv_reverse_tier_e_volume
    ,total_njv_reverse_tier_f_volume


    ,total_cj_reverse_volume
    ,total_cj_reverse_tier_a_volume
    ,total_cj_reverse_tier_b_volume
    ,total_cj_reverse_tier_c_volume
    ,total_cj_reverse_tier_d_volume
    ,total_cj_reverse_tier_e_volume
    ,total_cj_reverse_tier_f_volume

    ,total_jnt_reverse_volume
    ,total_jnt_reverse_tier_a_volume
    ,total_jnt_reverse_tier_b_volume
    ,total_jnt_reverse_tier_c_volume
    ,total_jnt_reverse_tier_d_volume
    ,total_jnt_reverse_tier_e_volume
    ,total_jnt_reverse_tier_f_volume

    -- remote volume 
    ,total_flash_remote_volume
    ,total_flash_remote_tier_a_volume
    ,total_flash_remote_tier_b_volume
    ,total_flash_remote_tier_c_volume
    ,total_flash_remote_tier_d_volume
    ,total_flash_remote_tier_e_volume
    ,total_flash_remote_tier_f_volume
    
    ,total_kerry_remote_volume
    ,total_kerry_remote_tier_a_volume
    ,total_kerry_remote_tier_b_volume
    ,total_kerry_remote_tier_c_volume
    ,total_kerry_remote_tier_d_volume
    ,total_kerry_remote_tier_e_volume
    ,total_kerry_remote_tier_f_volume

    ,total_njv_remote_volume
    ,total_njv_remote_tier_a_volume
    ,total_njv_remote_tier_b_volume
    ,total_njv_remote_tier_c_volume
    ,total_njv_remote_tier_d_volume
    ,total_njv_remote_tier_e_volume
    ,total_njv_remote_tier_f_volume


    ,total_cj_remote_volume
    ,total_cj_remote_tier_a_volume
    ,total_cj_remote_tier_b_volume
    ,total_cj_remote_tier_c_volume
    ,total_cj_remote_tier_d_volume
    ,total_cj_remote_tier_e_volume
    ,total_cj_remote_tier_f_volume

    ,total_jnt_remote_volume
    ,total_jnt_remote_tier_a_volume
    ,total_jnt_remote_tier_b_volume
    ,total_jnt_remote_tier_c_volume
    ,total_jnt_remote_tier_d_volume
    ,total_jnt_remote_tier_e_volume
    ,total_jnt_remote_tier_f_volume


    -- 4pl rate card 
    ,flash_shipping_fee_cpo
    ,flash_shipping_fee_cpo_tier_a
    ,flash_shipping_fee_cpo_tier_b
    ,flash_shipping_fee_cpo_tier_c
    ,flash_shipping_fee_cpo_tier_d
    ,flash_shipping_fee_cpo_tier_e
    ,flash_shipping_fee_cpo_tier_f

    ,kerry_shipping_fee_cpo
    ,kerry_shipping_fee_cpo_tier_a
    ,kerry_shipping_fee_cpo_tier_b
    ,kerry_shipping_fee_cpo_tier_c
    ,kerry_shipping_fee_cpo_tier_d
    ,kerry_shipping_fee_cpo_tier_e
    ,kerry_shipping_fee_cpo_tier_f

    ,njv_shipping_fee_cpo
    ,njv_shipping_fee_cpo_tier_a
    ,njv_shipping_fee_cpo_tier_b
    ,njv_shipping_fee_cpo_tier_c
    ,njv_shipping_fee_cpo_tier_d
    ,njv_shipping_fee_cpo_tier_e
    ,njv_shipping_fee_cpo_tier_f

    ,cj_shipping_fee_cpo
    ,cj_shipping_fee_cpo_tier_a
    ,cj_shipping_fee_cpo_tier_b
    ,cj_shipping_fee_cpo_tier_c
    ,cj_shipping_fee_cpo_tier_d
    ,cj_shipping_fee_cpo_tier_e
    ,cj_shipping_fee_cpo_tier_f

    ,jnt_shipping_fee_cpo
    ,jnt_shipping_fee_cpo_tier_a
    ,jnt_shipping_fee_cpo_tier_b
    ,jnt_shipping_fee_cpo_tier_c
    ,jnt_shipping_fee_cpo_tier_d
    ,jnt_shipping_fee_cpo_tier_e
    ,jnt_shipping_fee_cpo_tier_f

    -- cod cpo 
    ,cast(total_flash_cod_fee as double)/total_flash_cod_volume as flash_cod_cpo 
    ,cast(total_flash_tier_a_cod_fee as double)/total_flash_cod_tier_a_volume as flash_tier_a_cod_cpo 
    ,cast(total_flash_tier_b_cod_fee as double)/total_flash_cod_tier_b_volume as flash_tier_b_cod_cpo 
    ,cast(total_flash_tier_c_cod_fee as double)/total_flash_cod_tier_c_volume as flash_tier_c_cod_cpo 
    ,cast(total_flash_tier_d_cod_fee as double)/total_flash_cod_tier_d_volume as flash_tier_d_cod_cpo 
    ,cast(total_flash_tier_e_cod_fee as double)/total_flash_cod_tier_e_volume as flash_tier_e_cod_cpo 
    ,cast(total_flash_tier_f_cod_fee as double)/total_flash_cod_tier_f_volume as flash_tier_f_cod_cpo 


    ,cast(total_kerry_cod_fee as double)/total_kerry_cod_volume as kerry_cod_cpo 
    ,cast(total_kerry_tier_a_cod_fee as double)/total_kerry_cod_tier_a_volume as kerry_tier_a_cod_cpo
    ,cast(total_kerry_tier_b_cod_fee as double)/total_kerry_cod_tier_b_volume as kerry_tier_b_cod_cpo 
    ,cast(total_kerry_tier_c_cod_fee as double)/total_kerry_cod_tier_c_volume as kerry_tier_c_cod_cpo 
    ,cast(total_kerry_tier_d_cod_fee as double)/total_kerry_cod_tier_d_volume as kerry_tier_d_cod_cpo 
    ,cast(total_kerry_tier_e_cod_fee as double)/total_kerry_cod_tier_e_volume as kerry_tier_e_cod_cpo 
    ,cast(total_kerry_tier_f_cod_fee as double)/total_kerry_cod_tier_f_volume as kerry_tier_f_cod_cpo  
    


    ,cast(total_njv_cod_fee as double)/total_njv_cod_volume as njv_cod_cpo 
    ,cast(total_njv_tier_a_cod_fee as double)/total_njv_cod_tier_a_volume as njv_tier_a_cod_cpo
    ,cast(total_njv_tier_b_cod_fee as double)/total_njv_cod_tier_b_volume as njv_tier_b_cod_cpo
    ,cast(total_njv_tier_c_cod_fee as double)/total_njv_cod_tier_c_volume as njv_tier_c_cod_cpo
    ,cast(total_njv_tier_d_cod_fee as double)/total_njv_cod_tier_d_volume as njv_tier_d_cod_cpo
    ,cast(total_njv_tier_e_cod_fee as double)/total_njv_cod_tier_e_volume as njv_tier_e_cod_cpo
    ,cast(total_njv_tier_f_cod_fee as double)/total_njv_cod_tier_f_volume as njv_tier_f_cod_cpo
    
    ,cast(total_cj_cod_fee as double)/total_cj_cod_volume as cj_cod_cpo 
    ,cast(total_cj_tier_a_cod_fee as double)/total_cj_cod_tier_a_volume as cj_tier_a_cod_cpo 
    ,cast(total_cj_tier_b_cod_fee as double)/total_cj_cod_tier_b_volume as cj_tier_b_cod_cpo 
    ,cast(total_cj_tier_c_cod_fee as double)/total_cj_cod_tier_c_volume as cj_tier_c_cod_cpo 
    ,cast(total_cj_tier_d_cod_fee as double)/total_cj_cod_tier_d_volume as cj_tier_d_cod_cpo 
    ,cast(total_cj_tier_e_cod_fee as double)/total_cj_cod_tier_e_volume as cj_tier_e_cod_cpo 
    ,cast(total_cj_tier_f_cod_fee as double)/total_cj_cod_tier_f_volume as cj_tier_f_cod_cpo 

    ,cast(total_jnt_cod_fee as double)/total_jnt_cod_volume as jnt_cod_cpo 
    ,cast(total_jnt_tier_a_cod_fee as double)/total_jnt_cod_tier_a_volume as jnt_tier_a_cod_cpo 
    ,cast(total_jnt_tier_b_cod_fee as double)/total_jnt_cod_tier_b_volume as jnt_tier_b_cod_cpo 
    ,cast(total_jnt_tier_c_cod_fee as double)/total_jnt_cod_tier_c_volume as jnt_tier_c_cod_cpo 
    ,cast(total_jnt_tier_d_cod_fee as double)/total_jnt_cod_tier_d_volume as jnt_tier_d_cod_cpo 
    ,cast(total_jnt_tier_e_cod_fee as double)/total_jnt_cod_tier_e_volume as jnt_tier_e_cod_cpo 
    ,cast(total_jnt_tier_f_cod_fee as double)/total_jnt_cod_tier_f_volume as jnt_tier_f_cod_cpo 

    
    -- remote cpo 
    ,cast(total_flash_remote_cost as double)/total_flash_remote_volume as flash_remote_cpo 
    ,cast(total_flash_tier_a_remote_cost as double)/total_flash_remote_tier_a_volume as flash_tier_a_remote_cpo
    ,cast(total_flash_tier_b_remote_cost as double)/total_flash_remote_tier_b_volume as flash_tier_b_remote_cpo
    ,cast(total_flash_tier_c_remote_cost as double)/total_flash_remote_tier_c_volume as flash_tier_c_remote_cpo
    ,cast(total_flash_tier_d_remote_cost as double)/total_flash_remote_tier_d_volume as flash_tier_d_remote_cpo
    ,cast(total_flash_tier_e_remote_cost as double)/total_flash_remote_tier_e_volume as flash_tier_e_remote_cpo
    ,cast(total_flash_tier_f_remote_cost as double)/total_flash_remote_tier_f_volume as flash_tier_f_remote_cpo

    ,cast(total_kerry_remote_cost as double)/total_kerry_remote_volume as kerry_remote_cpo 
    ,cast(total_kerry_tier_a_remote_cost as double)/total_kerry_remote_tier_a_volume as kerry_tier_a_remote_cpo
    ,cast(total_kerry_tier_b_remote_cost as double)/total_kerry_remote_tier_b_volume as kerry_tier_b_remote_cpo
    ,cast(total_kerry_tier_c_remote_cost as double)/total_kerry_remote_tier_c_volume as kerry_tier_c_remote_cpo
    ,cast(total_kerry_tier_d_remote_cost as double)/total_kerry_remote_tier_d_volume as kerry_tier_d_remote_cpo
    ,cast(total_kerry_tier_e_remote_cost as double)/total_kerry_remote_tier_e_volume as kerry_tier_e_remote_cpo
    ,cast(total_kerry_tier_f_remote_cost as double)/total_kerry_remote_tier_f_volume as kerry_tier_f_remote_cpo


    ,cast(total_njv_remote_cost as double)/total_njv_remote_volume as njv_remote_cpo 
    ,cast(total_njv_tier_a_remote_cost as double)/total_njv_remote_tier_a_volume as njv_tier_a_remote_cpo 
    ,cast(total_njv_tier_b_remote_cost as double)/total_njv_remote_tier_b_volume as njv_tier_b_remote_cpo 
    ,cast(total_njv_tier_c_remote_cost as double)/total_njv_remote_tier_c_volume as njv_tier_c_remote_cpo 
    ,cast(total_njv_tier_d_remote_cost as double)/total_njv_remote_tier_d_volume as njv_tier_d_remote_cpo 
    ,cast(total_njv_tier_e_remote_cost as double)/total_njv_remote_tier_e_volume as njv_tier_e_remote_cpo 
    ,cast(total_njv_tier_f_remote_cost as double)/total_njv_remote_tier_f_volume as njv_tier_f_remote_cpo 


    ,cast(total_cj_remote_cost as double)/total_cj_remote_volume as cj_remote_cpo 
    ,cast(total_cj_tier_a_remote_cost as double)/total_cj_remote_tier_a_volume as cj_remote_cpo_tier_a 
    ,cast(total_cj_tier_b_remote_cost as double)/total_cj_remote_tier_b_volume as cj_remote_cpo_tier_b
    ,cast(total_cj_tier_c_remote_cost as double)/total_cj_remote_tier_c_volume as cj_remote_cpo_tier_c
    ,cast(total_cj_tier_d_remote_cost as double)/total_cj_remote_tier_d_volume as cj_remote_cpo_tier_d
    ,cast(total_cj_tier_e_remote_cost as double)/total_cj_remote_tier_e_volume as cj_remote_cpo_tier_e
    ,cast(total_cj_tier_f_remote_cost as double)/total_cj_remote_tier_f_volume as cj_remote_cpo_tier_f

    ,cast(total_jnt_remote_cost as double)/total_jnt_remote_volume as jnt_remote_cpo 
    ,cast(total_jnt_tier_a_remote_cost as double)/total_jnt_remote_tier_a_volume as jnt_remote_cpo_tier_a 
    ,cast(total_jnt_tier_b_remote_cost as double)/total_jnt_remote_tier_b_volume as jnt_remote_cpo_tier_b
    ,cast(total_jnt_tier_c_remote_cost as double)/total_jnt_remote_tier_c_volume as jnt_remote_cpo_tier_c
    ,cast(total_jnt_tier_d_remote_cost as double)/total_jnt_remote_tier_d_volume as jnt_remote_cpo_tier_d
    ,cast(total_jnt_tier_e_remote_cost as double)/total_jnt_remote_tier_e_volume as jnt_remote_cpo_tier_e
    ,cast(total_jnt_tier_f_remote_cost as double)/total_jnt_remote_tier_f_volume as jnt_remote_cpo_tier_f

     -- reverse cpo 
    ,cast(total_flash_reverse_fee as double)/total_flash_reverse_volume as flash_reverse_cpo 
    ,cast(total_flash_reverse_tier_a_fee as double)/total_flash_reverse_tier_a_volume as flash_tier_a_reverse_cpo
    ,cast(total_flash_reverse_tier_b_fee as double)/total_flash_reverse_tier_b_volume as flash_tier_b_reverse_cpo 
    ,cast(total_flash_reverse_tier_c_fee as double)/total_flash_reverse_tier_c_volume as flash_tier_c_reverse_cpo 
    ,cast(total_flash_reverse_tier_d_fee as double)/total_flash_reverse_tier_d_volume as flash_tier_d_reverse_cpo 
    ,cast(total_flash_reverse_tier_e_fee as double)/total_flash_reverse_tier_e_volume as flash_tier_e_reverse_cpo 
    ,cast(total_flash_reverse_tier_f_fee as double)/total_flash_reverse_tier_f_volume as flash_tier_f_reverse_cpo 

    ,cast(total_kerry_reverse_fee as double)/total_kerry_reverse_volume as kerry_reverse_cpo 
    ,cast(total_kerry_reverse_tier_a_fee as double)/total_kerry_reverse_tier_a_volume as kerry_tier_a_reverse_cpo
    ,cast(total_kerry_reverse_tier_b_fee as double)/total_kerry_reverse_tier_b_volume as kerry_tier_b_reverse_cpo 
    ,cast(total_kerry_reverse_tier_c_fee as double)/total_kerry_reverse_tier_c_volume as kerry_tier_c_reverse_cpo 
    ,cast(total_kerry_reverse_tier_d_fee as double)/total_kerry_reverse_tier_d_volume as kerry_tier_d_reverse_cpo 
    ,cast(total_kerry_reverse_tier_e_fee as double)/total_kerry_reverse_tier_e_volume as kerry_tier_e_reverse_cpo 
    ,cast(total_kerry_reverse_tier_f_fee as double)/total_kerry_reverse_tier_f_volume as kerry_tier_f_reverse_cpo 
    
    
    ,cast(total_njv_reverse_fee as double)/total_njv_reverse_volume as njv_reverse_cpo 
    ,cast(total_njv_reverse_tier_a_fee as double)/total_njv_reverse_tier_a_volume as njv_tier_a_reverse_cpo
    ,cast(total_njv_reverse_tier_b_fee as double)/total_njv_reverse_tier_b_volume as njv_tier_b_reverse_cpo
    ,cast(total_njv_reverse_tier_c_fee as double)/total_njv_reverse_tier_c_volume as njv_tier_c_reverse_cpo
    ,cast(total_njv_reverse_tier_d_fee as double)/total_njv_reverse_tier_d_volume as njv_tier_d_reverse_cpo
    ,cast(total_njv_reverse_tier_e_fee as double)/total_njv_reverse_tier_e_volume as njv_tier_e_reverse_cpo
    ,cast(total_njv_reverse_tier_f_fee as double)/total_njv_reverse_tier_f_volume as njv_tier_f_reverse_cpo



    ,cast(total_cj_reverse_fee as double)/total_cj_reverse_volume as cj_reverse_cpo 
    ,cast(total_cj_reverse_tier_a_fee as double)/total_cj_reverse_tier_a_volume as cj_tier_a_reverse_cpo
    ,cast(total_cj_reverse_tier_b_fee as double)/total_cj_reverse_tier_b_volume as cj_tier_b_reverse_cpo
    ,cast(total_cj_reverse_tier_c_fee as double)/total_cj_reverse_tier_c_volume as cj_tier_c_reverse_cpo
    ,cast(total_cj_reverse_tier_d_fee as double)/total_cj_reverse_tier_d_volume as cj_tier_d_reverse_cpo
    ,cast(total_cj_reverse_tier_e_fee as double)/total_cj_reverse_tier_e_volume as cj_tier_e_reverse_cpo
    ,cast(total_cj_reverse_tier_f_fee as double)/total_cj_reverse_tier_f_volume as cj_tier_f_reverse_cpo

    ,cast(total_jnt_reverse_fee as double)/total_cj_reverse_volume as cj_reverse_cpo 
    ,cast(total_jnt_reverse_tier_a_fee as double)/total_cj_reverse_tier_a_volume as cj_tier_a_reverse_cpo
    ,cast(total_jnt_reverse_tier_b_fee as double)/total_cj_reverse_tier_b_volume as cj_tier_b_reverse_cpo
    ,cast(total_jnt_reverse_tier_c_fee as double)/total_cj_reverse_tier_c_volume as cj_tier_c_reverse_cpo
    ,cast(total_jnt_reverse_tier_d_fee as double)/total_cj_reverse_tier_d_volume as cj_tier_d_reverse_cpo
    ,cast(total_jnt_reverse_tier_e_fee as double)/total_cj_reverse_tier_e_volume as cj_tier_e_reverse_cpo
    ,cast(total_jnt_reverse_tier_f_fee as double)/total_cj_reverse_tier_f_volume as cj_tier_f_reverse_cpo

    -- shipping fee
    ,total_flash_shipping_fee
    ,total_flash_tier_a_shipping_fee
    ,total_flash_tier_b_shipping_fee
    ,total_flash_tier_c_shipping_fee
    ,total_flash_tier_d_shipping_fee
    ,total_flash_tier_e_shipping_fee
    ,total_flash_tier_f_shipping_fee
    
    ,total_kerry_shipping_fee
    ,total_kerry_tier_a_shipping_fee
    ,total_kerry_tier_b_shipping_fee
    ,total_kerry_tier_c_shipping_fee
    ,total_kerry_tier_d_shipping_fee
    ,total_kerry_tier_e_shipping_fee
    ,total_kerry_tier_f_shipping_fee
    
    ,total_njv_shipping_fee
    ,total_njv_tier_a_shipping_fee
    ,total_njv_tier_b_shipping_fee
    ,total_njv_tier_c_shipping_fee
    ,total_njv_tier_d_shipping_fee
    ,total_njv_tier_e_shipping_fee
    ,total_njv_tier_f_shipping_fee
    

    ,total_cj_shipping_fee
    ,total_cj_tier_a_shipping_fee
    ,total_cj_tier_b_shipping_fee
    ,total_cj_tier_c_shipping_fee
    ,total_cj_tier_d_shipping_fee
    ,total_cj_tier_e_shipping_fee
    ,total_cj_tier_f_shipping_fee

    ,total_jnt_shipping_fee
    ,total_jnt_tier_a_shipping_fee
    ,total_jnt_tier_b_shipping_fee
    ,total_jnt_tier_c_shipping_fee
    ,total_jnt_tier_d_shipping_fee
    ,total_jnt_tier_e_shipping_fee
    ,total_jnt_tier_f_shipping_fee

    -- cod fee
    ,total_flash_cod_fee
    ,total_flash_tier_a_cod_fee
    ,total_flash_tier_b_cod_fee
    ,total_flash_tier_c_cod_fee
    ,total_flash_tier_d_cod_fee
    ,total_flash_tier_e_cod_fee
    ,total_flash_tier_f_cod_fee

    ,total_kerry_cod_fee
    ,total_kerry_tier_a_cod_fee
    ,total_kerry_tier_b_cod_fee
    ,total_kerry_tier_c_cod_fee
    ,total_kerry_tier_d_cod_fee
    ,total_kerry_tier_e_cod_fee
    ,total_kerry_tier_f_cod_fee




    ,total_njv_cod_fee
    ,total_njv_tier_a_cod_fee
    ,total_njv_tier_b_cod_fee
    ,total_njv_tier_c_cod_fee
    ,total_njv_tier_d_cod_fee
    ,total_njv_tier_e_cod_fee
    ,total_njv_tier_f_cod_fee


    ,total_cj_cod_fee
    ,total_cj_tier_a_cod_fee
    ,total_cj_tier_b_cod_fee
    ,total_cj_tier_c_cod_fee
    ,total_cj_tier_d_cod_fee
    ,total_cj_tier_e_cod_fee
    ,total_cj_tier_f_cod_fee


    -- remote fee
    ,total_flash_remote_cost
    ,total_flash_tier_a_remote_cost
    ,total_flash_tier_b_remote_cost
    ,total_flash_tier_c_remote_cost
    ,total_flash_tier_d_remote_cost
    ,total_flash_tier_e_remote_cost
    ,total_flash_tier_f_remote_cost
    


    ,total_kerry_remote_cost
    ,total_kerry_tier_a_remote_cost
    ,total_kerry_tier_b_remote_cost
    ,total_kerry_tier_c_remote_cost
    ,total_kerry_tier_d_remote_cost
    ,total_kerry_tier_e_remote_cost
    ,total_kerry_tier_f_remote_cost


    ,total_njv_remote_cost
    ,total_njv_tier_a_remote_cost
    ,total_njv_tier_b_remote_cost
    ,total_njv_tier_c_remote_cost
    ,total_njv_tier_d_remote_cost
    ,total_njv_tier_e_remote_cost
    ,total_njv_tier_f_remote_cost
    
    
    
    ,total_cj_remote_cost 
    ,total_cj_tier_a_remote_cost 
    ,total_cj_tier_b_remote_cost 
    ,total_cj_tier_c_remote_cost 
    ,total_cj_tier_d_remote_cost 
    ,total_cj_tier_e_remote_cost 
    ,total_cj_tier_f_remote_cost 

    -- reverse fee 
    ,total_flash_reverse_fee
    ,total_flash_reverse_tier_a_fee 
    ,total_flash_reverse_tier_b_fee 
    ,total_flash_reverse_tier_c_fee 
    ,total_flash_reverse_tier_d_fee 
    ,total_flash_reverse_tier_e_fee 
    ,total_flash_reverse_tier_f_fee 

    ,total_kerry_reverse_fee  
    ,total_kerry_reverse_tier_a_fee
    ,total_kerry_reverse_tier_b_fee
    ,total_kerry_reverse_tier_c_fee
    ,total_kerry_reverse_tier_d_fee
    ,total_kerry_reverse_tier_e_fee
    ,total_kerry_reverse_tier_f_fee


    ,total_njv_reverse_fee  
    ,total_njv_reverse_tier_a_fee
    ,total_njv_reverse_tier_b_fee
    ,total_njv_reverse_tier_c_fee
    ,total_njv_reverse_tier_d_fee
    ,total_njv_reverse_tier_e_fee
    ,total_njv_reverse_tier_f_fee


    ,total_cj_reverse_fee
    ,total_cj_reverse_tier_a_fee
    ,total_cj_reverse_tier_b_fee
    ,total_cj_reverse_tier_c_fee
    ,total_cj_reverse_tier_d_fee
    ,total_cj_reverse_tier_e_fee
    ,total_cj_reverse_tier_f_fee


    -- total cost 
    ,total_flash_shipping_fee + total_flash_remote_cost + total_flash_reverse_fee + total_flash_cod_fee as flash_total_cost
    ,total_flash_tier_a_shipping_fee + total_flash_tier_a_remote_cost + total_flash_reverse_tier_a_fee + total_flash_tier_a_cod_fee as flash_tier_a_total_cost
    ,total_flash_tier_b_shipping_fee + total_flash_tier_b_remote_cost + total_flash_reverse_tier_b_fee + total_flash_tier_b_cod_fee as flash_tier_b_total_cost
    ,total_flash_tier_c_shipping_fee + total_flash_tier_c_remote_cost + total_flash_reverse_tier_c_fee + total_flash_tier_c_cod_fee as flash_tier_c_total_cost
    ,total_flash_tier_d_shipping_fee + total_flash_tier_d_remote_cost + total_flash_reverse_tier_d_fee + total_flash_tier_d_cod_fee as flash_tier_d_total_cost
    ,total_flash_tier_e_shipping_fee + total_flash_tier_e_remote_cost + total_flash_reverse_tier_e_fee + total_flash_tier_e_cod_fee as flash_tier_e_total_cost
    ,total_flash_tier_f_shipping_fee + total_flash_tier_f_remote_cost + total_flash_reverse_tier_f_fee + total_flash_tier_f_cod_fee as flash_tier_f_total_cost



    ,total_kerry_shipping_fee + total_kerry_remote_cost + total_kerry_reverse_fee + total_kerry_cod_fee as kerry_total_cost 
    ,total_kerry_tier_a_shipping_fee + total_kerry_tier_a_remote_cost + total_kerry_reverse_tier_a_fee + total_kerry_tier_a_cod_fee as kerry_tier_a_total_cost
    ,total_kerry_tier_b_shipping_fee + total_kerry_tier_b_remote_cost + total_kerry_reverse_tier_b_fee + total_kerry_tier_b_cod_fee as kerry_tier_b_total_cost 
    ,total_kerry_tier_c_shipping_fee + total_kerry_tier_c_remote_cost + total_kerry_reverse_tier_c_fee + total_kerry_tier_c_cod_fee as kerry_tier_c_total_cost 
    ,total_kerry_tier_d_shipping_fee + total_kerry_tier_d_remote_cost + total_kerry_reverse_tier_d_fee + total_kerry_tier_d_cod_fee as kerry_tier_d_total_cost 
    ,total_kerry_tier_e_shipping_fee + total_kerry_tier_e_remote_cost + total_kerry_reverse_tier_e_fee + total_kerry_tier_e_cod_fee as kerry_tier_e_total_cost 
    ,total_kerry_tier_f_shipping_fee + total_kerry_tier_f_remote_cost + total_kerry_reverse_tier_f_fee + total_kerry_tier_f_cod_fee as kerry_tier_f_total_cost 



    ,total_njv_shipping_fee + total_njv_remote_cost + total_njv_reverse_fee + total_njv_cod_fee as njv_total_cost
    ,total_njv_tier_a_shipping_fee + total_njv_tier_a_remote_cost + total_njv_reverse_tier_a_fee + total_njv_tier_a_cod_fee as njv_tier_a_total_cost 
    ,total_njv_tier_b_shipping_fee + total_njv_tier_b_remote_cost + total_njv_reverse_tier_b_fee + total_njv_tier_b_cod_fee as njv_tier_b_total_cost 
    ,total_njv_tier_c_shipping_fee + total_njv_tier_c_remote_cost + total_njv_reverse_tier_c_fee + total_njv_tier_c_cod_fee as njv_tier_c_total_cost 
    ,total_njv_tier_d_shipping_fee + total_njv_tier_d_remote_cost + total_njv_reverse_tier_d_fee + total_njv_tier_d_cod_fee as njv_tier_d_total_cost 
    ,total_njv_tier_e_shipping_fee + total_njv_tier_e_remote_cost + total_njv_reverse_tier_e_fee + total_njv_tier_e_cod_fee as njv_tier_e_total_cost 
    ,total_njv_tier_f_shipping_fee + total_njv_tier_f_remote_cost + total_njv_reverse_tier_f_fee + total_njv_tier_f_cod_fee as njv_tier_f_total_cost 



    ,total_cj_shipping_fee + total_cj_remote_cost + total_cj_reverse_fee + total_cj_cod_fee as cj_total_cost
    ,total_cj_tier_a_shipping_fee + total_cj_tier_a_remote_cost + total_cj_reverse_tier_a_fee + total_cj_tier_a_cod_fee as cj_tier_a_total_cost
    ,total_cj_tier_b_shipping_fee + total_cj_tier_b_remote_cost + total_cj_reverse_tier_b_fee + total_cj_tier_b_cod_fee as cj_tier_b_total_cost 
    ,total_cj_tier_c_shipping_fee + total_cj_tier_c_remote_cost + total_cj_reverse_tier_c_fee + total_cj_tier_c_cod_fee as cj_tier_c_total_cost 
    ,total_cj_tier_d_shipping_fee + total_cj_tier_d_remote_cost + total_cj_reverse_tier_d_fee + total_cj_tier_d_cod_fee as cj_tier_d_total_cost 
    ,total_cj_tier_e_shipping_fee + total_cj_tier_e_remote_cost + total_cj_reverse_tier_e_fee + total_cj_tier_e_cod_fee as cj_tier_e_total_cost 
    ,total_cj_tier_f_shipping_fee + total_cj_tier_f_remote_cost + total_cj_reverse_tier_f_fee + total_cj_tier_f_cod_fee as cj_tier_f_total_cost 
    

    -- cost type cpo 
    ,cast(total_flash_shipping_fee + total_kerry_shipping_fee + total_njv_shipping_fee + total_cj_shipping_fee as double)/(total_flash_volume + total_kerry_volume + total_njv_volume + total_cj_volume) as shipping_fee_cpo 
    ,cast(total_flash_tier_a_shipping_fee + total_kerry_tier_a_shipping_fee + total_njv_tier_a_shipping_fee + total_cj_tier_a_shipping_fee as double)/(total_flash_tier_a_volume + total_kerry_tier_a_volume + total_njv_tier_a_volume + total_cj_tier_a_volume) as shipping_fee_cpo_tier_a 
    ,cast(total_flash_tier_b_shipping_fee + total_kerry_tier_b_shipping_fee + total_njv_tier_b_shipping_fee + total_cj_tier_b_shipping_fee as double)/(total_flash_tier_b_volume + total_kerry_tier_b_volume + total_njv_tier_b_volume + total_cj_tier_b_volume) as shipping_fee_cpo_tier_b
    ,cast(total_flash_tier_c_shipping_fee + total_kerry_tier_c_shipping_fee + total_njv_tier_c_shipping_fee + total_cj_tier_c_shipping_fee as double)/(total_flash_tier_c_volume + total_kerry_tier_c_volume + total_njv_tier_c_volume + total_cj_tier_c_volume) as shipping_fee_cpo_tier_c
    ,cast(total_flash_tier_d_shipping_fee + total_kerry_tier_d_shipping_fee + total_njv_tier_d_shipping_fee + total_cj_tier_d_shipping_fee as double)/(total_flash_tier_d_volume + total_kerry_tier_d_volume + total_njv_tier_d_volume + total_cj_tier_d_volume) as shipping_fee_cpo_tier_d
    ,cast(total_flash_tier_e_shipping_fee + total_kerry_tier_e_shipping_fee + total_njv_tier_e_shipping_fee + total_cj_tier_e_shipping_fee as double)/(total_flash_tier_e_volume + total_kerry_tier_e_volume + total_njv_tier_e_volume + total_cj_tier_e_volume) as shipping_fee_cpo_tier_e
    ,cast(total_flash_tier_f_shipping_fee + total_kerry_tier_f_shipping_fee + total_njv_tier_f_shipping_fee + total_cj_tier_f_shipping_fee as double)/(total_flash_tier_f_volume + total_kerry_tier_f_volume + total_njv_tier_f_volume + total_cj_tier_f_volume) as shipping_fee_cpo_tier_f 



    ,cast(total_flash_remote_cost + total_kerry_remote_cost + total_njv_remote_cost + total_cj_remote_cost as double)/(total_flash_remote_volume + total_kerry_remote_volume + total_njv_remote_volume + total_cj_remote_volume) as remote_fee_cpo
    ,cast(total_flash_tier_a_remote_cost + total_kerry_tier_a_remote_cost + total_njv_tier_a_remote_cost + total_cj_tier_a_remote_cost as double)/(total_flash_remote_tier_a_volume + total_kerry_remote_tier_a_volume + total_njv_remote_tier_a_volume + total_cj_remote_tier_a_volume) as remote_tier_a_fee_cpo
    ,cast(total_flash_tier_b_remote_cost + total_kerry_tier_b_remote_cost + total_njv_tier_b_remote_cost + total_cj_tier_b_remote_cost as double)/(total_flash_remote_tier_b_volume + total_kerry_remote_tier_b_volume + total_njv_remote_tier_b_volume + total_cj_remote_tier_b_volume) as remote_tier_b_fee_cpo
    ,cast(total_flash_tier_c_remote_cost + total_kerry_tier_c_remote_cost + total_njv_tier_c_remote_cost + total_cj_tier_c_remote_cost as double)/(total_flash_remote_tier_c_volume + total_kerry_remote_tier_c_volume + total_njv_remote_tier_c_volume + total_cj_remote_tier_c_volume) as remote_tier_c_fee_cpo
    ,cast(total_flash_tier_d_remote_cost + total_kerry_tier_d_remote_cost + total_njv_tier_d_remote_cost + total_cj_tier_d_remote_cost as double)/(total_flash_remote_tier_d_volume + total_kerry_remote_tier_d_volume + total_njv_remote_tier_d_volume + total_cj_remote_tier_d_volume) as remote_tier_d_fee_cpo
    ,cast(total_flash_tier_e_remote_cost + total_kerry_tier_e_remote_cost + total_njv_tier_e_remote_cost + total_cj_tier_e_remote_cost as double)/(total_flash_remote_tier_e_volume + total_kerry_remote_tier_e_volume + total_njv_remote_tier_e_volume + total_cj_remote_tier_e_volume) as remote_tier_e_fee_cpo
    ,cast(total_flash_tier_f_remote_cost + total_kerry_tier_f_remote_cost + total_njv_tier_f_remote_cost + total_cj_tier_f_remote_cost as double)/(total_flash_remote_tier_f_volume + total_kerry_remote_tier_f_volume + total_njv_remote_tier_f_volume + total_cj_remote_tier_f_volume) as remote_tier_f_fee_cpo




    ,cast(total_flash_reverse_fee + total_kerry_reverse_fee + total_njv_reverse_fee + total_cj_reverse_fee as double)/(total_flash_reverse_volume + total_kerry_reverse_volume + total_njv_reverse_volume + total_cj_reverse_volume) as reverse_fee_cpo 
    ,cast(total_flash_reverse_tier_a_fee + total_kerry_reverse_tier_a_fee + total_njv_reverse_tier_a_fee + total_cj_reverse_tier_a_fee as double)/(total_flash_reverse_tier_a_volume + total_kerry_reverse_tier_a_volume + total_njv_reverse_tier_a_volume + total_cj_reverse_tier_a_volume) as reverse_tier_a_fee_cpo
    ,cast(total_flash_reverse_tier_b_fee + total_kerry_reverse_tier_b_fee + total_njv_reverse_tier_b_fee + total_cj_reverse_tier_b_fee as double)/(total_flash_reverse_tier_b_volume + total_kerry_reverse_tier_b_volume + total_njv_reverse_tier_b_volume + total_cj_reverse_tier_b_volume) as reverse_tier_b_fee_cpo
    ,cast(total_flash_reverse_tier_c_fee + total_kerry_reverse_tier_c_fee + total_njv_reverse_tier_c_fee + total_cj_reverse_tier_c_fee as double)/(total_flash_reverse_tier_c_volume + total_kerry_reverse_tier_c_volume + total_njv_reverse_tier_c_volume + total_cj_reverse_tier_c_volume) as reverse_tier_c_fee_cpo
    ,cast(total_flash_reverse_tier_d_fee + total_kerry_reverse_tier_d_fee + total_njv_reverse_tier_d_fee + total_cj_reverse_tier_d_fee as double)/(total_flash_reverse_tier_d_volume + total_kerry_reverse_tier_d_volume + total_njv_reverse_tier_d_volume + total_cj_reverse_tier_d_volume) as reverse_tier_d_fee_cpo
    ,cast(total_flash_reverse_tier_e_fee + total_kerry_reverse_tier_e_fee + total_njv_reverse_tier_e_fee + total_cj_reverse_tier_e_fee as double)/(total_flash_reverse_tier_e_volume + total_kerry_reverse_tier_e_volume + total_njv_reverse_tier_e_volume + total_cj_reverse_tier_e_volume) as reverse_tier_e_fee_cpo
    ,cast(total_flash_reverse_tier_f_fee + total_kerry_reverse_tier_f_fee + total_njv_reverse_tier_f_fee + total_cj_reverse_tier_f_fee as double)/(total_flash_reverse_tier_f_volume + total_kerry_reverse_tier_f_volume + total_njv_reverse_tier_f_volume + total_cj_reverse_tier_f_volume) as reverse_tier_f_fee_cpo


    ,cast(total_flash_cod_fee + total_kerry_cod_fee + total_njv_cod_fee + total_cj_cod_fee as double)/(total_flash_cod_volume + total_kerry_cod_volume + total_njv_cod_volume + total_cj_cod_volume) as cod_fee_cpo
    ,cast(total_flash_tier_a_cod_fee + total_kerry_tier_a_cod_fee + total_njv_tier_a_cod_fee + total_cj_tier_a_cod_fee as double)/(total_flash_cod_tier_a_volume + total_kerry_cod_tier_a_volume + total_njv_cod_tier_a_volume + total_cj_cod_tier_a_volume) as cod_tier_a_fee_cpo
    ,cast(total_flash_tier_b_cod_fee + total_kerry_tier_b_cod_fee + total_njv_tier_b_cod_fee + total_cj_tier_b_cod_fee as double)/(total_flash_cod_tier_b_volume + total_kerry_cod_tier_b_volume + total_njv_cod_tier_b_volume + total_cj_cod_tier_b_volume) as cod_tier_b_fee_cpo
    ,cast(total_flash_tier_c_cod_fee + total_kerry_tier_c_cod_fee + total_njv_tier_c_cod_fee + total_cj_tier_c_cod_fee as double)/(total_flash_cod_tier_c_volume + total_kerry_cod_tier_c_volume + total_njv_cod_tier_c_volume + total_cj_cod_tier_c_volume) as cod_tier_c_fee_cpo
    ,cast(total_flash_tier_d_cod_fee + total_kerry_tier_d_cod_fee + total_njv_tier_d_cod_fee + total_cj_tier_d_cod_fee as double)/(total_flash_cod_tier_d_volume + total_kerry_cod_tier_d_volume + total_njv_cod_tier_d_volume + total_cj_cod_tier_d_volume) as cod_tier_d_fee_cpo
    ,cast(total_flash_tier_e_cod_fee + total_kerry_tier_e_cod_fee + total_njv_tier_e_cod_fee + total_cj_tier_e_cod_fee as double)/(total_flash_cod_tier_e_volume + total_kerry_cod_tier_e_volume + total_njv_cod_tier_e_volume + total_cj_cod_tier_e_volume) as cod_tier_e_fee_cpo
    ,cast(total_flash_tier_f_cod_fee + total_kerry_tier_f_cod_fee + total_njv_tier_f_cod_fee + total_cj_tier_f_cod_fee as double)/(total_flash_cod_tier_f_volume + total_kerry_cod_tier_f_volume + total_njv_cod_tier_f_volume + total_cj_cod_tier_f_volume) as cod_tier_f_fee_cpo


    -- true cpo 
    ,cast( total_flash_shipping_fee + total_flash_remote_cost + total_flash_reverse_fee + total_flash_cod_fee   as double)/total_flash_volume as true_flash_cpo 
    ,cast( total_flash_tier_a_shipping_fee + total_flash_tier_a_remote_cost + total_flash_reverse_tier_a_fee + total_flash_tier_a_cod_fee   as double)/total_flash_tier_a_volume as true_flash_tier_a_cpo
    ,cast( total_flash_tier_b_shipping_fee + total_flash_tier_b_remote_cost + total_flash_reverse_tier_b_fee + total_flash_tier_b_cod_fee   as double)/total_flash_tier_b_volume as true_flash_tier_b_cpo 
    ,cast( total_flash_tier_c_shipping_fee + total_flash_tier_c_remote_cost + total_flash_reverse_tier_c_fee + total_flash_tier_c_cod_fee   as double)/total_flash_tier_c_volume as true_flash_tier_c_cpo 
    ,cast( total_flash_tier_d_shipping_fee + total_flash_tier_d_remote_cost + total_flash_reverse_tier_d_fee + total_flash_tier_d_cod_fee   as double)/total_flash_tier_d_volume as true_flash_tier_d_cpo 
    ,cast( total_flash_tier_e_shipping_fee + total_flash_tier_e_remote_cost + total_flash_reverse_tier_e_fee + total_flash_tier_e_cod_fee   as double)/total_flash_tier_e_volume as true_flash_tier_e_cpo 
    ,cast( total_flash_tier_f_shipping_fee + total_flash_tier_f_remote_cost + total_flash_reverse_tier_f_fee + total_flash_tier_f_cod_fee   as double)/total_flash_tier_f_volume as true_flash_tier_f_cpo 



    ,cast( total_kerry_shipping_fee + total_kerry_remote_cost + total_kerry_reverse_fee + total_kerry_cod_fee   as double)/total_kerry_volume as true_kerry_cpo 
    ,cast( total_kerry_tier_a_shipping_fee + total_kerry_tier_a_remote_cost + total_kerry_reverse_tier_a_fee + total_kerry_tier_a_cod_fee   as double)/total_kerry_tier_a_volume as true_kerry_tier_a_cpo
    ,cast( total_kerry_tier_b_shipping_fee + total_kerry_tier_b_remote_cost + total_kerry_reverse_tier_b_fee + total_kerry_tier_b_cod_fee   as double)/total_kerry_tier_b_volume as true_kerry_tier_b_cpo 
    ,cast( total_kerry_tier_c_shipping_fee + total_kerry_tier_c_remote_cost + total_kerry_reverse_tier_c_fee + total_kerry_tier_c_cod_fee   as double)/total_kerry_tier_c_volume as true_kerry_tier_c_cpo 
    ,cast( total_kerry_tier_d_shipping_fee + total_kerry_tier_d_remote_cost + total_kerry_reverse_tier_d_fee + total_kerry_tier_d_cod_fee   as double)/total_kerry_tier_d_volume as true_kerry_tier_d_cpo 
    ,cast( total_kerry_tier_e_shipping_fee + total_kerry_tier_e_remote_cost + total_kerry_reverse_tier_e_fee + total_kerry_tier_e_cod_fee   as double)/total_kerry_tier_e_volume as true_kerry_tier_e_cpo 
    ,cast( total_kerry_tier_f_shipping_fee + total_kerry_tier_f_remote_cost + total_kerry_reverse_tier_f_fee + total_kerry_tier_f_cod_fee   as double)/total_kerry_tier_f_volume as true_kerry_tier_f_cpo 


    ,cast( total_njv_shipping_fee + total_njv_remote_cost + total_njv_reverse_fee + total_njv_cod_fee   as double)/total_njv_volume as true_njv_cpo
    ,cast( total_njv_tier_a_shipping_fee + total_njv_tier_a_remote_cost + total_njv_reverse_tier_a_fee + total_njv_tier_a_cod_fee   as double)/total_njv_tier_a_volume as true_njv_tier_a_cpo
    ,cast( total_njv_tier_b_shipping_fee + total_njv_tier_b_remote_cost + total_njv_reverse_tier_b_fee + total_njv_tier_b_cod_fee   as double)/total_njv_tier_b_volume as true_njv_tier_b_cpo 
    ,cast( total_njv_tier_c_shipping_fee + total_njv_tier_c_remote_cost + total_njv_reverse_tier_c_fee + total_njv_tier_c_cod_fee   as double)/total_njv_tier_c_volume as true_njv_tier_c_cpo 
    ,cast( total_njv_tier_d_shipping_fee + total_njv_tier_d_remote_cost + total_njv_reverse_tier_d_fee + total_njv_tier_d_cod_fee   as double)/total_njv_tier_d_volume as true_njv_tier_d_cpo 
    ,cast( total_njv_tier_e_shipping_fee + total_njv_tier_e_remote_cost + total_njv_reverse_tier_e_fee + total_njv_tier_e_cod_fee   as double)/total_njv_tier_e_volume as true_njv_tier_e_cpo 
    ,cast( total_njv_tier_f_shipping_fee + total_njv_tier_f_remote_cost + total_njv_reverse_tier_f_fee + total_njv_tier_f_cod_fee   as double)/total_njv_tier_f_volume as true_njv_tier_f_cpo 


    ,cast( total_cj_shipping_fee + total_cj_remote_cost + total_cj_reverse_fee + total_cj_cod_fee   as double)/total_cj_volume as true_cj_cpo
    ,cast( total_cj_tier_a_shipping_fee + total_cj_tier_a_remote_cost + total_cj_reverse_tier_a_fee + total_cj_tier_a_cod_fee   as double)/total_cj_tier_a_volume as true_cj_cpo
    ,cast( total_cj_tier_b_shipping_fee + total_cj_tier_b_remote_cost + total_cj_reverse_tier_b_fee + total_cj_tier_b_cod_fee   as double)/total_cj_tier_b_volume as true_cj_cpo 
    ,cast( total_cj_tier_c_shipping_fee + total_cj_tier_c_remote_cost + total_cj_reverse_tier_c_fee + total_cj_tier_c_cod_fee   as double)/total_cj_tier_c_volume as true_cj_cpo 
    ,cast( total_cj_tier_d_shipping_fee + total_cj_tier_d_remote_cost + total_cj_reverse_tier_d_fee + total_cj_tier_d_cod_fee   as double)/total_cj_tier_d_volume as true_cj_cpo 
    ,cast( total_cj_tier_e_shipping_fee + total_cj_tier_e_remote_cost + total_cj_reverse_tier_e_fee + total_cj_tier_e_cod_fee   as double)/total_cj_tier_e_volume as true_cj_cpo 
    ,cast( total_cj_tier_f_shipping_fee + total_cj_tier_f_remote_cost + total_cj_reverse_tier_f_fee + total_cj_tier_f_cod_fee   as double)/total_cj_tier_f_volume as true_cj_cpo 

    -- volume by weight tier 
    ,flash_GBKK_05
    ,flash_GBKK_1
    ,flash_GBKK_2
    ,flash_GBKK_3
    ,flash_GBKK_4
    ,flash_GBKK_5
    ,flash_GBKK_6
    ,flash_GBKK_7
    ,flash_GBKK_8
    ,flash_GBKK_9
    ,flash_GBKK_10
    ,flash_GBKK_11
    ,flash_GBKK_12
    ,flash_GBKK_13
    ,flash_GBKK_14
    ,flash_GBKK_15
    ,flash_GBKK_16
    ,flash_GBKK_17
    ,flash_GBKK_18
    ,flash_GBKK_19
    ,flash_GBKK_20
    ,flash_GBKK_21

    ,flash_UPC_05
    ,flash_UPC_1
    ,flash_UPC_2
    ,flash_UPC_3
    ,flash_UPC_4
    ,flash_UPC_5
    ,flash_UPC_6
    ,flash_UPC_7
    ,flash_UPC_8
    ,flash_UPC_9
    ,flash_UPC_10
    ,flash_UPC_11
    ,flash_UPC_12
    ,flash_UPC_13
    ,flash_UPC_14
    ,flash_UPC_15
    ,flash_UPC_16
    ,flash_UPC_17
    ,flash_UPC_18
    ,flash_UPC_19
    ,flash_UPC_20
    ,flash_UPC_21


    ,kerry_GBKK_05
    ,kerry_GBKK_1
    ,kerry_GBKK_2
    ,kerry_GBKK_3
    ,kerry_GBKK_4
    ,kerry_GBKK_5
    ,kerry_GBKK_6
    ,kerry_GBKK_7
    ,kerry_GBKK_8
    ,kerry_GBKK_9
    ,kerry_GBKK_10
    ,kerry_GBKK_11
    ,kerry_GBKK_12
    ,kerry_GBKK_13
    ,kerry_GBKK_14
    ,kerry_GBKK_15
    ,kerry_GBKK_16
    ,kerry_GBKK_17
    ,kerry_GBKK_18
    ,kerry_GBKK_19
    ,kerry_GBKK_20
    ,kerry_GBKK_21

    ,kerry_UPC_05
    ,kerry_UPC_1
    ,kerry_UPC_2
    ,kerry_UPC_3
    ,kerry_UPC_4
    ,kerry_UPC_5
    ,kerry_UPC_6
    ,kerry_UPC_7
    ,kerry_UPC_8
    ,kerry_UPC_9
    ,kerry_UPC_10
    ,kerry_UPC_11
    ,kerry_UPC_12
    ,kerry_UPC_13
    ,kerry_UPC_14
    ,kerry_UPC_15
    ,kerry_UPC_16
    ,kerry_UPC_17
    ,kerry_UPC_18
    ,kerry_UPC_19
    ,kerry_UPC_20
    ,kerry_UPC_21


    ,njv_GBKK_05
    ,njv_GBKK_1
    ,njv_GBKK_2
    ,njv_GBKK_3
    ,njv_GBKK_4
    ,njv_GBKK_5
    ,njv_GBKK_6
    ,njv_GBKK_7
    ,njv_GBKK_8
    ,njv_GBKK_9
    ,njv_GBKK_10
    ,njv_GBKK_11
    ,njv_GBKK_12
    ,njv_GBKK_13
    ,njv_GBKK_14
    ,njv_GBKK_15
    ,njv_GBKK_16
    ,njv_GBKK_17
    ,njv_GBKK_18
    ,njv_GBKK_19
    ,njv_GBKK_20
    ,njv_GBKK_21

    ,njv_UPC_05
    ,njv_UPC_1
    ,njv_UPC_2
    ,njv_UPC_3
    ,njv_UPC_4
    ,njv_UPC_5
    ,njv_UPC_6
    ,njv_UPC_7
    ,njv_UPC_8
    ,njv_UPC_9
    ,njv_UPC_10
    ,njv_UPC_11
    ,njv_UPC_12
    ,njv_UPC_13
    ,njv_UPC_14
    ,njv_UPC_15
    ,njv_UPC_16
    ,njv_UPC_17
    ,njv_UPC_18
    ,njv_UPC_19
    ,njv_UPC_20
    ,njv_UPC_21    

    ,kerry_upc
    ,kerry_gbkk
from pre_delivered_agg
left join pre_reverse_agg 
on pre_delivered_agg.report_month = pre_reverse_agg.report_month
where pre_delivered_agg.report_month is not null 
order by 1 