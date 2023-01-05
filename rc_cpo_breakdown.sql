with seller_address AS 
(
SELECT 
    fleet_order.shipment_id
    ,CASE 
        WHEN pickup.seller_province IS NOT NULL THEN pickup.seller_province
        ELSE dropoff.seller_province 
    END AS seller_province
    ,CASE 
        WHEN pickup.seller_district IS NOT NULL THEN pickup.seller_district
        ELSE dropoff.seller_district 
    END AS seller_district
FROM spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live AS fleet_order
LEFT JOIN 
    (
    SELECT 
        pickup1.pickup_order_id
        ,seller_addr_state AS seller_province
        ,seller_addr_district AS seller_district
        FROM spx_mart.shopee_fms_th_db__pickup_order_tab__th_daily_s0_live pickup1
        INNER JOIN 
        (
        SELECT
            pickup_order_id
            ,MAX(ctime) AS latest_ctime 
        FROM spx_mart.shopee_fms_th_db__pickup_order_tab__th_daily_s0_live
        GROUP BY
            pickup_order_id
        ) AS pickup2
        ON pickup1.pickup_order_id = pickup2.pickup_order_id
        AND pickup1.ctime = pickup2.latest_ctime
    ) pickup
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
)
,delivery_pickup_rc_check as
(
select 
    shipment_id 
    ,pickup_rc_station_name
    ,delivery_rc_station_name
    ,case 
        when pickup_rc_station_name is not null then 1 
        else 0 
    end as is_rc_pickup
    ,case 
        when delivery_rc_station_name is not null then 1 
        else 0 
    end as is_rc_delivery
from thopsbi_lof.dwd_thspx_pub_shipment_info_di_th
)
,pre_agg as
(
select 
    rc_table.shipment_id
    ,case 
        when order_type = 1 then 'CB'
        when order_type = 0 then 'WH' 
        when order_type >= 2 then 'MKP' 
    end as order_type
,is_rc_delivery
,is_rc_pickup
,is_nerca_receive
,total_nerca_cost
,is_nercb_receive
,total_nercb_cost
,is_norca_receive
,total_norca_cost
,is_norcb_receive
,total_norcb_cost
,is_sorca_receive
,total_sorca_cost
,is_sorcb_receive
,total_sorcb_cost
,is_cerc_receive
,total_cerc_cost
,total_rc_cost
from dev_thopsbi_lof.spx_analytics_cost_rc_cpo_jan_weight  as rc_table 
left join delivery_pickup_rc_check
on delivery_pickup_rc_check.shipment_id = rc_table.shipment_id
)
select 
    order_type
    ,cast(sum(total_rc_cost) as double)/sum(case when total_rc_cost > 0 then 1 else 0 end) as cpo_overall_rc 
    ,cast(sum(case when is_rc_delivery = 1 and is_rc_pickup = 1  then total_rc_cost else 0 end) as double)/sum(case when is_rc_delivery = 1 and is_rc_pickup = 1 then 1 else 0 end) as cpo_delivery_and_pickup_rc
    ,cast(sum(case when is_rc_pickup = 0 and is_rc_delivery = 1 then total_rc_cost else 0 end) as double)/sum(case when is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as cpo_delivery_rc
    ,cast(sum(case when  order_type = 'MKP' and is_rc_pickup = 1 and is_rc_delivery = 0 then total_rc_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as cpo_pickup_rc

    -- NERC_A 
    ,cast(sum(case when is_nerca_receive = 1 then total_nerca_cost else 0 end) as double)/sum(case when is_nerca_receive = 1  then 1 else 0 end) as nerca_overall_cpo 
    ,cast(sum(case when is_nerca_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then total_nerca_cost else 0 end) as double)/sum(case when is_nerca_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as nerca_delivery_cpo
    ,cast(sum(case when  order_type = 'MKP' and is_nerca_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then total_nerca_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_nerca_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as nerca_pickup_cpo 

    -- NERC_B
    ,cast(sum(case when is_nercb_receive = 1 then total_nercb_cost else 0 end) as double)/sum(case when is_nercb_receive = 1  then 1 else 0 end) as nercb_overall_cpo 
    ,cast(sum(case when is_nercb_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then total_nercb_cost else 0 end) as double)/sum(case when is_nercb_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as nercb_delivery_cpo
    ,cast(sum(case when  order_type = 'MKP' and is_nercb_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then total_nercb_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_nercb_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as nercb_pickup_cpo 

    -- NORC_A
    ,cast(sum(case when is_norca_receive = 1 then total_norca_cost else 0 end) as double)/sum(case when is_norca_receive = 1  then 1 else 0 end) as norca_overall_cpo
    ,cast(sum(case when is_norca_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then total_norca_cost else 0 end) as double)/sum(case when is_norca_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as norca_delivery_cpo
    ,cast(sum(case when  order_type = 'MKP' and is_norca_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then total_norca_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_norca_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as norca_pickup_cpo 

    -- NORC_B 
    ,cast(sum(case when is_norcb_receive = 1 then total_norcb_cost else 0 end) as double)/sum(case when is_norcb_receive = 1  then 1 else 0 end) as norcb_overall_cpo
    ,cast(sum(case when is_norcb_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then total_norcb_cost else 0 end) as double)/sum(case when is_norcb_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as norcb_delivery_cpo
    ,cast(sum(case when  order_type = 'MKP' and is_norcb_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then total_norcb_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_norcb_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as norcb_pickup_cpo

    -- SORC_A 
    ,cast(sum(case when is_sorca_receive = 1 then total_sorca_cost else 0 end) as double)/sum(case when is_sorca_receive = 1  then 1 else 0 end) as sorca_overall_cpo 
    ,cast(sum(case when is_sorca_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then total_sorca_cost else 0 end) as double)/sum(case when is_sorca_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as sorca_delivery_cpo
    ,cast(sum(case when  order_type = 'MKP' and is_sorca_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then total_sorca_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_sorca_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as sorca_pickup_cpo 

    -- SORC_B
    ,cast(sum(case when is_sorcb_receive = 1 then total_sorcb_cost else 0 end) as double)/sum(case when is_sorcb_receive = 1  then 1 else 0 end) as sorcb_overall_cpo 
    ,cast(sum(case when is_sorcb_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then total_sorcb_cost else 0 end) as double)/sum(case when is_sorcb_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as sorcb_delivery_cpo
    ,cast(sum(case when  order_type = 'MKP' and is_sorcb_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then total_sorcb_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_sorcb_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as sorcb_pickup_cpo 

    -- CERC
    ,cast(sum(case when is_cerc_receive = 1 then total_cerc_cost else 0 end) as double)/sum(case when is_cerc_receive = 1  then 1 else 0 end) as cerc_overall_cpo 
    ,cast(sum(case when is_cerc_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then total_cerc_cost else 0 end) as double)/sum(case when is_cerc_receive = 1 and is_rc_pickup = 0 and is_rc_delivery = 1 then 1 else 0 end) as cerc_delivery_cpo
    ,cast(sum(case when order_type = 'MKP' and is_cerc_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then total_cerc_cost else 0 end) as double)/sum(case when  order_type = 'MKP' and is_cerc_receive = 1 and is_rc_pickup = 1 and is_rc_delivery = 0 then 1 else 0 end) as cerc_pickup_cpo 
from pre_agg
group by 
    order_type
order by 
    order_type desc  




with pre_agg as
( 
select 
    shipment_id
    ,(total_nerca_cost + total_nercb_cost + total_norca_cost + total_norcb_cost + total_sorca_cost + total_sorcb_cost + total_cerc_cost ) as total_rc_cost
    ,(nerca_full_time_staff_cost + nercb_full_time_staff_cost + norca_full_time_staff_cost + norcb_full_time_staff_cost + sorca_full_time_cost + sorcb_full_time_staff_cost + cerc_full_time_staff_cost) as total_full_time_staff_cost
    ,(nerca_temp_staff_cost + nercb_temp_staff_cost + norca_temp_staff_cost + norcb_temp_staff_cost + sorca_temp_staff_cost + sorcb_temp_staff_cost + cerc_temp_staff_cost ) as temp_staff_cost 
    ,(nerca_ot_cost_cost + nercb_ot_cost + norca_ot_cost + norcb_ot_cost + sorca_ot_cost + sorcb_ot_cost + cerc_ot_cost) as total_ot_cost
    ,(nerca_sub_contract_cost + nercb_sub_contract_cost + norca_sub_contract_cost + norcb_sub_contract_cost + sorca_sub_contract_cost + sorcb_sub_contract_cost + cerc_sub_contract_cost) as total_sub_contract_cost 
    ,(nerca_property_cost + nercb_property_cost + norca_property_cost + norcb_property_cost + sorca_property_cost + sorcb_property_cost + cerc_property_cost  ) as property_cost 
    ,(nerca_depreciation_cost + nercb_depreciation_cost + norca_depreciation_cost + norcb_depreciation_cost + sorca_depreciation_cost + sorcb_depreciation_cost + cerc_depreciation_cost) as depreciation_cost 
    ,(nerca_g_and_a_cost + nercb_g_and_a_cost + norca_g_and_a_cost + norcb_g_and_a_cost + sorca_g_and_a_cost + sorcb_g_and_a_cost + cerc_g_and_a_cost ) as total_g_and_a_cost 
    ,(nerca_adhoc_cost + nercb_adhoc_cost + norca_adhoc_cost + norcb_adhoc_cost + sorca_adhoc_cost + sorcb_adhoc_cost + cerc_adhoc_cost ) as adhoc_cost 
    ,(nerca_others_cost + nercb_others_cost + norca_others_cost + norcb_others_cost + sorca_others_cost + sorcb_others_cost + cerc_others_cost ) as others_cost 
    ,is_nerca_receive
    ,total_nerca_cost
    ,nerca_full_time_staff_cost
    ,nerca_temp_staff_cost
    ,nerca_ot_cost_cost
    ,nerca_sub_contract_cost
    ,nerca_property_cost
    ,nerca_depreciation_cost
    ,nerca_g_and_a_cost
    ,nerca_adhoc_cost
    ,nerca_others_cost

    
    ,is_nercb_receive
    ,total_nercb_cost
    ,nercb_full_time_staff_cost
    ,nercb_temp_staff_cost
    ,nercb_ot_cost
    ,nercb_sub_contract_cost
    ,nercb_property_cost
    ,nercb_depreciation_cost
    ,nercb_g_and_a_cost
    ,nercb_adhoc_cost
    ,nercb_others_cost


    ,is_norca_receive
    ,total_norca_cost
    ,norca_temp_staff_cost
    ,norca_full_time_staff_cost
    ,norca_ot_cost
    ,norca_sub_contract_cost
    ,norca_property_cost
    ,norca_depreciation_cost
    ,norca_g_and_a_cost
    ,norca_adhoc_cost
    ,norca_others_cost


    ,is_norcb_receive
    ,total_norcb_cost
    ,norcb_full_time_staff_cost
    ,norcb_temp_staff_cost
    ,norcb_ot_cost
    ,norcb_sub_contract_cost
    ,norcb_property_cost
    ,norcb_depreciation_cost
    ,norcb_g_and_a_cost
    ,norcb_adhoc_cost
    ,norcb_others_cost


    ,is_sorca_receive
    ,total_sorca_cost
    ,sorca_temp_staff_cost
    ,sorca_full_time_cost
    ,sorca_ot_cost
    ,sorca_sub_contract_cost
    ,sorca_property_cost
    ,sorca_depreciation_cost
    ,sorca_g_and_a_cost
    ,sorca_adhoc_cost
    ,sorca_others_cost

   
    ,is_sorcb_receive
    ,total_sorcb_cost
    ,sorcb_temp_staff_cost
    ,sorcb_full_time_staff_cost
    ,sorcb_ot_cost
    ,sorcb_sub_contract_cost
    ,sorcb_property_cost
    ,sorcb_depreciation_cost
    ,sorcb_g_and_a_cost
    ,sorcb_adhoc_cost
    ,sorcb_others_cost

    
    ,is_cerc_receive
    ,total_cerc_cost
    ,cerc_temp_staff_cost
    ,cerc_full_time_staff_cost
    ,cerc_ot_cost
    ,cerc_sub_contract_cost
    ,cerc_property_cost
    ,cerc_depreciation_cost
    ,cerc_g_and_a_cost
    ,cerc_adhoc_cost
    ,cerc_others_cost



    

from dev_thopsbi_lof.spx_analytics_cost_rc_cpo_jan 
)
select 
    -- cost 

    sum(total_rc_cost) as total_rc_cost 
    ,sum(total_full_time_staff_cost) as total_full_time_staff_cost
    ,sum(temp_staff_cost) as temp_staff_cost
    ,sum(total_ot_cost) as total_ot_cost
    ,sum(total_sub_contract_cost) as total_sub_contract_cost
    ,sum(property_cost)  as property_cost
    ,sum(depreciation_cost) as depreciation_cost
    ,sum(total_g_and_a_cost) as total_g_and_a_cost
    ,sum(adhoc_cost) as adhoc_cost
    ,sum(others_cost) as others_cost

    ,sum(total_nerca_cost) as total_nerca_cost
    ,sum(nerca_full_time_staff_cost) as nerca_full_time_staff_cost
    ,sum(nerca_temp_staff_cost) as nerc_temp_staff_cost
    ,sum(nerca_ot_cost_cost) as nerca_ot_cost
    ,sum(nerca_sub_contract_cost) as nerca_sub_contract_cost
    ,sum(nerca_property_cost) as nerca_property_cost
    ,sum(nerca_depreciation_cost) as nerca_depreciation_cost
    ,sum(nerca_g_and_a_cost) as nerca_g_and_a_cost
    ,sum(nerca_adhoc_cost) as nerca_adhoc_cost
    ,sum(nerca_others_cost) as nerca_others_cost

    ,sum(total_nercb_cost) as total_nercb_cost
    ,sum(nercb_full_time_staff_cost) as nercb_full_time_staff_cost
    ,sum(nercb_temp_staff_cost) as nercb_temp_staff_cost
    ,sum(nercb_ot_cost) as nercb_ot_cost
    ,sum(nercb_sub_contract_cost)  as nercb_sub_contract_cost
    ,sum(nercb_property_cost) as nercb_property_cost
    ,sum(nercb_depreciation_cost) as nercb_depreciation_cost
    ,sum(nercb_g_and_a_cost) as nercb_g_and_a_cost
    ,sum(nercb_adhoc_cost) as nercb_adhoc_cost
    ,sum(nercb_others_cost) as nercb_others_cost

    ,SUM(total_norca_cost) as total_norca_cost
    ,SUM(norca_full_time_staff_cost) as norca_full_time_staff_cost
    ,sum(norca_temp_staff_cost) as norca_temp_staff_cost
    ,sum(norca_ot_cost) as norca_ot_cost
    ,sum(norca_sub_contract_cost) as norca_sub_contract_cost
    ,sum(norca_property_cost) as norca_property_cost
    ,sum(norca_depreciation_cost) as norca_depreciation_cost
    ,sum(norca_g_and_a_cost) as norca_g_and_a_cost
    ,sum(norca_adhoc_cost) as norca_adhoc_cost
    ,sum(norca_others_cost) as norca_others_cost

    ,sum(total_norcb_cost) as total_norcb_cost
    ,sum(norcb_full_time_staff_cost) as norcb_full_time_staff_cost 
    ,sum(norcb_temp_staff_cost) as norcb_temp_staff_cost
    ,sum(norcb_ot_cost) as norcb_ot_cost
    ,sum(norcb_sub_contract_cost) as norcb_sub_contract_cost
    ,sum(norcb_property_cost) as norcb_property_cost 
    ,sum(norcb_depreciation_cost) as norcb_depreciation_cost
    ,sum(norcb_g_and_a_cost) as norcb_g_and_a_cost
    ,sum(norcb_adhoc_cost) as norcb_adhoc_cost
    ,sum(norcb_others_cost) as norcb_others_cost

    ,sum(total_sorca_cost) as total_sorca_cost
    ,sum(sorca_full_time_cost) as sorca_full_time_cost
    ,sum(sorca_temp_staff_cost) as sorca_temp_staff_cost
    ,sum(sorca_ot_cost) assorca_ot_cost
    ,sum(sorca_sub_contract_cost) as sorca_sub_contract_cost
    ,sum(sorca_property_cost) as sorca_property_cost
    ,sum(sorca_depreciation_cost) as sorca_depreciation_cost
    ,sum(sorca_g_and_a_cost) as sorca_g_and_a_cost
    ,sum(sorca_adhoc_cost) as sorca_adhoc_cost
    ,sum(sorca_others_cost) as sorca_others_cost

    ,sum(total_sorcb_cost) as total_sorcb_cost
    ,sum(sorcb_full_time_staff_cost) as sorcb_full_time_staff_cost
    ,sum(sorcb_temp_staff_cost) as sorcb_temp_staff_cost
    ,sum(sorcb_ot_cost) as sorcb_ot_cost
    ,sum(sorcb_sub_contract_cost) as sorcb_sub_contract_cost
    ,sum(sorcb_property_cost) as sorcb_property_cost
    ,sum(sorcb_depreciation_cost) as sorcb_depreciation_cost
    ,sum(sorcb_g_and_a_cost) as sorcb_g_and_a_cost
    ,sum(sorcb_adhoc_cost) as sorcb_adhoc_cost
    ,sum(sorcb_others_cost) as sorcb_others_cost

    ,sum(total_cerc_cost) as total_cerc_cost
    ,sum(cerc_full_time_staff_cost) as cerc_full_time_staff_cost
    ,sum(cerc_temp_staff_cost) as cerc_temp_staff_cost
    ,sum(cerc_ot_cost) as cerc_ot_cost
    ,sum(cerc_sub_contract_cost) as cerc_sub_contract_cost
    ,sum(cerc_property_cost) as cerc_property_cost
    ,sum(cerc_depreciation_cost) as cerc_depreciation_cost
    ,sum(cerc_g_and_a_cost) as cerc_g_and_a_cost
    ,sum(cerc_adhoc_cost) as cerc_adhoc_cost
    ,sum(cerc_others_cost) as cerc_others_cost

    --cpo 
    ,sum(total_rc_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as rc_cpo 
    ,sum(total_full_time_staff_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as full_time_staff_cpo 
    ,sum(temp_staff_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as temp_staff_cpo 
    ,sum(total_ot_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as total_ot_cost 
    ,sum(total_sub_contract_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as total_sub_contract_cpo
    ,sum(property_cost)/sum(case when total_rc_cost>0 then 1 else 0 end)  as property_cpo
    ,sum(depreciation_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as depreciation_cpo
    ,sum(total_g_and_a_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as total_g_and_a_cpo
    ,sum(adhoc_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as adhoc_cpo
    ,sum(others_cost)/sum(case when total_rc_cost>0 then 1 else 0 end) as others_cpo

    ,sum(total_nerca_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_cpo  
    ,sum(nerca_full_time_staff_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_full_time_staff_cpo 
    ,sum(nerca_temp_staff_cost)/sum(case when  is_nerca_receive = 1 then 1 else 0 end) as nerca_temp_staff_cpo 
    ,sum(nerca_ot_cost_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_ot_cpo 
    ,sum(nerca_sub_contract_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_sub_contract_cpo 
    ,sum(nerca_property_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_property_cpo  
    ,sum(nerca_depreciation_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_depreciation_cpo 
    ,sum(nerca_g_and_a_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_g_and_a_cpo 
    ,sum(nerca_adhoc_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_adhoc_cpo
    ,sum(nerca_others_cost)/sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerca_others_cpo 
    
    ,sum(total_nercb_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_cpo
    ,sum(nercb_full_time_staff_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_full_time_staff_cost
    ,sum(nercb_temp_staff_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_temp_staff_cpo 
    ,sum(nercb_ot_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_ot_cpo 
    ,sum(nercb_sub_contract_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end)  as nercb_sub_contract_cpo 
    ,sum(nercb_property_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_property_cpo
    ,sum(nercb_depreciation_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_depreciation_cpo 
    ,sum(nercb_g_and_a_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_g_and_a_cpo 
    ,sum(nercb_adhoc_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_adhoc_cpo 
    ,sum(nercb_others_cost)/sum(case when is_nercb_receive = 1 then 1 else 0 end) as nercb_others_cpo

   
    ,SUM(total_norca_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_cpo 
    ,SUM(norca_full_time_staff_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_full_time_staff_cpo 
    ,sum(norca_temp_staff_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_temp_staff_cpo 
    ,sum(norca_ot_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_ot_cpo 
    ,sum(norca_sub_contract_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_sub_contract_cpo 
    ,sum(norca_property_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_property_cpo 
    ,sum(norca_depreciation_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_depreciation_cpo 
    ,sum(norca_g_and_a_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_g_and_a_cpo 
    ,sum(norca_adhoc_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_adhoc_cpo 
    ,sum(norca_others_cost)/sum(case when is_norca_receive = 1 then 1 else 0 end) as norca_others_cpo 


    ,sum(total_norcb_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_cpo 
    ,sum(norcb_full_time_staff_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_full_time_staff_cpo 
    ,sum(norcb_temp_staff_cost)/sum(case when is_norcb_receive = 1  then 1 else 0 end) as norcb_temp_staff_cpo 
    ,sum(norcb_ot_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_ot_cpo 
    ,sum(norcb_sub_contract_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_sub_contract_cpo 
    ,sum(norcb_property_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_property_cpo 
    ,sum(norcb_depreciation_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_depreciation_cpo 
    ,sum(norcb_g_and_a_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_g_and_a_cpo 
    ,sum(norcb_adhoc_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_adhoc_cpo 
    ,sum(norcb_others_cost)/sum(case when is_norcb_receive = 1 then 1 else 0 end) as norcb_others_cpo 

    ,sum(total_sorca_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_cpo 
    ,sum(sorca_full_time_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_full_time_cpo 
    ,sum(sorca_temp_staff_cost)/sum(case when is_sorca_receive = 1  then 1 else 0 end) as sorca_temp_staff_cpo 
    ,sum(sorca_ot_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_ot_cpo 
    ,sum(sorca_sub_contract_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_sub_contract_cpo 
    ,sum(sorca_property_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_property_cpo 
    ,sum(sorca_depreciation_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_depreciation_cpo 
    ,sum(sorca_g_and_a_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_g_and_a_cpo 
    ,sum(sorca_adhoc_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_adhoc_cpo 
    ,sum(sorca_others_cost)/sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorca_others_cpo 

    ,sum(total_sorcb_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_cpo 
    ,sum(sorcb_full_time_staff_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_full_time_staff_cpo
    ,sum(sorcb_temp_staff_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_temp_staff_cpo  
    ,sum(sorcb_ot_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_ot_cpo
    ,sum(sorcb_sub_contract_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_sub_contract_cpo 
    ,sum(sorcb_property_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_property_cpo 
    ,sum(sorcb_depreciation_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_depreciation_cpo 
    ,sum(sorcb_g_and_a_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_g_and_a_cpo 
    ,sum(sorcb_adhoc_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_adhoc_cpo 
    ,sum(sorcb_others_cost)/sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorcb_others_cpo 

    ,sum(total_cerc_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as total_cerc_cost
    ,sum(cerc_full_time_staff_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_full_time_staff_cost
    ,sum(cerc_temp_staff_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_temp_staff_cpo  
    ,sum(cerc_ot_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_ot_cpo 
    ,sum(cerc_sub_contract_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_sub_contract_cost
    ,sum(cerc_property_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_property_cost
    ,sum(cerc_depreciation_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_depreciation_cost
    ,sum(cerc_g_and_a_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_g_and_a_cost
    ,sum(cerc_adhoc_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_adhoc_cost
    ,sum(cerc_others_cost)/sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_others_cost

    --volume
    ,sum(case when is_nerca_receive = 1 then 1 else 0 end) as nerc_a_volume 
    ,sum(case when is_nercb_receive = 1 then 1 else 0 end) as nerc_b_volume 
    ,sum(case when is_norca_receive = 1 then 1 else 0 end) as norc_a_volume 
    ,sum(case when is_norcb_receive = 1 then 1 else 0 end) as norc_b_volume 
    ,sum(case when is_sorca_receive = 1 then 1 else 0 end) as sorc_a_volume 
    ,sum(case when is_sorcb_receive = 1 then 1 else 0 end) as sorc_b_volume 
    ,sum(case when is_cerc_receive = 1 then 1 else 0 end) as cerc_volume

    


from pre_agg






