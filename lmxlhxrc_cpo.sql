with
fact_order as 
(
select  
    lm_detail.report_month
    ,lm_detail.shipment_id
    ,case   
        when shipment_detail.is_cross_border = true then 'CB'
        when shipment_detail.is_bulky = true then 'BK'
        when shipment_detail.is_warehouse = true then 'WH'
        when shipment_detail.is_marketplace = true then 'MP'
        when shipment_detail.is_open_service = true then 'OS'
        else 'MP'
    end as order_type
    ,case   
        when shipment_detail.is_overflow = true then '4PL'
        else 'E2E'
    end as shipment_type
    ,shipment_detail.seller_area_name
    ,shipment_detail.buyer_area_name
    ,all_function.lm_delivered_date 
    ,lm_detail.weight_tier
    ,lm_detail.district_tier
    ,try(split(lm_detail.station_name, ' ')[1]) as station_name
    ,lm_detail.province_name
    ,lm_detail.district_name
    ,lm_detail.driver_type
    ,lm_detail.contract_type
    ,lm_detail.weight_payout
    ,lm_detail.cpo_bonus
    ,lm_detail.cpo_goodboy
    ,lm_detail.cpo_extra
    ,lm_detail.cpo_discipline
    ,lm_detail.cpo_salary
    ,lm_detail.cpo_subcon_incentive
    ,lm_detail.cpo_subcon_daily_wage
    ,lm_detail.cpo_ic_cost
    ,lm_detail.cpo_hub_rental
    ,lm_detail.cpo_lm_hub_people
    ,lm_detail.cpo_total
    ,lh_detail.total_lh_cost as lh_cpo_total
    ,lh_detail.reverse_cost
from dev_thopsbi_lof.spx_analytics_cost_lm_cpo as lm_detail
left join thopsbi_spx.dwd_pub_shipment_info_di_th as shipment_detail
on lm_detail.shipment_id = shipment_detail.shipment_id
left join dev_thopsbi_lof.spx_analytics_cost_lh_cpo_v1 as lh_detail
on lh_detail.shipment_id = lm_detail.shipment_id
left join dev_thopsbi_lof.spx_analytics_all_function_cpo_v4 as all_function 
on all_function.shipment_id = lm_detail.shipment_id
)
,agg_detail as 
(
select  
    report_month
    ,shipment_type
    ,order_type
    ,seller_area_name
    ,buyer_area_name
    ,district_tier as buyer_district_tier
    ,province_name
    ,district_name
    ,station_name
    ,driver_type
    ,contract_type
    ,weight_tier
    ,count(shipment_id) as delivered_order
    ,sum(weight_payout) as weight_payout
    ,sum(cpo_bonus) as cpo_bonus
    ,sum(cpo_goodboy) as cpo_goodboy
    ,sum(cpo_extra) as cpo_extra
    ,sum(cpo_discipline) as cpo_discipline
    ,sum(cpo_salary) as cpo_salary
    ,sum(cpo_subcon_incentive) as cpo_subcon_incentive
    ,sum(cpo_subcon_daily_wage) as cpo_subcon_daily_wage
    ,sum(cpo_ic_cost) as cpo_ic_cost
    ,sum(cpo_hub_rental) as cpo_hub_rental
    ,sum(cpo_lm_hub_people) as cpo_lm_hub_people
from fact_order
where   
    report_month = '2022-05' 
group by 
    report_month
    ,shipment_type
    ,order_type
    ,seller_area_name
    ,buyer_area_name
    ,district_tier
    ,province_name
    ,district_name
    ,station_name
    ,driver_type
    ,contract_type
    ,weight_tier
order by 
    report_month asc
    ,shipment_type asc
    ,order_type asc
    ,seller_area_name asc
    ,buyer_area_name asc
    ,district_tier asc
    ,province_name asc
    ,district_name asc
    ,station_name asc
    ,driver_type asc
    ,contract_type asc
    ,weight_tierca asc
)
------------------------------- LM by district tier ------------------------------- 
select 
    report_month
    ,shipment_type
    ,order_type
    ,district_tier
    ,count(shipment_id) as delivered_order
    ,sum(weight_payout) as weight_payout
    ,sum(cpo_bonus) as cpo_bonus
    ,sum(cpo_goodboy) as cpo_goodboy
    ,sum(cpo_extra) as cpo_extra
    ,sum(cpo_discipline) as cpo_discipline
    ,sum(cpo_salary) as cpo_salary
    ,sum(cpo_subcon_incentive) as cpo_subcon_incentive
    ,sum(cpo_subcon_daily_wage) as cpo_subcon_daily_wage
    ,sum(cpo_ic_cost) as cpo_ic_cost
    ,sum(cpo_hub_rental) as cpo_hub_rental
    ,sum(cpo_lm_hub_people) as cpo_lm_hub_people
    ,sum(cpo_total) as cpo_total
    ,sum(cpo_total) as cpo_total
    ,sum(lh_cpo_total) - sum(reverse_cost) as lh_cpo_exclude_pu
from    fact_order
where   
    report_month = '2022-05' 
    and lm_delivered_date between  date('2022-05-05') and date('2022-05-25')
group by 
    report_month
    ,shipment_type
    ,order_type
    ,district_tier
order by 
    report_month asc
    ,shipment_type asc
    ,order_type asc
    ,district_tier asc