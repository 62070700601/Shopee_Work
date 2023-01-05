with lh_raw_shipment as
(
select 
    shipment_id 
    ,buyer_region_name
    ,lh_report_month
    ,lh_is_route_type_direct 
    ,lh_is_route_type_shuttle 
    ,lh_is_route_type_transit 
    ,lh_direct_cost 
    ,lh_shuttle_cost 
    ,lh_transit_cost 
    ,lh_am_cost 
    ,lh_reverse_cost
    ,lh_adhoc_cost 
    ,lh_unused_cost 
    ,lh_other_cost 
    ,lh_cpo_total
from dev_thopsbi_lof.spx_analytics_all_function_cpo_v4 as v4
where 
    lh_report_month = date('2022-06-01') 
    and partition_date = date('2022-06-03')
)
select 
    lh_report_month
    ,buyer_region_name
    ,count(*) as total_volume 
    ,1.00*count(*)/30 as ado
    ,sum(lh_cpo_total) as lh_cpo_total
    ,sum(lh_direct_cost) as lh_direct_cost
    ,sum(lh_shuttle_cost) as lh_shuttle_cost
    ,sum(lh_transit_cost) as lh_transit_cost
    ,sum(lh_am_cost) as lh_am_cost 
    ,sum(lh_reverse_cost) as lh_reverse_cost
    ,sum(lh_adhoc_cost) as lh_adhoc_cost 
    ,sum(lh_unused_cost) as lh_unused_cost  
    ,sum(lh_other_cost) as lh_other_cost 
    ,1.00*sum(lh_cpo_total)/count(*) as lh_cpo_total
    ,1.00*sum(lh_direct_cost)/count(*) as lh_direct_cost
    ,1.00*sum(lh_shuttle_cost)/count(*) as lh_shuttle_cost
    ,1.00*sum(lh_transit_cost)/count(*) as lh_transit_cost
    ,1.00*sum(lh_am_cost)/count(*) as lh_am_cost 
    ,1.00*sum(lh_reverse_cost)/count(*) as lh_reverse_cost
    ,1.00*sum(lh_adhoc_cost)/count(*) as lh_adhoc_cost 
    ,1.00*sum(lh_unused_cost)/count(*) as lh_unused_cost  
    ,1.00*sum(lh_other_cost)/count(*) as lh_other_cost 
from  lh_raw_shipment
group by 
    lh_report_month
    ,buyer_region_name 
order by 
    lh_report_month
    ,buyer_region_name 
