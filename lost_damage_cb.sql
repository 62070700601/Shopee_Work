/*
วันที่เป็น damaged lost order type  cb ของเดือน 8 กับ 9 ที่เป็นของ indonesia 
*/
with lost_damage_track_ID as 
(
select 
    fleet_order.shipment_id
    ,fleet_order.status
    ,lost_damage_time as timestamp_status
    ,case 
        when fleet_order.status = 11 then 'Lost'
        when fleet_order.status = 12 then 'Damaged'
        when fleet_order.status = 26 then 'Disposed'
    end as "status_type"
    ,origin_region
    ,order_type_name
from 
    (
    select 
        fleet_order.shipment_id
        ,fleet_order.status
        ,FROM_UNIXTIME(fleet_order.ctime-3600) as lost_damage_time
        ,origin_region
        ,row_number() over (partition by fleet_order.shipment_id order by FROM_UNIXTIME(fleet_order.ctime-3600) desc) as rank_num
        ,pub_shipment.order_type_name
        -- fleet_order.*
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as fleet_order
    left join sls_mart.dwd_sls_cdt_order_detail_info_df_th as region_country
    on fleet_order.shipment_id = region_country.lm_tn
    left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
    on fleet_order.shipment_id = pub_shipment.shipment_id 
    where 
        FROM_UNIXTIME(fleet_order.ctime-3600) between date('2022-10-01 00:00:00.000') and date('2022-10-31 23:59:59.000')
        and status in (11,12,26)
        and pub_shipment.order_type_name = 'CB'
    ) fleet_order
where 
    rank_num = 1
    and status in (11,12,26)
    -- and origin_region in ('ID','CN')
    and lost_damage_time between date('2022-10-01 00:00:00.000') and date('2022-10-31 23:59:59.000')
group by 
    fleet_order.shipment_id
    ,fleet_order.status
    ,lost_damage_time
    ,case 
        when fleet_order.status = 11 then 'Lost'
        when fleet_order.status = 12 then 'Damaged'
        when fleet_order.status = 26 then 'Disposed'
    end
    ,origin_region
    ,order_type_name
order by 
    lost_damage_time
)
