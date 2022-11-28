/*
1.นับจำนวน seller_reigion_name ว่าแต่ละภูมิภาคนี้มันส่งไป buyer_region_name ไหนบ้าง
2.order type = > mkp,osv
3.first_attempt_pickup_timestamp => 1 aug - 31 aug
4.column ที่ใช้
    seller_region_name
    seller_province_name
    buyer_region_name
    buyer_province_name
    order_type_name
ใน Order tracking 
    FMHub_Pickup_Onhold = 37
    FMHub_Pickup_done = 39
5.ตัวอย่าง shipment
    SPXTH02040911569A
-- select *
-- from thopsbi_spx.dwd_pub_shipment_info_df_th
-- limit 1000
*/
with FMHub_Pickup_Onhold_Done_time as 
(
    select 
        *
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as status_time
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as fleet_order
        where 
            status in (2,91)
    )
    -- where   
    --     date(status_time) between date('2022-10-01 00:00:00.000') and date('2022-10-31 23:59:59.000')
    --     row_num = 1
)
,pub_shipment as 
(
    select  
        seller_region_name
        -- ,seller_province_name
        ,buyer_region_name
        -- ,order_type_name
        ,count(buyer_region_name) as count_region
        -- ,buyer_province_name
    from thopsbi_spx.dwd_pub_shipment_info_df_th
    left join FMHub_Pickup_Onhold_Done_time
    on thopsbi_spx.dwd_pub_shipment_info_df_th.shipment_id = FMHub_Pickup_Onhold_Done_time.shipment_id
    where 
        -- order_type_name in ('NON_SHOPEE_MARKETPLACE_STANDARD','MARKETPLACE')
        date(status_time) between date('2022-10-01 00:00:00.000') and date('2022-10-31 23:59:59.000')
        and seller_region_name is not null 
        and buyer_region_name is not null
    group by 
        seller_region_name
        ,buyer_region_name
        -- ,order_type_name
    order by 
        seller_region_name
        ,buyer_region_name
        -- ,order_type_name
)
select *
from pub_shipment
-- group by 
--     order_type_name




