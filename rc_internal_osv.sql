SELECT *
FROM 
(
select 
    fleet_order.shipment_id
    ,fleet_dtm.is_open_service
    ,create_operator.create_timestamp
    ,case 
        when seller_info.seller_name = 'รวิศ อินทแย้ม' then 783031071
        when seller_info.seller_name = 'นัฐพงษ์ จันต๊ะมา' then 783314250
        when seller_info.seller_name = 'พัชราภรณ์ เป้งสะท้าน' then 783695143
        when seller_info.seller_name = 'จิระนันท์ เจือกโว้น' then 783627969
        when seller_info.seller_name = 'อมรเทพ คงคล้าย' then 783314249
        when seller_info.seller_name = 'รัฐศักดิ์ ปริญญ์วิริยะกุล' then 783360683
        when seller_info.seller_name = 'เจตน์ณรงค์ นวลทอง'  then 783314879
        when seller_info.seller_name = 'มนตรี คำดี' then 783853021
        when seller_info.seller_name =  'ณัฐฆนิล ตวงสิทธินันท์' then 783338064
        when seller_info.seller_name = 'พจน์​ภคิน​ ​รติ​เฉลิม​วงศ์​' then 783624790
    end as account_id  
    ,seller_info.seller_name 
    ,mp_order.item_name
    --,mp_order.sku_id
    ,seller_info.seller_addr as sender_address
    ,buyer_info.buyer_addr as receiver_address
    ,order_type_name as delivery_instruction
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live fleet_order
left join 
    (
    select 
        shipment_id
        ,min(case when status = 0 then from_unixtime(ctime-3600) end ) as create_timestamp
        ,min(case when status = 0 then operator end) as create_operator 
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    group by 
        shipment_id
    ) as create_operator
on create_operator.shipment_id = fleet_order.shipment_id
left join spx_mart.shopee_fleet_order_th_db__seller_info_tab__reg_daily_s0_live as seller_info 
on seller_info.shipment_id = fleet_order.shipment_id
left join thopsbi_spx.dwd_pub_shipment_info_df_th as fleet_dtm
on fleet_order.shipment_id = fleet_dtm.shipment_id
left join mp_order.dwd_order_item_all_ent_df__th_s0_live as mp_order
on mp_order.order_sn = fleet_order.shopee_order_sn
left join spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live as buyer_info 
on buyer_info.shipment_id = fleet_order.shipment_id
)
where 
    date(create_timestamp) between date('2022-04-01') and date('2022-07-20') AND account_id IS NOT NULL AND is_open_service = TRUE

