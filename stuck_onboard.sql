-- status index : https://docs.google.com/spreadsheets/d/1Afbu965rBwgeyd1PPolWqs4WyHYGBrK5TyRwi6lY2_k/edit#gid=1745763950
-- fleet_order : spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live

-- r2 and r3 
select 
    fleet_order.shipment_id
    ,croosdock_track.is_4pl 
    ,case 
        when exceptional_time is not null and  croosdock_track.is_4pl = true then 'r3'
        when exceptional_time is not null then 'r2'
    end as r2_r3_type 
    ,r2_r3_destination
    ,wrong_destination
    ,date(exceptional_time) as exceptional_date 
from thopsbi_spx.dwd_soc_shipment_info_df_th as fleet_order 
left join 
    (
    select 
        shipment_id
        ,status_time as exceptional_time 
        ,r2_destination as r2_r3_destination
        ,wrong_destination
    from 
        (
        select 
            shipment_id
            ,from_unixtime(ctime-3600) as status_time 
            ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) as r2_destination 
            ,try(cast(json_extract(json_parse(content),'$.pickup_station_name') as varchar)) as wrong_destination  
            ,row_number() over(partition by shipment_id order by ctime desc) as row_number 
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status in (82,83)
        )
    where 
        row_number = 1 
        and date(status_time) between current_date - interval '30' day and current_date - interval '1' day and r2_destination like 'SOC_'
    ) as r2_r3_track
on fleet_order.shipment_id = r2_r3_track.shipment_id
left join thopsbi_spx.dwd_4pl_shipment_info_di_th as croosdock_track 
on croosdock_track.shipment_id = fleet_order.shipment_id
where 
    date(exceptional_time) between current_date - interval '30' day and current_date - interval '1' day
-- weird route : SPXTH022090989317
-- order tracking 
select 
    shipment_id 
    ,status
    ,from_unixtime(ctime-3600) as status_time 
    ,ctime
    ,station_id
    ,operator
    ,content 
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
where 
    shipment_id = 'SPXTH022069639617'
order by 
    ctime 
-- extract content 
select 
    shipment_id 
    ,status
    ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) as destination_station 
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
where 
    shipment_id = 'SPXTH022069639617' 
--- select first status using min 
select 
    shipment_id
    ,min(case when status = 8 then from_unixtime(ctime-3600) end) as soc_received_time 
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
where 
    shipment_id = 'SPXTH022069639617'
group by 
    shipment_id
--- select last operator using operator 
select 
    shipment_id 
    ,operator
    ,row_number
from 
    (
    select 
        shipement_id 
        ,operator
        ,row_number() over(partition by shipment_id order by ctime desc) as row_number
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    )
where 
    row_number = 1 
-- check column data mart 
select 
    shipment_id
    ,latest_status_name
    ,latest_operator_name
    ,latest_station_id
    ,latest_awb_station_name
from thopsbi_spx.dwd_gen_shipment_info_di_th
where 
    shipment_id = 'SPXTH022069639617'
-- All column data_mart
 select 
      
    -- shipment 
    shipment_id 
    ,sls_tracking_no

    -- last status
    ,latest_status_name
    ,latest_operator_name
    ,latest_station_id
    ,latest_awb_station_name

    -- order path 
    ,origin_order_path
    ,updated_order_path
    ,shipment_inbound_route_type
    ,shipment_outbound_route_type

    -- fm 
    ,updated_order_path_fm_hub_station_name
    -- rc pickup
    ,updated_order_path_pickup_rc_station_name
    ,updated_order_path_pickup_rc_station_id

    -- soc station 
    ,updated_order_path_pickup_soc_station_name -- soc inbound
    ,updated_order_path_delivery_soc_station_id
    
    ,updated_order_path_delivery_soc_station_name -- soc outbound 
    ,updated_order_path_delivery_soc_station_id

    -- rc delivery
    ,updated_order_path_delivery_rc_station_name
    ,updated_order_path_delivery_rc_station_id

    -- lm hub 
    ,updated_order_path_lm_hub_station_name

    -- flag check 
    ,order_type_name
    ,is_marketplace
    ,is_bulky
    ,is_cross_border
    ,is_warehouse
    ,is_open_service
    ,is_sip
    ,is_delivered_in_serviceable_area

    -- parcel info
    ,payment_method_name
    ,cogs_amount
    ,package_weight_in_kg
    ,manual_package_weight_in_kg
    ,manual_package_length_in_cm
    ,manual_package_width_in_cm
    ,manual_package_height_in_cm

    ,sc_weight_in_kg
    ,sls_package_weight_in_kg
    ,sls_package_length_in_cm
    ,sls_package_width_in_cm
    ,sls_package_height_in_cm

    ,shipment_pricing_amount
    ,cod_amount

    ,seller_name
    ,seller_address
    ,seller_district_name
    ,seller_province_name
    ,seller_region_name
    ,buyer_name
    ,buyer_address
    ,buyer_region_name
    ,buyer_area_name

    -- lost 
    ,lost_timestamp
    ,lost_station_name

    -- damage
    ,damaged_timestamp
    ,damaged_station_name


from thopsbi_spx.dwd_gen_shipment_info_di_th 
where 
    shipment_id = 'SPXTH022069639617'

 -- SOC column data 
select 
    shipment_id 
    ,first_soc_station_name

    --rec 
    ,first_soc_received_timestamp
    ,first_soc_received_operator_name

    -- pack 
    ,first_soc_packing_timestamp
    ,first_soc_packed_timestamp
    ,first_soc_packed_operator_name

    -- lh pack 
    ,first_soc_lh_packing_timestamp
    ,first_soc_lh_packed_timestamp
    ,first_soc_lh_packed_operator_name



from thopsbi_spx.dwd_soc_shipment_info_di_th
where 
    shipment_id = 'SPXTH022069639617'

