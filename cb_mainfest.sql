WITH station AS 
(
SELECT
    CAST(station_tab.id AS INTEGER) AS id
    ,TRY(SPLIT(station_name,' ')[1]) AS station_name
    ,station_type
    ,CASE station_type
        WHEN 0 THEN 'Admin'
        WHEN 2 THEN 'SOC'
        WHEN 3 THEN 'LM'
        WHEN 5 THEN '4PL'
        WHEN 7 THEN 'FM'
        WHEN 9 THEN 'DOP'
    END AS station_type_name
FROM spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live station_tab
)
,latest_status AS 
(
    SELECT
        shipment_id
        ,status AS latest_status
        ,status_name.status_name
        ,latest_timestamp
        ,station_id
        ,station_name
        ,operator
    FROM 
    (
        SELECT
            shipment_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) AS latest_timestamp
            ,station_id
            ,operator
            ,ROW_NUMBER() OVER(PARTITION BY shipment_id ORDER BY ctime DESC) AS rank_num
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        WHERE 
            shipment_id LIKE 'TH%' OR shipment_id LIKE 'SPXTH%'
    ) as latest_status_track
    LEFT JOIN station 
    ON latest_status_track.station_id = station.id
    LEFT JOIN 
    (
    SELECT
        CAST(status_id AS INTEGER) AS status_id
        ,status_name
    FROM dev_thopsbi_lof.spx_analytics_status_map_v2 AS sm1
    ) as status_name 
    on status_name.status_id = latest_status_track.status
    WHERE 
        rank_num = 1
)
select
fleet_order.shipment_id
,fleet_order.sls_tracking_number
,container_manifest_ext_tab.manifest_task_id
,latest_status.status_name as latest_status
,order_track.receive_time as first_soc_received_time
,order_track.receive_operator
,case 
    when return_remark.remark is not null then remark
    when status_count.buyer_reschedule >= 3 then 'buyer_reschedule' -- r1
    when status_count.cant_contact_buyer_arrival >= 3 then 'buyer_arrival'  -- r1
    when status_count.cant_contact_buyer >= 3 then 'cant_contact_buyer' -- r1
    when status_count.number_not_in_service >= 3 then 'contact_not_in_service' -- r1 
    when status_count.addr_is_incorrect = 1 then 'addr_incorrect' -- r1 
    when status_count.addr_is_incomplete = 1 then 'addr_incomplete' -- r1
    when status_count.insufficient_time >=3 then 'insufficient_time' -- r1 
    when status_count.reject_by_receipient = 1 then 'recipient_reject' -- r1 
    when status_count.receipt_never_place_order = 1 then 'never_place_order' -- r1 
    when status_count.Invalid_condition = 1 then 'invalid_condition' -- r1 
    when status_count.traffic_accident >=3 then 'traffic_accident' --r1 
    when status_count.office_schedule_for_weekday >=3 then 'office_reschedule' --r1 
    when status_count.out_of_serviceable_area = 1 then 'out_of_serviceable_area' -- r1 
else 'non_sop_return_order' end as to_be_return
from  spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live  as fleet_order
left join 
    (
    select
        fleet_order_id
        ,container_no
    from
        (
        select 
            fleet_order_id
            ,container_no
            ,ROW_NUMBER() over(partition by fleet_order_id order by ctime desc ) as ROW_NUMBER 
        from spx_mart.ods_shopee_spx_container_th_db__container_order_tab_df_th 
        ) 
    where ROW_NUMBER = 1
    ) as container_order_tab
on fleet_order.shipment_id = container_order_tab.fleet_order_id
left join spx_mart.shopee_spx_container_th_db__container_manifest_ext_tab__reg_continuous_s0_live as container_manifest_ext_tab
on container_order_tab.container_no = container_manifest_ext_tab.container_no
left join latest_status
on latest_status.shipment_id = fleet_order.shipment_id
left join 
    (
    select 
        shipment_id
        ,receive_time
        ,operator as receive_operator
    
    from 
        (
        select 
            shipment_id
            ,from_unixtime(ctime-3600) as receive_time 
            ,operator
            ,ROW_NUMBER() over(partition by shipment_id order by ctime) as ROW_NUMBER 
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
        where 
            status = 8 
            and station_id in (3,242)
        )
    where  ROW_NUMBER = 1
    ) as order_track
on order_track.shipment_id = fleet_order.shipment_id
left join 
    (
    select 
        shipment_id
        ,return_soc_returned
    from 
        (
        select 
            shipment_id
            ,from_unixtime(ctime-3600) as return_soc_returned
            ,ROW_NUMBER() over(partition by shipment_id order by ctime) as ROW_NUMBER 
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
        where 
            status = 6 
            and station_id = 3
        )
    where
        ROW_NUMBER = 1
    ) as return_soc_returned
on return_soc_returned.shipment_id = fleet_order.shipment_id
left join 
    (
    select
        shipment_id
        ,count( case when status in (80,5) then shipment_id END) as on_hold
        ,count( case when status in (80,5) and on_hold_reason = 17 then shipment_id END) as buyer_reschedule 
        ,count( case when status in (80,5) and on_hold_reason = 18 then shipment_id END) as cant_contact_buyer_arrival
        ,count( case when status in (80,5) and on_hold_reason = 19 then shipment_id END) as cant_contact_buyer
        ,count( case when status in (80,5) and on_hold_reason = 20 then shipment_id END) as number_not_in_service 
        ,count( case when status in (80,5) and on_hold_reason = 21 then shipment_id END) as addr_is_incorrect
        ,count(case when status in (80,5) and on_hold_reason in (22,23) then shipment_id END) as addr_is_incomplete
        ,count( case when status in (80,5) and on_hold_reason = 24 then shipment_id END) as insufficient_time
        ,count( case when status in (80,5) and on_hold_reason = 25 then shipment_id END) as reject_by_receipient 
        ,count( case when status in (80,5) and on_hold_reason = 26 then shipment_id END) as receipt_never_place_order  
        ,count( case when status in (80,5) and on_hold_reason = 27 then shipment_id END) as Invalid_condition
        ,count( case when status in (80,5) and on_hold_reason = 31 then shipment_id END) as traffic_accident 
        ,count( case when status in (80,5) and on_hold_reason = 32 then shipment_id END) as island_delivery_delay 
        ,count( case when status in (80,5) and on_hold_reason = 33 then shipment_id END) as office_schedule_for_weekday
        ,count( case when status in (80,5) and on_hold_reason = 34 then shipment_id END) as missorted_parcel  
        ,count( case when status in (80,5) and on_hold_reason = 55 then shipment_id END) as out_of_serviceable_area
        ,count( case when status in (80,5) and on_hold_reason = 28 then shipment_id END) as damaged_parcel      
    from spx_mart.shopee_fms_th_db__order_tracking_tab__th_continuous_s0_live 
    group by 
        shipment_id 
    ) as status_count
on status_count.shipment_id = fleet_order.shipment_id
left join 
    (
    select 
        order_id
        ,min(remark) as remark
    from spx_mart.shopee_fms_th_db__abnormal_order_record_tab__reg_continuous_s0_live as return_remark 
    where 
        remark != ''
    group by 
        order_id 
    ) as return_remark
on return_remark.order_id = fleet_order.shipment_id
where 
    fleet_order.shipment_id = 'SPXTH022897796444'

--SPXTH029295956735

