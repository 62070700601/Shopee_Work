----- return dashboard 
WITH temp_date AS 
(
SELECT CAST(date_column AS date) report_date
FROM
    (
        (
    VALUES 
            "sequence"((current_date - INTERVAL  '31' DAY), (current_date - INTERVAL  '1' DAY), INTERVAL  '1' DAY)
        ) t1 (date_array)
    CROSS JOIN UNNEST(date_array) t2 (date_column)
    )
)
,sla_precale AS 
(
    SELECT 
        DATE(report_date) AS recieve_date,
        DATE(sla_d1) AS sla_d_1_date,
        DATE(sla_d2) AS sla_d_2_date,
        DATE(sla_d3) AS sla_d_3_date,
        DATE(sla_d4) as sla_d_4_date,
        CAST(FROM_ISO8601_TIMESTAMP(report_date) AS TIMESTAMP) rec_date_time,
        CAST(FROM_ISO8601_TIMESTAMP(sla_d1) AS TIMESTAMP) sla_d_1_time,
        CAST(FROM_ISO8601_TIMESTAMP(sla_d2) AS TIMESTAMP) sla_d_2_time
    FROM dev_thopsbi_lof.spx_analytics_sla_precal_date_v1
),
raw_return as
(
select 
    fleet_order.shipment_id 
    ,fleet_order.order_type 
    ,CASE WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 or timestamp_3pl is not null THEN 1 ELSE 0 END AS is_4pl
    ,case 
        when station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 or timestamp_3pl is not null then '4PL'
        when returm_lm_destination like 'SOC%' or returm_lm_destination = 'SOCE' then 'LM'
        when (returm_lm_destination like '%RC' or returm_lm_destination in ('NORC-A','NORC-B','NERC','EARC','CERC','SORC')) or (Return_SOC_Received_at_rc < Return_SOC_Received_at_soc) then 'RC'
        else 'LM'
    end as route_type 
    -- status timestamp 
    ,Return_LMHub_LHTransported
    ,Return_SOC_lh_transported_at_rc
    ,Return_SOC_Received_at_rc
    ,Return_SOC_Received_at_soc
    ,Return_SOC_packing_at_soc
    ,Return_SOC_packed_at_soc
    ,Return_SOC_LH_packing_at_soc
    ,Return_SOC_LH_packed_at_soc
    ,Return_SOC_LH_transporting_at_soc
    ,Return_SOC_LH_transported_at_soc
    ,Return_SOC_returned_soc
    ,Return_SOC_returning_soc
    ,end_status
    ,case 
        when fleet_order.order_type < 2 then 'จังหวัดสมุทรสาคร'
        when pickup_order.seller_addr_state is not null then pickup_order.seller_addr_state
        when pickup_order.seller_addr_state is null and dropoff_order.seller_state is not null then dropoff_order.seller_state 
        else 'จังหวัดกรุงเทพมหานคร'
    end as seller_state
    ,case 
        when Return_LMHub_received > Return_SOC_Received_at_soc then Return_LMHub_received 
    end as Return_LMHub_received
    ,case 
        when Return_fm_received > Return_SOC_Received_at_soc then Return_fm_received 
    end as Return_fm_received 
    ,case 
        when Return_rc_received_after_soc > Return_SOC_Received_at_soc then Return_SOC_Received_at_soc 
    end as Return_rc_received_after_soc
    ,case 
        when non_return_timestamp is not null then 1 else 0 
    end as is_non_return_order
    ,return_from_3pl
    ,case 
        when status_count.buyer_reschedule >= 1 then 'buyer_reschedule' -- r1
        when status_count.cant_contact_buyer_arrival >= 1 then 'buyer_arrival'  -- r1
        when status_count.cant_contact_buyer >= 1 then 'cant_contact_buyer' -- r1
        when status_count.number_not_in_service >= 1 then 'contact_not_in_service' -- r1 
        when status_count.addr_is_incorrect >= 1 then 'addr_incorrect' -- r1 
        when status_count.addr_is_incomplete = 1 then 'addr_incomplete' -- r1
        when status_count.insufficient_time >= 1 then 'insufficient_time' -- r1 
        when status_count.reject_by_receipient >= 1 then 'recipient_reject' -- r1 
        when status_count.receipt_never_place_order >= 1 then 'never_place_order' -- r1 
        when status_count.Invalid_condition >=1 then 'invalid_condition' -- r1 
        when status_count.traffic_accident >=1 then 'traffic_accident' --r1 
        when status_count.office_schedule_for_weekday >=1 then 'office_reschedule' --r1 
        when status_count.out_of_serviceable_area >=1 then 'out_of_serviceable_area' -- r1 
    else 'non_sop_return_order' end as to_be_return   
    ,Return_SOC_assigning
    ,Return_SOC_assigned
    ,Return_fail
    ,Return_SOC_LHpacked_operator
    ,Return_SOC_LHtransporting_operator
    ,Return_SOC_received_operator
    ,to_be_return
    ,office_reschedule
    ,traffic_accident
    ,out_of_serviceable_area
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order 
left join 
    (
    select 
        shipment_id 
        ,min(case when status = 57 then try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar))end) as returm_lm_destination
        ,min(case when status in (18,89) then  FROM_UNIXTIME(ctime-3600) end) as timestamp_3pl 
        ,min(case when status = 95 then  FROM_UNIXTIME(ctime-3600) end) as return_from_3pl 
        ,min(case when status = 57 then  FROM_UNIXTIME(ctime-3600) end) as Return_LMHub_LHTransported 
        ,min(case when status = 65 and station_id not in (3,242) then FROM_UNIXTIME(ctime-3600) end) as Return_SOC_lh_transported_at_rc
        ,min(case when status = 58 and station_id not in (3,242) then FROM_UNIXTIME(ctime-3600) end) as Return_SOC_Received_at_rc
        ,min(case when status = 58 and station_id = 3 then FROM_UNIXTIME(ctime-3600) end) as Return_SOC_Received_at_soc 
        ,min(case when status = 59 and station_id = 3 then FROM_UNIXTIME(ctime-3600) end) as Return_SOC_packing_at_soc 
        ,min(case when status = 60 and station_id = 3 then FROM_UNIXTIME(ctime-3600) end) as Return_SOC_packed_at_soc 
        ,min(case when status = 61 and station_id = 3 then  FROM_UNIXTIME(ctime-3600) end) as Return_SOC_LH_packing_at_soc 
        ,min(case when status = 62 and station_id = 3 then  FROM_UNIXTIME(ctime-3600) end) as Return_SOC_LH_packed_at_soc
        ,min(case when status = 64 and station_id = 3 then  FROM_UNIXTIME(ctime-3600) end) as Return_SOC_LH_transporting_at_soc 
        ,min(case when status = 65 and station_id = 3 then  FROM_UNIXTIME(ctime-3600) end) as Return_SOC_LH_transported_at_soc 
        ,min(case when status = 63  and station_id = 3 then  FROM_UNIXTIME(ctime-3600) end) as Return_SOC_returning_soc
        ,min(case when status = 6  and station_id = 3 then  FROM_UNIXTIME(ctime-3600) end) as Return_SOC_returned_soc 
        -- test backlog 
        ,max(case when status in (10,52,53,54,55,119,120) then  FROM_UNIXTIME(ctime-3600) end) as Return_LMHub_received
        ,max(case when status in (67,68,69,70,71,115,116,73) then  FROM_UNIXTIME(ctime-3600) end) as Return_fm_received 
        ,max(case when status in (58,59) and station_id not in (3,242) then  FROM_UNIXTIME(ctime-3600) end) as Return_rc_received_after_soc  
        -- end status 
        ,max(case when status in (11,12,4,26) then  FROM_UNIXTIME(ctime-3600) end) as end_status 
        ,min(case when status = 58 and  station_id = 3 and operator in ('gee.boonprat@shopee.com','gun.sathiank@shopee.com','nittaya.nan@shopee.com','eakkarin.san@shopee.com','kay.padthama@shopee.com') then ctime end) as non_return_timestamp 
        --
        ,min(case when status = 117 and station_id = 3 then FROM_UNIXTIME(ctime-3600) end) as Return_SOC_assigning
        ,min(case when status = 118 and station_id = 3 then FROM_UNIXTIME(ctime-3600) end) as Return_SOC_assigned
        ,min(case when status = 14 and station_id = 3 then FROM_UNIXTIME(ctime-3600) end) as Return_fail
        ,min(case when status = 62 and station_id = 3 then operator end) as Return_SOC_LHpacked_operator
        ,min(case when status = 64 and station_id = 3 then operator end) as Return_SOC_LHtransporting_operator
        ,min(case when status = 58 and station_id = 3 then operator end) as Return_SOC_received_operator
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
    group by shipment_id 
    ) as order_track 
on order_track.shipment_id = fleet_order.shipment_id    
LEFT JOIN spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_daily_s0_live AS pickup_order
ON fleet_order.shipment_id = pickup_order.pickup_order_id
LEFT JOIN spx_mart.shopee_fms_th_db__dropoff_order_tab__th_daily_s0_live as dropoff_order 
on fleet_order.shipment_id = dropoff_order.shipment_id
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live AS station 
ON fleet_order.station_id = station.id
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
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
        group by shipment_id 
        ) as status_count
on status_count.shipment_id = fleet_order.shipment_id
where date(order_track.Return_SOC_Received_at_soc) > current_date - interval '40' day 
--where fleet_order.order_type < 2 and date(Return_SOC_Received_at_soc) = current_date - interval '2' day
--where returm_lm_destination is not null and date(Return_SOC_Received_at_soc) > current_date - interval '15' day and (CASE WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 or timestamp_3pl is not null THEN 1 ELSE 0 END) = 1
)
,region_map as
(
select 
    raw_return.*
    ,case 
        when region_map.lh_region is not null then region_map.lh_region 
    else 'GBKK' end as lh_region
from raw_return
LEFT JOIN 
    (
    SELECT 
        DISTINCT province
        ,lh_region 
    from thopsbi_lof.spx_index_region_temp
    ) as region_map
on region_map.province = raw_return.seller_state
where is_non_return_order = 0
)
,ontime_cal as
(
select 
    region_map.*
    ,case 
        when order_type >= 2 then 
            case  
                when is_4pl = 1 and lh_region = 'GBKK' and date(Return_SOC_LH_packed_at_soc - interval '10' hour) <= sla_precale.sla_d_1_date   then 1
                when lh_region = 'GBKK' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.recieve_date   then 1 
                when lh_region = 'SOUTH' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date  then 1 
                when lh_region = 'WEST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date   then 1 
                when lh_region = 'EAST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date   then 1 
                when lh_region = 'CENTRAL' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date    then 1 
                when lh_region = 'NORTHEAST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date  then 1                  
            end 
        when order_type < 2 then 
            case 
                when date(Return_SOC_returning_soc - interval '6' hour) <= sla_precale.recieve_date then 1                
            end
        else 0 
    end as is_outbound_ot 
    ,case 
        when order_type >= 2 then 
            case  
                when is_4pl = 1 and lh_region = 'GBKK' and date(Return_SOC_LH_packed_at_soc - interval '12' hour) <= sla_precale.sla_d_1_date   then 1
                when lh_region = 'GBKK' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.recieve_date   then 1 
                when lh_region = 'SOUTH' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date  then 1 
                when lh_region = 'WEST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date   then 1 
                when lh_region = 'EAST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date   then 1 
                when lh_region = 'CENTRAL' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date    then 1 
                when lh_region = 'NORTHEAST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date  then 1                  
            end 
        when order_type < 2 then 
            case 
                when date(Return_SOC_returning_soc - interval '6' hour) <= sla_precale.sla_d_1_date then 1                
            end
        else 0 
    end as is_outbound_ot_2 
    ,case 
        when order_type >= 2 then 
            case  
                when is_4pl = 1 and lh_region = 'GBKK' and date(Return_SOC_LH_packed_at_soc - interval '12' hour) <= sla_precale.sla_d_1_date   then 1
                when lh_region = 'GBKK' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.recieve_date                  then 1 
                when lh_region = 'SOUTH' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date                 then 1 
                when lh_region = 'WEST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date                  then 1 
                when lh_region = 'EAST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date                  then 1 
                when lh_region = 'CENTRAL' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date               then 1 
                when lh_region = 'NORTHEAST' and date(Return_SOC_LH_packed_at_soc - interval '6' hour) <= sla_precale.sla_d_1_date             then 1                  
            end 
        when order_type < 2 then 
            case 
                when date(Return_SOC_returning_soc - interval '6' hour) <= sla_precale.sla_d_1_date then 1    
                when date(Return_Soc_returning_soc - interval '6' hour) <= sla_precale.sla_d_2_date then 2
                when date(Return_Soc_retunring_soc - interval '6' hour) <= sla_precale.sla_d_3_date then 3        
            end
        else 0 
    end as is_outbound_ot_3
    ,case 
        when is_4pl = 0 and route_type = 'LM' and date(Return_LMHub_LHTransported - interval '12' hour) <= sla_precale.recieve_date then 1 
        when is_4pl = 0 and route_type = 'RC' and date(Return_SOC_Received_at_rc - interval '12' hour )  <= sla_precale.recieve_date then 1 
        when is_4pl = 1 and date(return_from_3pl - interval '12' hour) <= sla_precale. sla_d_1_date then 1 
        else 0  
    end as is_received_ontime 
from region_map
left join sla_precale
on sla_precale.recieve_date = date(region_map.Return_SOC_Received_at_soc)
-- where date(Return_SOC_Received_at_soc) = current_date - interval '1' day and order_type = 2 
)
select 
    shipment_id
    ,order_type
    ,case 
        when order_type = 0 then 'wh'
        when order_type = 1 then 'cb'
        when order_type >= 2 then 'mkp'
    end as order_type_name
    ,lh_region AS return_to_seller_region
    ,lh_region AS return_soc_Received_at_soc
    ,Return_SOC_Received_at_soc
    ,Return_SOC_packed_at_soc
    ,Return_SOC_LH_packed_at_soc
    ,Return_SOC_returning_soc
    ,Return_SOC_returned_soc
    ,Return_SOC_assigning
    ,Return_SOC_assigned
    ,Return_fail
    ,Return_SOC_received_operator
    ,Return_SOC_LHpacked_operator
    ,Return_SOC_LHtransporting_operator
from ontime_cal
where date(Return_SOC_Received_at_soc) = current_date - interval '1' day