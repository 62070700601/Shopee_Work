with cte as 
(
select 
    lh_tab_pending.id
    ,lh_tab_pending.trip_number
    ,from_unixtime(lh_tab_pending.trip_date-3600) as trip_date
    ,lh_tab_pending.trip_status
    ,case 
        when from_unixtime(transportation_trip_loading.ctime-3600) >= DATE_TRUNC('day', from_unixtime(transportation_trip_loading.ctime-3600)) + interval '6' hour then date(from_unixtime(transportation_trip_loading.ctime-3600))
        else date(from_unixtime(transportation_trip_loading.ctime-3600)) - interval '1' day
    end as ata_date
    ,case 
        when lh_tab_pending.trip_status = 0 then 'Created'
        when lh_tab_pending.trip_status = 5 then 'Assigned'
        when lh_tab_pending.trip_status = 10 then 'Operating'
        when lh_tab_pending.trip_status = 30 then 'Seal'
        when lh_tab_pending.trip_status = 40 then 'Departed'
        when lh_tab_pending.trip_status = 50 then 'Arrived'
        when lh_tab_pending.trip_status = 60 then 'Unseal' -- OK   
        when lh_tab_pending.trip_status = 80 then 'Operating'
    end as trip_status_name 
    ,lh_tab_pending.operator
    ,lh_trip.departure_station
    ,lh_trip.arrive_station
    ,from_unixtime(lh_tab_pending.ctime-3600) as Operation_Time
    ,from_unixtime(lh_tab_pending.mtime-3600) as Last_Time_Update
    ,lh_tab_pending.trip_name
    ,lh_tab_pending.driver
    ,lh_tab_pending.vehicle_number
    ,transportation_trip_loading.to_weight * 0.00100000 as "Weight (kg)"
    ,transportation_trip_loading.to_parcel_quantity as "Order Quantity"
    ,transportation_trip_loading.to_number as "TO Number"
    ,from_unixtime(transportation_trip_loading.loaded_time-3600) as "Outbound Scan Time"
    ,transportation_trip_loading.to_dest_station as "Receive Station ID"
    ,station_table_name_dest.station_name as "Receive Station Name"
    ,transportation_trip_loading.loaded_station as "Sender Station ID"
    ,station_table_name_loaded.station_name as "Sender Station Name"
    ,from_unixtime(transportation_trip_loading.unloaded_time-3600) as unloaded_loading_cast
    ,from_unixtime(transportation_trip_loading.ctime-3600) as ctime_loading_cast
    ,from_unixtime(transportation_trip_loading.mtime-3600) as mtime_loading_cast
    ,row_number() over (partition by lh_tab_pending.trip_number order by from_unixtime(transportation_trip_loading.ctime-3600) asc) as rank_num
from spx_mart.shopee_line_haul_network_th_db__transportation_trip_tab__reg_continuous_s0_live as lh_tab_pending
left join spx_mart.shopee_line_haul_network_th_db__transportation_trip_history_tab__reg_continuous_s0_live as lh_tab_complete
on lh_tab_pending.trip_number = lh_tab_complete.trip_number
left join spx_mart.shopee_line_haul_network_th_db__transportation_trip_loading_record_tab__reg_daily_s0_live as transportation_trip_loading
on lh_tab_pending.id = transportation_trip_loading.trip_id
LEFT JOIN spx_mart.shopee_line_haul_network_th_db__transportation_trip_line_tab__reg_continuous_s0_live as lh_trip ----- edited desination last status
ON lh_tab_pending.id = lh_trip.trip_id
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station_table_name_loaded
on station_table_name_loaded.id = transportation_trip_loading.loaded_station
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station_table_name_dest
on station_table_name_dest.id = transportation_trip_loading.to_dest_station
where 
    lh_tab_complete.trip_number is null
    and lh_tab_pending.trip_status != 0
    and date(from_unixtime(lh_tab_pending.trip_date-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '14' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day)
group by 
    lh_tab_pending.id
    ,lh_tab_pending.trip_number
    ,from_unixtime(lh_tab_pending.trip_date-3600)
    ,lh_tab_pending.trip_status
    ,case 
        when lh_tab_pending.trip_status = 0 then 'Created'
        when lh_tab_pending.trip_status = 5 then 'Assigned'
        when lh_tab_pending.trip_status = 10 then 'Operating'
        when lh_tab_pending.trip_status = 30 then 'Seal'
        when lh_tab_pending.trip_status = 40 then 'Departed'
        when lh_tab_pending.trip_status = 50 then 'Arrived'
        when lh_tab_pending.trip_status = 60 then 'Unseal' -- OK
        when lh_tab_pending.trip_status = 80 then 'Operating'
    end 
    ,case 
        when from_unixtime(lh_tab_pending.trip_date-3600) >= DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '6' hour then from_unixtime(lh_tab_pending.trip_date-3600)
        else from_unixtime(lh_tab_pending.trip_date-3600) - interval '1' day
    end
    ,lh_tab_pending.start_station 
    ,lh_tab_pending.operator
    ,from_unixtime(lh_tab_pending.ctime-3600) 
    ,from_unixtime(lh_tab_pending.mtime-3600) 
    ,lh_tab_pending.trip_name
    ,lh_tab_pending.driver
    ,transportation_trip_loading.to_weight * 0.00100000
    ,transportation_trip_loading.to_parcel_quantity
    ,transportation_trip_loading.to_number
    ,from_unixtime(transportation_trip_loading.loaded_time-3600) 
    ,to_dest_station
    ,lh_trip.departure_station
    ,lh_trip.arrive_station
    ,transportation_trip_loading.loaded_station
    ,transportation_trip_loading.to_dest_station
    ,station_table_name_loaded.station_name
    ,station_table_name_dest.station_name
    ,lh_tab_pending.vehicle_number
    ,from_unixtime(transportation_trip_loading.unloaded_time-3600)
    ,from_unixtime(transportation_trip_loading.ctime-3600)
    ,from_unixtime(transportation_trip_loading.mtime-3600)
)
,shipment_check_sender_or_reciver as
(
select 
    status_name
    ,status
    ,shipment_id
    -- ,time_stamp
    ,linehaul_id
    ,lead_rank
    ,case
        when status_name = 'SOC_LHTransported' and lead_rank = 'LMHub_Received' then 'receiver correct'
        when status_name = 'SOC_LHTransporting' and lead_rank = 'LMHub_Received' then 'receiver wrong'
        when status_name in ('SOC_LHPacked','SOC_LHPacking') and lead_rank = 'LMHub_Received' then 'sender wrong'
        when status_name = 'SOC_LHTransported' and lead_rank = 'SOC_Received' then 'receiver correct'
        when status_name = 'SOC_LHTransporting' and lead_rank = 'SOC_Received' then 'receiver wrong'
        when status_name in ('SOC_LHPacked','SOC_LHPacking') and lead_rank = 'SOC_Received' then 'sender wrong'
        when status_name = 'FMHub_LHTransported' and lead_rank = 'SOC_Received' then 'receiver correct'
        when status_name = 'FMHub_LHTransporting' and lead_rank = 'SOC_Received' then 'receiver wrong'
        when status_name in ('FMHub_LHPacked','FMHub_LHPacking') and lead_rank = 'SOC_Received' then 'sender wrong'
        when status_name = 'Return_LMHub_LHTransported' and lead_rank = 'Return_SOC_Received' then 'receiver correct'
        when status_name = 'Return_LMHub_LHTransporting' and lead_rank = 'Return_SOC_Received' then 'receiver wrong'
        when status_name in ('Return_LMHub_LHPacked','Return_LMHub_LHPacking') and lead_rank = 'Return_SOC_Received' then 'sender wrong'
        when status_name = 'Return_SOC_LHTransported' and lead_rank = 'Return_SOC_Received' then 'receiver correct'
        when status_name = 'Return_SOC_LHTransporting' and lead_rank = 'Return_SOC_Received' then 'receiver wrong'
        when status_name in ('Return_SOC_LHPacked','Return_SOC_LHPacking') and lead_rank = 'Return_SOC_Received' then 'sender wrong'
        when status_name = 'Return_SOC_LHTransported' and lead_rank = 'Return_LMHub_Received' then 'receiver correct'
        when status_name = 'Return_SOC_LHTransporting' and lead_rank = 'Return_LMHub_Received' then 'receiver wrong'
        when status_name in ('Return_SOC_LHPacked','Return_SOC_LHPacking') and lead_rank = 'Return_LMHub_Received' then 'sender wrong'
        when status_name = 'LMHub_LHTransported' and lead_rank = 'SOC_Received' then 'receiver correct'
        when status_name = 'LMHub_LHTransporting' and lead_rank = 'SOC_Received' then 'receiver wrong'
        when status_name in ('LMHub_LHPacked','LMHub_LHPacking') and lead_rank = 'SOC_Received' then 'sender wrong'
        when status not in (15,47,233,56,64) and status_name not in ('SOC_LHTransported','FMHub_LHTransported','Return_LMHub_LHTransported','Return_SOC_LHTransported','LMHub_LHTransported') and lead_rank is null then 'sender wrong' 
        when status in (15,47,233,56,64) and lead_rank is null then 'LH wrong'
        else 'receiver wrong'
    end as sender_or_reciver
from
(
select 
    thspx_fact_order_tracking_di_th.status_name
    ,order_track.status
    ,order_track.shipment_id
    ,from_unixtime(ctime-3600) as time_stamp
    ,TRY(cast(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id') as varchar)) as linehaul_id 
    ,lead(thspx_fact_order_tracking_di_th.status_name) over (partition by shipment_id order by from_unixtime(ctime-3600) asc) as lead_rank
    ,row_number() over (partition by TRY(cast(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id') as varchar)) order by from_unixtime(ctime-3600) desc) as rank_num
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
left join 
    (
        select 
            status
            ,status_name
        from thopsbi_lof.thspx_fact_order_tracking_di_th
        group by 
            status
            ,status_name
    ) as thspx_fact_order_tracking_di_th
on order_track.status = thspx_fact_order_tracking_di_th.status
where 
    -- shipment_id = 'SPXTH02429567805B'
    date(from_unixtime(ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '30' day) and date(DATE_TRUNC('day', current_timestamp) - interval '0' day)
    -- TRY(cast(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id') as varchar)) = 'LT0MBM000HQ41'
order by
    from_unixtime(ctime-3600) asc
)
where 
    status in (34,35,15,36,45,46,47,48,231,232,233,234,54,56,57,55,61,65,64,62)
    and rank_num = 1
    -- and lead_rank not in ('LMHub_LHTransported','SOC_LHTransported','Return_LMHub_LHTransported','Return_LMHub_LHTransporting','Return_SOC_LHTransported','FMHub_LHTransported','SOC_LHTransporting','FMHub_LHTransporting','LMHub_LHTransporting','Return_SOC_LHTransporting','SOC_LHPacked','FMHub_LHPacked','LMHub_Packed','Return_LMHub_LHPacked','Return_SOC_LHPacked','LMHub_LHPacked')
group by 
    status_name
    ,status
    ,shipment_id
    -- ,time_stamp
    ,linehaul_id
    ,lead_rank
)
-- With out cte
select 
    trip_number
    ,trip_status_name
    ,trip_name
    ,Last_Time_Update
    ,Operation_Time as "Create time"
    ,ctime_loading_cast as ata_timestamp
    ,ata_date
    ,trip_date as "Trip date"
    ,vehicle_number
    ,driver as driver_id
    ,operator
    -- ,shipment_check_sender_or_reciver.sender_or_reciver
    ,coalesce(shipment_check_sender_or_reciver.sender_or_reciver,'Unidentified') as sender_or_reciver_type
from cte
left join shipment_check_sender_or_reciver
on cte.trip_number = shipment_check_sender_or_reciver.linehaul_id
where 
    rank_num = 1
group by 
    trip_number
    ,trip_date
    ,Operation_Time
    -- ,"Sender Station Name"
    -- ,"Receive Station Name"
    ,trip_status_name
    ,ata_date
    ,Last_Time_Update
    ,operator
    ,driver
    ,vehicle_number
    ,trip_name
    ,ctime_loading_cast
    ,coalesce(shipment_check_sender_or_reciver.sender_or_reciver,'Unidentified')
order by
    trip_date asc