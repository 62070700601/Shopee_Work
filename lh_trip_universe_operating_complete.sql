with cte as 
(
select 
    lh_tab_complete.trip_number
    ,from_unixtime(lh_tab_complete.trip_date-3600) as trip_date
    ,from_unixtime(lh_tab_complete.ctime-3600) as "Create time"
    ,station_table_name_loaded.station_name as "Sender Station Name"
    ,station_table_name_dest.station_name as "Receive Station Name"
    ,lh_tab_complete.operator
    ,lh_tab_complete.trip_name
    ,from_unixtime(lh_tab_complete.mtime-3600) as Last_Time_Update
    ,from_unixtime(lh_tab_complete.ctime-3600) as Operation_Time
    ,from_unixtime(transportation_trip_loading.ctime-3600) as ata_timestamp
    ,lh_tab_complete.driver
    ,lh_tab_complete.vehicle_number
    ,case 
        when from_unixtime(transportation_trip_loading.ctime-3600) >= DATE_TRUNC('day', from_unixtime(transportation_trip_loading.ctime-3600)) + interval '6' hour then date(from_unixtime(transportation_trip_loading.ctime-3600))
        else date(from_unixtime(transportation_trip_loading.ctime-3600)) - interval '1' day
    end as ata_date   
    ,row_number() over (partition by lh_tab_complete.trip_number order by from_unixtime(transportation_trip_loading.loaded_time-3600) asc) as rank_num
    ,case 
        when lh_tab_complete.trip_status = 90 then 'Completed'
        when lh_tab_complete.trip_status = 100 then 'Cancelled'
    end as trip_status_name
from spx_mart.shopee_line_haul_network_th_db__transportation_trip_history_tab__reg_continuous_s0_live as lh_tab_complete
left join spx_mart.shopee_line_haul_network_th_db__transportation_trip_loading_record_history_tab__reg_continuous_s0_live as transportation_trip_loading
on lh_tab_complete.id = transportation_trip_loading.trip_id
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station_table_name_loaded
on station_table_name_loaded.id = transportation_trip_loading.loaded_station
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station_table_name_dest
on station_table_name_dest.id = transportation_trip_loading.to_dest_station
where 
    date(from_unixtime(lh_tab_complete.trip_date-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '14' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day)
    and lh_tab_complete.trip_status != 100
    -- lh_tab_complete.trip_number = 'LT0MBK000JAF1'
group by 
    lh_tab_complete.trip_number
    ,from_unixtime(lh_tab_complete.ctime-3600)
    ,from_unixtime(lh_tab_complete.trip_date-3600)
    ,case 
        when lh_tab_complete.trip_status = 90 then 'Completed'
        when lh_tab_complete.trip_status = 100 then 'Cancelled'
    end
    ,station_table_name_loaded.station_name
    ,station_table_name_dest.station_name
    ,loaded_time
    ,lh_tab_complete.operator
    ,lh_tab_complete.trip_name
    ,from_unixtime(lh_tab_complete.mtime-3600) 
    ,from_unixtime(lh_tab_complete.ctime-3600) 
    ,from_unixtime(transportation_trip_loading.ctime-3600) 
    ,lh_tab_complete.driver
    ,lh_tab_complete.vehicle_number
    ,case 
        when from_unixtime(transportation_trip_loading.ctime-3600) >= DATE_TRUNC('day', from_unixtime(transportation_trip_loading.ctime-3600)) + interval '6' hour then date(from_unixtime(transportation_trip_loading.ctime-3600))
        else date(from_unixtime(transportation_trip_loading.ctime-3600)) - interval '1' day
    end 
order by 
    from_unixtime(lh_tab_complete.ctime-3600) asc
)
select 
    -- cte.*
    trip_number
    ,trip_status_name
    ,trip_name
    ,Last_Time_Update
    ,"Create time"
    ,ata_timestamp
    ,ata_date
    ,trip_date
    -- ,"Sender Station Name"
    -- ,"Receive Station Name"
    ,vehicle_number
    ,driver
    ,operator
from cte
where 
    rank_num = 1
group by 
    trip_number
    ,trip_status_name
    ,trip_name
    ,Last_Time_Update
    ,"Create time"
    ,ata_timestamp
    ,ata_date
    ,trip_date
    -- ,"Sender Station Name"
    -- ,"Receive Station Name"
    ,vehicle_number
    ,driver
    ,operator
order by
    trip_date asc