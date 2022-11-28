with last_status as
(
select *
		from 
		( select 
			shipment_id
			,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_num
			,status
			,FROM_UNIXTIME(ctime-3600) as time_stamp
		from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
		) rank_ship
		where 
			row_num = 1 and status in (12,2,50,49,137,116,115,72,120,119,124,118,63,5,74,126,91,99,100,92,89,94,97,98,90,80,93,95,51,81,3,4,11,14,73,125,6,38,31,96,26)
		order by time_stamp asc
)
select
		last_status.shipment_id
        ,last_status.row_num
        ,last_status.status
        ,case 
            when last_status.status = 51 then 'Collected' 
            when last_status.status = 81 then '3PL_Delivered'
            when last_status.status = 3 then 'Cancelled'
            when last_status.status = 4 then 'Delivered'
            when last_status.status = 14 then 'Return_Failed'   
            when last_status.status = 73 then 'Return_FMHub_Returned'
            when last_status.status = 125 then 'Return_LMHub_Returned'
            when last_status.status = 6 then 'Return_SOC_Returned'
            when last_status.status = 38 then 'FMHub_Pickup_Failed'
            when last_status.status = 31 then 'SOC_Pickup_Failed'
            when last_status.status = 96 then '3PL_Lost'
            when last_status.status = 26 then 'Disposed' 
            when last_status.status = 11 then 'Lost'
            when last_status.status = 12 then 'Damaged'
            when last_status.status = 2 then 'Delivering'
            when last_status.status = 50 then 'LMHub_Assigned'
            when last_status.status = 49 then 'LMHub_Assigning'
            when last_status.status = 137 then 'LMHub_Delivery_transfer'
            when last_status.status = 116 then 'Return_FMHub_Assigned'
            when last_status.status = 115 then 'Return_FMHub_Assigning'
            when last_status.status = 72 then 'Return_FMHub_Returning'
            when last_status.status = 120 then 'Return_LMHub_Assigned'
            when last_status.status = 119 then 'Return_LMHub_Assigning'
            when last_status.status = 124 then 'Return_LMHub_Returning'
            when last_status.status = 118 then 'Return_SOC_Assigned'
            when last_status.status = 117  then 'Return_SOC_Assigning'
            when last_status.status = 63 then 'Return_SOC_Returning' 
            when last_status.status = 5 then 'OnHold'
            when status = 74 then 'Return_FMHub_Onhold'
            when status = 126 then 'Return_LMHub_Onhold'
            when status = 91 then '3PL_Delivering'
            when status = 99 then '3PL_LMHub_Inbound'
            when status = 100 then '3PL_Others'
            when status = 92 then '3PL_Ready_For_Collection'
            when status = 89 then '3PL_Received'
            when status = 94 then '3PL_Returning'
            when status = 97 then '3PL_SOC_Inbound'
            when status = 98 then '3PL_SOC_Outbound'
            when status = 90 then '3PL_Transporting'
            when status = 80 then '3PL_Onhold'
            when status = 93 then '3PL_Return_Started'
            when status = 95 then '3PL_Returned'
        end as status_name
        ,last_status.time_stamp as timestamp_status
        ,maxtime_unlogsuccessful.time_stamp as maxtime_unlogstatus
from last_status
inner join 
(
    SELECT 
		shipment_id
		,max(from_unixtime(ctime-3600)) as time_stamp
		,station_name
    FROM spx_mart.shopee_fms_log_th_db__order_unsuccessful_operation_log_tab__reg_continuous_s0_live
    WHERE 
		station_id in (3,1784)
    group by 
		shipment_id
		,station_name
) maxtime_unlogsuccessful
on last_status.shipment_id = maxtime_unlogsuccessful.shipment_id
where 
    date(last_status.time_stamp) between DATE(DATE_TRUNC('day', current_date)) - interval '1' day and DATE(DATE_TRUNC('day', current_date)) 
    and maxtime_unlogsuccessful.time_stamp is not null
order by last_status.time_stamp asc