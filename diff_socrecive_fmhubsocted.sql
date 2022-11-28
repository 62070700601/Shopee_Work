/*
column 1 => shipment_id
column 2.1 => โจทย์คือ  time(SOC_Received) - time(FMHub_LHTransported)  หลักให้จับ time(FMHub_LHTransported)
column 2.2 => ถ้าไม่มี time(FMHub_LHTransported) ให้จับ time(FMHub_Pickup_Handedover) แทน time(FMHub_Pickup_Handedover) - time(FMHub_LHTransported)
column 3 => lost_timestamp
column 4.1 => linehaul_task_id ของ FMHub_LHTransported
column 4.2 => pickup_task_id

FMHub_LHTransported = 48
FMHub_Pickup_Handedover	= 40
Lost = 11
SOC_Received = 8

-- shipment_inbound_route_type
-- INBOUND_TO_SOC => direct
-- INBOUND_FM_TO_SOC => shuttle

Example shipment_id
    SPXTH025708056039 => FMHub_LHTransported
    SPXTH024078282739 => FMHub_Pickup_Handedover
*/
with FMHub_LHTransported as 
(
select 
    shipment_id
    ,time_stamp
    ,station_name
    ,coalesce(linehaul_task_id,pickup_task_id_Handover) as task_id
    ,seller_area_name
    ,SUBSTRING(coalesce(linehaul_task_id,pickup_task_id_Handover), 1, 2) as substring_task_id
from 
(
select
    shipment_id
    ,time_stamp
    ,station_name
    ,linehaul_task_id
    ,coalesce(pickup_task_id_content,pickup_task_id) as pickup_task_id_Handover
    ,seller_area_name
    ,row_number
    ,row_number() over (partition by shipment_id order by time_stamp desc) as row_number_last
from   
    (
    select 
        order_tracking.shipment_id
        ,from_unixtime(order_tracking.ctime-3600) as time_stamp
        ,order_tracking.status
        ,order_tracking.station_id
        ,staion_table_name.station_name
        ,try(cast(json_extract(json_parse(content),'$.linehaul_task_id') as varchar)) as linehaul_task_id
        ,try(cast(json_extract(json_parse(content),'$.pickup_task_id') as varchar)) as pickup_task_id_content
        ,Pub_shipment.pickup_task_id
        ,seller_area_name
        ,row_number() over (partition by order_tracking.shipment_id,order_tracking.status order by from_unixtime(order_tracking.ctime-3600) desc) as row_number
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live  as order_tracking
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on order_tracking.station_id = staion_table_name.id
    left join thopsbi_spx.dwd_pub_shipment_info_df_th as Pub_shipment
    on order_tracking.shipment_id = Pub_shipment.shipment_id
    where
        order_tracking.status in (8,48,40)
        and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-09-25 00:00:00.000') and date('2022-09-25 23:59:59.000')
        -- and date(from_unixtime(order_tracking.ctime-3600)) between date(DATE_TRUNC('day',current_date ) - interval '7' day) and date(DATE_TRUNC('day',current_timestamp) + interval '23' hour + interval '59' minute + interval '59' second)
        -- and order_tracking.shipment_id = 'SPXTH025708056039'
        and shipment_inbound_route_type in ('INBOUND_TO_SOC','INBOUND_FM_TO_SOC')
    )
where   
    status in (48,40)
    and row_number = 1
)
where 
    row_number_last = 1
)
,SOC_Received as 
(
select
    shipment_id
    ,SOC_Received_Time
    ,station_name
from   
    (
    select 
        order_tracking.shipment_id
        ,from_unixtime(order_tracking.ctime-3600) as SOC_Received_Time
        ,order_tracking.status
        ,staion_table_name.station_name
        ,row_number() over (partition by order_tracking.shipment_id,order_tracking.status order by from_unixtime(order_tracking.ctime-3600) asc) as row_number
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live  as order_tracking
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on order_tracking.station_id = staion_table_name.id
    where
        order_tracking.status = 8
        and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date('2022-09-25 00:00:00.000') and date('2022-09-25 23:59:59.000')
        -- and date(from_unixtime(order_tracking.ctime-3600)) between date(DATE_TRUNC('day',current_date ) - interval '7' day) and date(DATE_TRUNC('day',current_timestamp) + interval '23' hour + interval '59' minute + interval '59' second)
        -- and order_tracking.shipment_id in ('SPXTH025708056039','SPXTH024078282739')
        -- and order_tracking.shipment_id = 'SPXTH024078282739'
        and station_id = 3
    )
where   
    status = 8 
    and row_number = 1
)
,lost as 
(
select 
    order_tracking.shipment_id
    ,from_unixtime(order_tracking.ctime-3600) as Lost_Time
    ,staion_table_name.station_name
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live  as order_tracking
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
on order_tracking.station_id = staion_table_name.id
where
    order_tracking.status = 11
)
select 
    FMHub_LHTransported.shipment_id
    ,FMHub_LHTransported.station_name
    ,seller_area_name
    ,case 
        when substring_task_id = 'LT' then 'SHUTTLE'
        when substring_task_id = 'PT' then 'DIRECT'
        when substring_task_id = 'QP' then 'DIRECT'
    end as fm_route_type
    ,date_diff('minute',FMHub_LHTransported.time_stamp,SOC_Received_Time) as diff_Socreceived_and_FMHub_LHTransported_minute
    ,FMHub_LHTransported.time_stamp as FMHub_LHTransported_time
    ,SOC_Received_Time
    -- ,SOC_Received_Time - FMHub_LHTransported.time_stamp as diff_Socreceived_and_FMHub_LHTransported
    ,FMHub_LHTransported.task_id
    ,Lost_Time
from FMHub_LHTransported
left join SOC_Received
on FMHub_LHTransported.shipment_id = SOC_Received.shipment_id
left join lost
on FMHub_LHTransported.shipment_id = lost.shipment_id
order by 
    FMHub_LHTransported.time_stamp asc