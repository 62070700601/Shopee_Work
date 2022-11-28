/*
Period Data: 17-21 Oct
 # Universe งานที่ SOC_Received ใน 17-21 Oct แบ่งตาม
>> FM Hub
>> Direct Truck
# Result จำนวนของพัสดุที่ Max Number ของ Width หรือ Length ตามช่วงด้านล่างนี้
0.00 - 0.50 cm
0.51 - 25.00 cm
25.01 - 35.00 cm
35.01 cm
ตัวอย่าง พัสดุ Width 8cm / Length 9cm, Max = 9cm อยู่ในช่วย 0.51-25cm



universe soc_received 
แล้วจับย้อนว่ามี fm ted ก่อน ถ้ามีให้เป็น shuttle ถ้าไม่มี ted มี pickup ป่าว ถ้ามีเป็น direct
FMHub_LHTransported	= 48
FMHub_Pickup_Done = 39

*/
with SOC_Received as
(
    select 
        shipment_id
        ,date_time
        ,station_name
        ,Range_Max_Number_width_length
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
            ,pub_shipment.manual_package_length_in_cm
            ,pub_shipment.manual_package_width_in_cm
            ,if(pub_shipment.manual_package_length_in_cm >= pub_shipment.manual_package_width_in_cm,pub_shipment.manual_package_length_in_cm,pub_shipment.manual_package_width_in_cm) as max_number_width_length
            ,case 
                when if(pub_shipment.manual_package_length_in_cm >= pub_shipment.manual_package_width_in_cm,pub_shipment.manual_package_length_in_cm,pub_shipment.manual_package_width_in_cm) between 0.00 and 0.50 then '0.00 - 0.50 cm'
                when if(pub_shipment.manual_package_length_in_cm >= pub_shipment.manual_package_width_in_cm,pub_shipment.manual_package_length_in_cm,pub_shipment.manual_package_width_in_cm) between 0.51 and 25.00 then '0.51 - 25.00 cm'
                when if(pub_shipment.manual_package_length_in_cm >= pub_shipment.manual_package_width_in_cm,pub_shipment.manual_package_length_in_cm,pub_shipment.manual_package_width_in_cm) between 25.01 and 35.00 then '25.01 - 35.00 cm'
                when if(pub_shipment.manual_package_length_in_cm >= pub_shipment.manual_package_width_in_cm,pub_shipment.manual_package_length_in_cm,pub_shipment.manual_package_width_in_cm) between 25.01 and 35.00 then '25.01 - 35.00 cm'
                when if(pub_shipment.manual_package_length_in_cm >= pub_shipment.manual_package_width_in_cm,pub_shipment.manual_package_length_in_cm,pub_shipment.manual_package_width_in_cm) > 35.00 then '35.01 cm'
            end Range_Max_Number_width_length
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
        on order_tracking.station_id = staion_table_name.id
        left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
        on order_tracking.shipment_id = pub_shipment.shipment_id
        where 
            order_tracking.status = 8
            -- and order_tracking.shipment_id = 'SPXTH02273438988A'
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '10' day) and date(DATE_TRUNC('day', current_timestamp) - interval '10' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status = 8
        -- and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '10' day) and date(DATE_TRUNC('day', current_timestamp) - interval '10' day + interval '23' hour + interval '59' minute + interval '59' second )
        and date_time between date '2022-10-17 00:00:00.000' and date '2022-10-21 00:00:00.000'
        
    group by 
        shipment_id
        ,date_time
        ,station_name
        ,Range_Max_Number_width_length
)
,FMHub_Pickup_Done as 
(
    select 
        shipment_id
        ,date_time
        ,station_name
        ,status
        ,rank_num
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
        on order_tracking.station_id = staion_table_name.id
        where 
            order_tracking.status in (39,48)
            and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date '2022-10-01 00:00:00.000' and date '2022-10-28 00:00:00.000'
            -- and date(FROM_UNIXTIME(order_tracking.ctime-3600)) between date(DATE_TRUNC('day', current_timestamp) - interval '20' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    )
    where 
        rank_num = 1
        and status in (39,48)
    group by 
        shipment_id
        ,date_time
        ,station_name
        ,status
        ,rank_num
)
select 
    SOC_Received.date_time
    ,FMHub_Pickup_Done.station_name
    ,SOC_Received.Range_Max_Number_width_length
    ,count(FMHub_Pickup_Done.shipment_id) as All_Result_Range_length_width
    -- ,count(if(FMHub_Pickup_Done.status = 48,1,0)) as Shuttle_Result_Range_length_width
    ,count(case when FMHub_Pickup_Done.status = 48 then 1 end) as Shuttle_Result_Range_length_width
    ,count(case when FMHub_Pickup_Done.status = 39 then 1 end) as Direct_Result_Range_length_width
    -- ,count(if(FMHub_Pickup_Done.status = 39,1,0)) as Direct_Result_Range_length_width
from SOC_Received
inner join FMHub_Pickup_Done
on SOC_Received.shipment_id = FMHub_Pickup_Done.shipment_id
where 
    SOC_Received.date_time between date '2022-10-17 00:00:00.000' and date '2022-10-21 00:00:00.000'
    -- SOC_Received.date_time between date(DATE_TRUNC('day', current_timestamp) - interval '10' day) and date(DATE_TRUNC('day', current_timestamp) - interval '10' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    FMHub_Pickup_Done.station_name
    ,SOC_Received.Range_Max_Number_width_length
    ,SOC_Received.date_time
order by 
    SOC_Received.date_time
    ,FMHub_Pickup_Done.station_name asc
    ,SOC_Received.Range_Max_Number_width_length asc
