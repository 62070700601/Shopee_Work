/*
โจทย์เก่า
-- Onhold D-1 
จับที่ last status ของวันนั้น Max_status_Onhold and last_status_onhold ดึง station ของ last mile,shipment_id,timestamp,count(onhold) ทั้งหมดที่เกิดขึ้น
สมมุตขึ้น onhold 3 รอบ ให้บอกว่าที่มันขึ้น onhold นั้นมี onhold อะไรบ้าง
Column ที่ดึงออก
1. Shipment_id ได้แล้ว
2. Timestamp ได้แล้ว
3. Station_name_last_mile ได้แล้ว
4. Count(Onhold) ได้แล้ว
5. Count(Onholde reason ทั้งหมด)
-- array_join(slice(array_agg(distinct Return_SOC_LHTransported.shipment_id),1,3),',') as shipment_id_example -- result is varchar
-- slice(array_agg(distinct Return_SOC_LHTransported.shipment_id),1,3) as shipment_id_example -- result is array
-- array_join(array[pickup_onhold_reason_name,cast(count(pickup_onhold_reason) as varchar )], ' = ') as cause_onhold



โจทย์ใหม่
1. เปลี่ยน logic เป็น เอา onhold ที่เกิดขึ้น D-1 มาทั้งหมดเลยไม่สนว่าสถานะล่าสุดจะเป็น onhold หรือป่าว > D-1 00:00:00 - D-1 23:59:59
2.เพิ่มมาอีก 1 คอลัมเหมือนคอลัม F ทำเป็นเวลาในการ onhold ในแต่ละครั้ง ใส่ , เหมือน F เลย ตัด col B ออกไปได้เลย
3.เพิ่มอีก 1 คอลัม ดึง timestamp pickup_done date มาด้วย ถ้าไม่มี pickup เอา created มาแทน
    Created	0
    FMHub_Pickup_Done	39
*/
with Onhold_Description as 
(
select 
    fleet_order_Onhold.shipment_id
    ,fleet_order_Onhold.time_stamp
    -- ,fleet_order_Onhold.rank_num
    ,staion_table_name.station_name
    -- ,on_hold_reason
    ,on_hold_reason_name
    from 
    (
        select 
            shipment_id
            ,try(cast(json_extract(json_parse(content),'$.station_id') as int)) as cast_station_id
            ,status
            ,FROM_UNIXTIME(ctime-3600) as time_stamp
            ,on_hold_reason
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as rank_num
            ,case 
                when on_hold_reason = 0 then 'No Onhold Reason'
                when on_hold_reason = 17 then 'Buyer Request Reschedule'
                when on_hold_reason = 18 then 'Cannot contact recipient upon arrival'
                when on_hold_reason = 19 then 'Hub cannot contact recipient'
                when on_hold_reason = 20 then 'Number not in service'
                when on_hold_reason = 21 then 'Address is incorrect'
                when on_hold_reason = 22 then 'Location is inaccessible'
                when on_hold_reason = 23 then 'Address is incomplete'
                when on_hold_reason = 24 then 'Insufficient Time for Delivery'
                when on_hold_reason = 25 then 'Rejected By Recipient'
                when on_hold_reason = 26 then 'Recipient Never Placed Order'
                when on_hold_reason = 27 then 'Invalid Conditions'
                when on_hold_reason = 28 then 'Damaged Parcel'
                when on_hold_reason = 29 then 'Parcel Lost'
                when on_hold_reason = 30 then 'Disaster Heavy Rain Flooding'
                when on_hold_reason = 31 then 'Traffic Accident'
                when on_hold_reason = 32 then 'Island Delivery Delay'
                when on_hold_reason = 33 then 'Office Order Scheduled For Weekday Delivery'
                when on_hold_reason = 34 then 'Missorted Parcel'
                when on_hold_reason = 54 then 'Order couldn not be delivered successfully'
                when on_hold_reason = 55 then 'Out of serviceable area'
                when on_hold_reason = 79 then 'Shipment is under investigation'
            end as on_hold_reason_name
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 5
    ) fleet_order_Onhold
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
    on fleet_order_Onhold.cast_station_id = staion_table_name.id
where 
    fleet_order_Onhold.status = 5
    and rank_num = 1 
    and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '5' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day) 
group by
    fleet_order_Onhold.shipment_id
    ,fleet_order_Onhold.time_stamp
    ,staion_table_name.station_name
    ,on_hold_reason_name 
order by 
    fleet_order_Onhold.time_stamp asc
)
,Onhold_reason_count as
(
select 
    t1.shipment_id
    ,count(t1.shipment_id) as count_onhold
    ,array_join(slice(array_agg(FROM_UNIXTIME(t1.ctime-3600) order by FROM_UNIXTIME(t1.ctime-3600) desc),1,30),',') as identify_timestamp_onhold_all
    ,array_join(slice(array_agg(                
                    case 
                    when t1.on_hold_reason = 0 then 'No Onhold Reason'
                    when t1.on_hold_reason = 17 then 'Buyer Request Reschedule'
                    when t1.on_hold_reason = 18 then 'Cannot contact recipient upon arrival'
                    when t1.on_hold_reason = 19 then 'Hub cannot contact recipient'
                    when t1.on_hold_reason = 20 then 'Number not in service'
                    when t1.on_hold_reason = 21 then 'Address is incorrect'
                    when t1.on_hold_reason = 22 then 'Location is inaccessible'
                    when t1.on_hold_reason = 23 then 'Address is incomplete'
                    when t1.on_hold_reason = 24 then 'Insufficient Time for Delivery'
                    when t1.on_hold_reason = 25 then 'Rejected By Recipient'
                    when t1.on_hold_reason = 26 then 'Recipient Never Placed Order'
                    when t1.on_hold_reason = 27 then 'Invalid Conditions'
                    when t1.on_hold_reason = 28 then 'Damaged Parcel'
                    when t1.on_hold_reason = 29 then 'Parcel Lost'
                    when t1.on_hold_reason = 30 then 'Disaster Heavy Rain Flooding'
                    when t1.on_hold_reason = 31 then 'Traffic Accident'
                    when t1.on_hold_reason = 32 then 'Island Delivery Delay'
                    when t1.on_hold_reason = 33 then 'Office Order Scheduled For Weekday Delivery'
                    when t1.on_hold_reason = 34 then 'Missorted Parcel'
                    when t1.on_hold_reason = 54 then 'Order couldn not be delivered successfully'
                    when t1.on_hold_reason = 55 then 'Out of serviceable area'
                    when t1.on_hold_reason = 79 then 'Shipment is under investigation'
                end order by FROM_UNIXTIME(t1.ctime-3600) desc),1,30),',') as identify_pickup_onhold_reason_all
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live t1
inner join 
        (
        select 
            fleet_order_Onhold.shipment_id
            ,fleet_order_Onhold.time_stamp
            ,fleet_order_Onhold.rank_num
            ,staion_table_name.station_name
            ,on_hold_reason
        from 
            (
            select 
                shipment_id
                ,try(cast(json_extract(json_parse(content),'$.station_id') as int)) as cast_station_id
                ,status
                ,FROM_UNIXTIME(ctime-3600) as time_stamp
                ,on_hold_reason
                ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as rank_num
            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            where 
                status = 5
            ) fleet_order_Onhold
        left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
        on fleet_order_Onhold.cast_station_id = staion_table_name.id
        where 
            -- fleet_order_Onhold.status = 5
            rank_num = 1 
            and fleet_order_Onhold.status = 5
            and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '5' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day) 
        group by 
            fleet_order_Onhold.shipment_id
            ,fleet_order_Onhold.time_stamp
            ,fleet_order_Onhold.rank_num
            ,staion_table_name.station_name
            ,on_hold_reason
        ) as t2
        on t1.shipment_id = t2.shipment_id
where 
    t1.status = 5
group by 
    t1.shipment_id
)
,pickupdone_or_create as 
(
select
    shipment_id
    ,max(time_stamp) as timestamp_pickupdone_or_create
from 
    (
    select 
        shipment_id
        ,status
        ,FROM_UNIXTIME(ctime-3600) as time_stamp
        ,row_number() over (partition by shipment_id,status order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    where 
        status in (39,0)
    )
where 
    rank_num = 1
    and status in (39,0)
    and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '30' day) and date(DATE_TRUNC('day', current_timestamp) - interval '0' day + interval '23' hour)
group by 
    shipment_id
)
select 
    Onhold_Description.shipment_id
    ,Onhold_Description.time_stamp 
    ,Onhold_Description.station_name 
    ,Onhold_Description.on_hold_reason_name
    ,Onhold_reason_count.count_onhold
    ,Onhold_reason_count.identify_pickup_onhold_reason_all
    ,Onhold_reason_count.identify_timestamp_onhold_all
    ,pickupdone_or_create.timestamp_pickupdone_or_create
from Onhold_Description
inner join Onhold_reason_count
on Onhold_Description.shipment_id = Onhold_reason_count.shipment_id
left join pickupdone_or_create
on Onhold_Description.shipment_id = pickupdone_or_create.shipment_id
order by 
    Onhold_Description.time_stamp asc