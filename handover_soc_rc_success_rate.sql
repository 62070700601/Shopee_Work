/*
arrang => fm_recived => soce
จะแบ่งเป็น 2 ก้อน
1.arrange pickup เท่าไหร่บ้าง แล้ว fm_recevied จิงๆมาเท่าไหร่
    cutoff_recived คือ 21.00
2.fm_recived ไปเท่าไหร่ แล้ว fm_ting ไปเท่าไหร่ ถ้าไม่มี fm_ting ก็จับเหมือนเดิมให้เป็น status อื่นๆ
โดยอันนี้จะจับจาก hub โดยตรง
    Universe FMHub_Received [D0] => cut_off คือ 23.59
    FMHub_LHTransporting_ontime [D0] => 22.59
    FMHub_Received [D0] => 21.59
เวลาทั้งหทด 45 days
case เปลี่ยนมาใช้ rank_num
Run ทุกวัน 24.00
*/
-- case 2 fm_recived
/*
arrang => fm_recived => soce
จะแบ่งเป็น 2 ก้อน
1.arrange pickup เท่าไหร่บ้าง แล้ว fm_recevied จิงๆมาเท่าไหร่
    cutoff_recived คือ 21.00
2.fm_recived ไปเท่าไหร่ แล้ว fm_ting ไปเท่าไหร่ ถ้าไม่มี fm_ting ก็จับเหมือนเดิมให้เป็น status อื่นๆ
โดยอันนี้จะจับจาก hub โดยตรง
    Universe FMHub_Received [D0] => cut_off คือ 23.59
    FMHub_LHTransporting_ontime [D0] => 22.59
    FMHub_Received [D0] => 21.59
เวลาทั้งหทด 45 days
case เปลี่ยนมาใช้ rank_num
Run ทุกวัน 24.00
*/
-- case 2 fm_recived
/*
arrang => fm_recived => soce
จะแบ่งเป็น 2 ก้อน
1.arrange pickup เท่าไหร่บ้าง แล้ว fm_recevied จิงๆมาเท่าไหร่
    cutoff_recived คือ 21.00
2.fm_recived ไปเท่าไหร่ แล้ว fm_ting ไปเท่าไหร่ ถ้าไม่มี fm_ting ก็จับเหมือนเดิมให้เป็น status อื่นๆ
โดยอันนี้จะจับจาก hub โดยตรง
    Universe FMHub_Received [D0] => cut_off คือ 23.59
    FMHub_LHTransporting_ontime [D0] => 22.59
    FMHub_Received [D0] => 21.59
เวลาทั้งหทด 45 days
case เปลี่ยนมาใช้ rank_num
Run ทุกวัน 24.00
*/
-- case 2 fm_recived
with FMHub_Received as
(
    select 
        shipment_id
        ,date_time
        ,station_name
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status = 42
    )
    where 
        rank_num = 1
        and status = 42
        and (split_part(station_name,' ',1) in ('FPHIT-A','FPHIT-B','FWTNG','FKPET','FTAKK','FMSOD','FNANN','FPJIT','FPBUN','FLOMS','FPRAE','FLOEI','FTHAI','FSWKL','FUTTA'
                                                                ,'FSRPI','FCMAI-A','FCMAI-B','FSSAI','FMRIM','FDSKT','FCDAO','FFRNG','FSTNG','FDONG','FSANK','FPAAN','FCRAI','FMSAI','FMJUN','FPYAO','FLPNG','FLPUN'
                                                                ,'FKRAT-A','FKRAT-B','FKRAT-C','FNSUG','FCOCH','FPAKC','FPIMY','FBUAY','FSKIU','FDKTD','FPTCI','FKNBR','FSNEN','FPHUK','FCYPM','FBRAM','FLPMT','FSTUK','FNRNG','FPKCI','FYASO','FSSKT','FSRIN','FSKPM','FPSAT','FUBON-A','FUBON-B','FWRIN','FDUDM'
                                                                ,'FKKAN-B','FKKAN-A','FBPAI','FCPAE','FKLSN','FYTAD','FNKPN','FTPNM','FMKAM','FKSPS','FKTWC','FMDHN','FROET','FSKON','FNKAI','FPSAI','FUDON-A','FUDON-B'
                                                                ,'FPPIN','FSMUI','FSRAT','FKDIT','FBNSN','FKRBI','FCPON','FPTIL','FSAWE','FTSNG','FCOUD','FNKSI','FTYAI','FTSLA','FSICN','FTLNG','FPHKT-A','FPHKT-B','FRNNG'
                                                                ,'FHYAI-B','FHYAI-A','FSKLA','FSDAO','FTANG','FNARA','FPTNI','FKGPO','FMYOR','FYLNG','FPATL','FKUKN','FYALA','FRMAN','FSTUN'
                                                                ,'FSWAN','FTAKI','FBPIN','FAYUT','FSENA','FAUTH','FWNOI','FLOPB','FKSRG','FCBDN','FPTNK','FKKOI','FSRBR','FBAMO','FPTBT','FPTBT','FNKAE','FSING','FTONG')
        OR split_part(station_name,' ',1) like 'D%')
        -- and date_time = date('2022-12-14')
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
        ,station_name
)
,FMHub_Received_ontime as
(
    select 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status = 42
    )
    where 
        rank_num = 1
        and status = 42
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '21:59:59.000'
    group by 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,FMHub_Received_late as
(
    select 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
        select 
            order_tracking.shipment_id
            ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
            ,order_tracking.status
            ,staion_table_name.station_name
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
        on order_tracking.station_id = staion_table_name.station_id
        where 
            order_tracking.status = 42
    )
    where 
        rank_num = 1
        and status = 42
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and split_part(cast(time_stamp AS varchar),' ',2) between '22:00:00.000' and '23:59:59.000'
    group by 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,fmhub_lhtransporting_ontime as
(
    select 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
    select
        order_tracking.shipment_id
        ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
        ,order_tracking.status
        ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
    where 
        order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status = 47
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        -- and date(time_stamp) = date('2022-10-21')
        -- and station_name = 'FPPIN - พุนพิน (U-412)'
        and split_part(cast(time_stamp AS varchar),' ',2) between '00:00:00.000' and '22:59:59.000'
    group by 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,fmhub_lhtransporting_late as
(
    select 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2) as split_time_stamp
    from 
    (
    select
        order_tracking.shipment_id
        ,FROM_UNIXTIME(order_tracking.ctime-3600) as time_stamp
        ,order_tracking.status
        ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
    where 
        order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status = 47
        and date(time_stamp) between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
        and split_part(cast(time_stamp AS varchar),' ',2) between '23:00:01.000' and '23:59:59.000'
    group by 
        shipment_id
        ,time_stamp
        ,split_part(cast(time_stamp AS varchar),' ',2)
)
,FMHub_Packing as 
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status = 43
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time    
)
,FMHub_Packed as 
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status = 44
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time    
)
,FMHub_LHPacked as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status = 46
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
)
,lost as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status = 11
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
)
,Damaged as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status = 12
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
)
,Return_all as
(
    select 
        shipment_id
        ,date_time
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,row_number() over (partition by order_tracking.shipment_id,date(FROM_UNIXTIME(order_tracking.ctime-3600)) order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        where 
            order_tracking.status in (43,44,46,47,10,58,67,11,12)
    )
    where 
        rank_num = 1
        and status in (10,58,67)
        and date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
    group by 
        shipment_id
        ,date_time
)
select 
    FMHub_Received.date_time
    ,FMHub_Received.station_name
    ,substring(split_part(FMHub_Received.station_name,' -',1),2,10) AS hub_name
    ,count(FMHub_Received.shipment_id) as total_FMHub_Received
    ,count(FMHub_Received_ontime.shipment_id) as ontime_received_10pm
    ,count(FMHub_Received_late.shipment_id) as late_received
    ,count(fmhub_lhtransporting_ontime.shipment_id) + count(fmhub_lhtransporting_late.shipment_id) as infull_fm_ting
    ,count(fmhub_lhtransporting_ontime.shipment_id) as ontime_lhtransporting_11pm
    ,count(fmhub_lhtransporting_late.shipment_id) as late_lhtransporting
    ,count(FMHub_Packing.shipment_id) as total_FMHub_Packing
    ,count(FMHub_Packed.shipment_id) as total_FMHub_Packed
    ,count(FMHub_LHPacked.shipment_id) as total_FMHub_LHPacked
    ,count(lost.shipment_id) as total_lost
    ,count(Damaged.shipment_id) as total_Damaged
    ,count(Return_all.shipment_id) as total_Return_all
    ,count(FMHub_Received.shipment_id) - (count(fmhub_lhtransporting_ontime.shipment_id) + count(fmhub_lhtransporting_late.shipment_id)) as hub_backlog
    ,(CAST(count(fmhub_lhtransporting_ontime.shipment_id) AS DOUBLE) + CAST(count(fmhub_lhtransporting_late.shipment_id) AS DOUBLE)) / count(FMHub_Received.shipment_id) as "%infull_fm_ting"
    ,CAST(count(fmhub_lhtransporting_ontime.shipment_id) AS DOUBLE) / count(FMHub_Received.shipment_id) as "%ontime_lhtransporting"
    ,(CAST(count(FMHub_Received.shipment_id) AS DOUBLE) - (CAST(count(fmhub_lhtransporting_ontime.shipment_id) AS DOUBLE) + CAST(count(fmhub_lhtransporting_late.shipment_id) AS DOUBLE))) / count(FMHub_Received.shipment_id) as "%hub_backlog"
from FMHub_Received
left join FMHub_Received_ontime
on FMHub_Received.shipment_id = FMHub_Received_ontime.shipment_id
and FMHub_Received.date_time = date(FMHub_Received_ontime.time_stamp)
left join FMHub_Received_late
on FMHub_Received.shipment_id = FMHub_Received_late.shipment_id
and FMHub_Received.date_time = date(FMHub_Received_late.time_stamp)
left join fmhub_lhtransporting_ontime
on FMHub_Received.shipment_id = fmhub_lhtransporting_ontime.shipment_id
and FMHub_Received.date_time = date(fmhub_lhtransporting_ontime.time_stamp)
left join fmhub_lhtransporting_late
on FMHub_Received.shipment_id = fmhub_lhtransporting_late.shipment_id
and FMHub_Received.date_time = date(fmhub_lhtransporting_late.time_stamp)
left join FMHub_Packing
on FMHub_Received.shipment_id = FMHub_Packing.shipment_id
and FMHub_Received.date_time = FMHub_Packing.date_time
left join FMHub_Packed
on FMHub_Received.shipment_id = FMHub_Packed.shipment_id
and FMHub_Received.date_time = FMHub_Packed.date_time
left join FMHub_LHPacked
on FMHub_Received.shipment_id = FMHub_LHPacked.shipment_id
and FMHub_Received.date_time = FMHub_LHPacked.date_time
left join lost
on FMHub_Received.shipment_id = lost.shipment_id
and FMHub_Received.date_time = lost.date_time
left join Damaged
on FMHub_Received.shipment_id = Damaged.shipment_id
and FMHub_Received.date_time = Damaged.date_time
left join Return_all
on FMHub_Received.shipment_id = Return_all.shipment_id
and FMHub_Received.date_time = Return_all.date_time
where  
    FMHub_Received.station_name is not null
    and FMHub_Received.date_time between date(DATE_TRUNC('day', current_timestamp) - interval '45' day) and date(DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '23' hour + interval '59' minute + interval '59' second )
group by 
    FMHub_Received.date_time
    ,FMHub_Received.station_name
order by
    FMHub_Received.date_time desc