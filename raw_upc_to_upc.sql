/*
rc received date	shipment id	pickup_rc_station_name	delivery_rc_station_name	soc_handover timestamp
min soc_received		NORC-A	SORC-A	"if station soc_handover = pickup station = show timestamp
if not null"
หา Min soc_received เริ่ม ที่ D-1 ก่อน
1.rc received date 
2.shipment id
3.pickup_rc_station_name คือ station Min soc_received
4.delivery_rc_station_name คือ ไปเช็ค destsination_name ว่าอยู่ ใน Station RC ไหน
5. soc_handover timestamp เงื่อนไขคือ "if station soc_handover = pickup station = show timestamp else  not null"
*/
-- SPXTH02044840655B
with cte as 
(
select 
    date(min_soc_received.time_stamp) as rc_received_date
    ,min_soc_received.shipment_id
    ,min_soc_received.station_name as pickup_rc_station_name
    ,last_status_order.status_name
from 
(
select 
    order_track.shipment_id
    ,from_unixtime(order_track.ctime-3600) as time_stamp
    ,order_track.status
    ,staion_table_name.station_name
    -- ,try(cast(json_extract(json_parse(content),'$.station_id') as int)) as pickup_rc_station_name 
    -- ,min(if(status = 39,date(FROM_UNIXTIME(ctime-3600)),null)) as date_FMHub_Pickup_Done
    ,station_id
    ,row_number() over (partition  by order_track.shipment_id order by from_unixtime(order_track.ctime-3600) asc) as rank_num
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
on order_track.station_id = staion_table_name.id
where 
    order_track.status = 8
    -- and date(from_unixtime(order_track.ctime-3600)) between date('2022-11-22') and date('2022-11-23')
) as min_soc_received
inner join 
(
select 
    last_status_name.shipment_id
    ,last_status_name.time_stamp
    ,thspx_fact_order_tracking_di_th.status_name
    ,row_number() over (partition by last_status_name.shipment_id order by last_status_name.time_stamp desc) as rank_num_last_status_duplicate
from 
    (
        select 
            status_name_order.shipment_id
            ,status_name_order.status
            ,from_unixtime(ctime-3600) as time_stamp
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as status_name_order
        inner join
        (
        select 
            shipment_id
            -- ,status
            ,max(FROM_UNIXTIME(ctime-3600)) as max_time_stamp
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
        -- where
        --     shipment_id = 'SPXTH02549630599B'
        group by 
            shipment_id
        ) last_status
        on status_name_order.shipment_id = last_status.shipment_id
        and from_unixtime(ctime-3600) = last_status.max_time_stamp
    ) last_status_name
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
on last_status_name.status = thspx_fact_order_tracking_di_th.status
) last_status_order
on min_soc_received.shipment_id = last_status_order.shipment_id
where 
    rank_num = 1    
    and rank_num_last_status_duplicate = 1
    and min_soc_received.time_stamp between DATE_TRUNC('day', current_timestamp) - interval '1' day + interval '6' hour and DATE_TRUNC('day', current_timestamp) + interval '6' hour
    -- and date(min_soc_received.time_stamp) between date('2022-11-22') and date('2022-11-23')
    -- หกโมงเช้าเมื่อวานถึงหกโมงเช้าวันนี้
group by 
    date(min_soc_received.time_stamp)
    ,min_soc_received.shipment_id
    ,min_soc_received.station_name 
    ,last_status_order.status_name
)
,cte1 as 
(
select 
    fleet_order.shipment_id
    ,staion_table_name.station_name
    ,staion_table_name2.station_name as dest_station_name
    ,case
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') then 'NORC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') then 'NORC-B'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') then 'NERC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') then 'NERC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') then 'SORC-A' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') then 'SORC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') then 'CERC'
        else pub_shipment.buyer_region_name
    end as rc_delivery_station
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
on fleet_order.shipment_id = pub_shipment.shipment_id
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
on fleet_order.pickup_station_id = staion_table_name.id
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name2
on fleet_order.station_id = staion_table_name2.id
-- where 
--     fleet_order.shipment_id = 'SPXTH01106545117C'
group by
    fleet_order.shipment_id
    ,staion_table_name.station_name
    ,staion_table_name2.station_name
    ,case
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') then 'NORC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') then 'NORC-B'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') then 'NERC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') then 'NERC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') then 'SORC-A' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') then 'SORC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') then 'CERC'
        else pub_shipment.buyer_region_name
    end
)
,min_soc_handover_order as
(
select 
    time_stamp as min_soc_handover_timestamp
    ,operator
    ,min_soc_handover.shipment_id
    ,min_soc_handover.station_name as soc_handover_station
from 
(
select 
    order_track.shipment_id
    ,from_unixtime(order_track.ctime-3600) as time_stamp
    ,order_track.status
    ,order_track.operator
    ,staion_table_name.station_name 
    ,try(cast(json_extract(json_parse(content),'$.station_id') as int)) as station_id
    ,row_number() over (partition  by order_track.shipment_id order by from_unixtime(order_track.ctime-3600) asc) as rank_num
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
on try(cast(json_extract(json_parse(content),'$.station_id') as int)) = staion_table_name.id
where 
    order_track.status = 18
    -- and date(from_unixtime(order_track.ctime-3600)) between date('2022-11-22') and date('2022-11-23')
) as min_soc_handover
where 
    rank_num = 1    
    -- and date(time_stamp) between date('2022-11-22') and date('2022-11-23')
)
select 
    cte.rc_received_date
    ,cte.shipment_id
    ,cte.pickup_rc_station_name
    ,cte1.rc_delivery_station
    ,cte.status_name as last_staus_name
    -- ,min_soc_handover_order.soc_handover_station
    ,if(min_soc_handover_order.soc_handover_station = cte.pickup_rc_station_name,min_soc_handover_timestamp,null) as min_soc_handover_time_stamp
    ,min_soc_handover_order.operator
from cte
inner join cte1
on cte.shipment_id = cte1.shipment_id
left join min_soc_handover_order
on cte.shipment_id = min_soc_handover_order.shipment_id
where
    pickup_rc_station_name in ('NORC-A','NORC-B','NERC-A','NERC-B','SORC-A','SORC-B','CERC')
group by 
    cte.rc_received_date
    ,cte.shipment_id
    ,cte.pickup_rc_station_name
    ,cte1.rc_delivery_station
    ,cte.status_name
    ,if(min_soc_handover_order.soc_handover_station = cte.pickup_rc_station_name,min_soc_handover_timestamp,null)
    ,min_soc_handover_order.operator
