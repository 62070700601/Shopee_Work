with pickup_date_cal AS 
(
SELECT
    shipment_id
    ,CASE WHEN (station_name LIKE 'D%' or station_name LIKE 'P%')  THEN dropoff_date ELSE pickup_date END AS pickup_done_date
    ,station_name AS dropoff_hub
FROM 
    (
    SELECT
        shipment_id
        ,MIN(CASE 
                WHEN status IN (13,39,42,8) THEN DATE(FROM_UNIXTIME(ctime-3600)) 
                WHEN status = 112 THEN DATE(FROM_UNIXTIME(ctime-3600)+INTERVAL '9' HOUR)  
            END
            ) AS pickup_date
        ,MIN(CASE WHEN status = 42 THEN DATE(FROM_UNIXTIME(ctime-3600)+ INTERVAL '9' HOUR) END) AS dropoff_date
        ,MIN(CASE WHEN status = 42 THEN TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.station_id') AS INTEGER)) END) AS dropoff_hub_id
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    --where date(from_unixtime(ctime-3600)) > CURRENT_DATE - interval '40' day 
    GROUP BY shipment_id
    ) order_tracking
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live station
ON order_tracking.dropoff_hub_id = CAST(station.id AS INTEGER)
)
,order_path_fm_hub as 
(
select 
    shipment_id
    ,station.station_name as last_station_name 
from 
    (
    select 
        shipment_id 
        ,order_path
        ,try(cast(replace(ltrim(split_part(order_path,',',cardinality(CAST(JSON_PARSE(order_path) AS ARRAY<INT>)))),']','') as int)) as last_staton_id
    from 
        (
        select 
            shipment_id 
            ,order_path
            ,ROW_NUMBER() over(partition  by shipment_id order by ctime) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 42 
            and date(from_unixtime(ctime - 3600)) >= current_date - interval '40' day 
        )
    where rank_num = 1 
    ) as order_path_check 
 left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station
 on order_path_check.last_staton_id = station.id
)
,origin_order_path as
(
select 
    shipment_id
    ,last_staton_id
    ,station.station_name
from     
    (
    select 
        shipment_id 
        ,origin_order_path
        ,try(cast(replace(ltrim(split_part(origin_order_path,',',cardinality(CAST(JSON_PARSE(origin_order_path) AS ARRAY<INT>)))),']','') as int)) as last_staton_id 
    from spx_mart.shopee_fleet_order_th_db__fleet_order_extension_tab__reg_daily_s0_live
    where 
        origin_order_path != '[]' 
        and date(from_unixtime(mtime)) >= current_date - interval '60' day 
    )as odp
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station
on odp.last_staton_id = station.id
),
raw_4pl as
(
select 
    fleet_order.shipment_id 
    ,CASE 
        WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 THEN 1 \
        ELSE 0 
    END AS is_4pl
    ,CASE 
        WHEN fleet_order.station_id = 37 THEN 'flash'
        WHEN fleet_order.channel_id = 50002 THEN 'flash'
        WHEN fleet_order.station_id IN (121,122) THEN 'cj'
        WHEN fleet_order.channel_id = 50003 THEN 'cj'
        WHEN fleet_order.station_id = 188 THEN 'njv'
        WHEN fleet_order.station_id = 25 THEN 'jnt'
        WHEN fleet_order.channel_id = 50001 THEN 'jnt'
        WHEN fleet_order.station_id = 5 or fleet_order.station_id = 170 THEN 'kerry'
        -- WHEN station.station_type = 5 THEN 'non-integrated'
    else 'spx' END as delivery_station
    ,case 
        when origin_order_path.station_name is not null then origin_order_path.station_name 
        else order_path_fm_hub.last_station_name 
    end as orgin_last_station
    ,CASE 
        WHEN buyer_info.buyer_addr_state IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 1 
        ELSE 0 
    END AS is_gbkk
    ,1.000*fleet_order.chargeable_weight/1000 as chargeable_weight
    ,DATE(status_order.handover_time - interval '6' hour) as handover_date 
    ,if( hour(status_order.handover_time) >= 2 and  hour(status_order.handover_time) <= 6,1,0) as hand_over_after_2am
    ,pickup_date_cal.pickup_done_date as picked_up_date
    ,date(status_order.delivered_time_4pl) as delivered_date_4pl 
    ,buyer_info.buyer_addr_district as buyer_district
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join
    ( 
    SELECT 
        shipment_id
        --,MIN(case when status in (8,13,39,42) then FROM_UNIXTIME(ctime-3600) end)  as pick_up_time
        ,MIN(case when status in (18,89) then FROM_UNIXTIME(ctime-3600) end)  as handover_time 
        ,MIN(case when status in (2,91,80,81) then FROM_UNIXTIME(ctime-3600) end)  as first_assigned_time
        ,MIN(case when status = 81 then FROM_UNIXTIME(ctime) end)  as delivered_time_4pl
        ,MIN(case when status = 8 THEN FROM_UNIXTIME(ctime -3600) END ) as soc_received_time 
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
    group by shipment_id 
    ) as status_order
ON status_order.shipment_id = fleet_order.shipment_id
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live AS buyer_info
ON fleet_order.shipment_id = buyer_info.shipment_id
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live AS station 
ON fleet_order.station_id = station.id
LEFT JOIN pickup_date_cal
on pickup_date_cal.shipment_id = fleet_order.shipment_id
--where pickup_date_cal.pickup_done_date > current_date - interval '180' day   
)
select 
    buyer_district
    ,case  
        when chargeable_weight <=0.5 then '00.0-00.5'
        when chargeable_weight <=1.0 then '00.5-01.0'
        when chargeable_weight <=2.0 then '01.0-02.0'
        when chargeable_weight <=3.0 then '02.0-03.0'
        when chargeable_weight <=4.0 then '03.0-04.0'
        when chargeable_weight <=5.0 then '04.0-05.0'
        when chargeable_weight <=6.0 then '05.0-06.0'
        when chargeable_weight <=7.0 then '06.0-07.0'
        when chargeable_weight <=8.0 then '07.0-08.0'
        when chargeable_weight <=9.0 then '08.0-09.0'
        when chargeable_weight <=10.0 then '09.0-10.0'
        when chargeable_weight <=11.0 then '10.0-11.0'
        when chargeable_weight <=12.0 then '11.0-12.0'
        when chargeable_weight <=13.0 then '12.0-13.0'
        when chargeable_weight <=14.0 then '13.0-14.0'
        when chargeable_weight <=15.0 then '14.0-15.0'
        when chargeable_weight <=16.0 then '15.0-16.0'
        when chargeable_weight <=17.0 then '16.0-17.0'
        when chargeable_weight <=18.0 then '17.0-18.0' 
        when chargeable_weight <=19.0 then '18.0-19.0'
        when chargeable_weight <=20.0 then '19.0-20.0'
        when chargeable_weight >20.0 then '>20.0'
    end as chargeable_weight
    ,count(*) as total_4pl_volume 
/* ,1.00*sum(total_4pl_cost)/count(*) as total_4pl_cpo 
,1.00*sum(shippping_fee_cost)/count(*) as shippingfee_cpo  
,sum(total_4pl_cost) as total_4pl_cost 
,sum(shippping_fee_cost) as shipping_fee_4pl 
,sum(remote_cost) as remote_cost
,sum(cod_cost)
,sum(return_cost) */
from raw_4pl
where delivered_date_4pl between date('2022-06-01') and date('2022-06-30') and is_4pl = 1 
group by 1,2
order by 1,2