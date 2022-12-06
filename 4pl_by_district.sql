WITH temp_date AS
(
SELECT
    CAST(date_column AS DATE) as  report_date
FROM
    (VALUES
        (SEQUENCE(CURRENT_DATE - INTERVAL '31' DAY, 
            CURRENT_DATE - INTERVAL '1' DAY, 
            INTERVAL '1' DAY)
        )
    ) AS t1(date_array)
CROSS JOIN
    UNNEST(date_array) AS t2(date_column)
    ),
sla_precale AS 
(
SELECT 
    CAST(report_date AS DATE) AS sla_d_0_date,
    CAST(sla_d1 AS DATE) AS sla_d_1_date,
    CAST(sla_d2 AS DATE) AS sla_d_2_date,
    CAST(sla_d3 AS DATE) AS sla_d_3_date,
    CAST(sla_d4 AS DATE) AS sla_d_4_date,
    CAST(sla_d5 AS DATE) AS sla_d_5_date,
    CAST(sla_office_d1 AS DATE) AS sla_office_d1,
    CAST(sla_office_d2 AS DATE) AS sla_office_d2,
    CAST(sla_d1 AS TIMESTAMP) AS sla_d_1_time,
    CAST(sla_d2 AS TIMESTAMP) AS sla_d_2_time
FROM dev_thopsbi_lof.spx_analytics_sla_precal_date_v1
)
,pickup_date_cal AS 
(
SELECT
    shipment_id,
    CASE WHEN (station_name LIKE 'D%' or station_name LIKE 'P%')  THEN dropoff_date ELSE pickup_date END AS pickup_done_date,
    station_name AS dropoff_hub
FROM 
    (
    SELECT
        shipment_id,
        MIN(
            CASE 
                WHEN status IN (13,39,42,8) THEN DATE(FROM_UNIXTIME(ctime-3600)) 
                WHEN status = 112 THEN DATE(FROM_UNIXTIME(ctime-3600)+INTERVAL '9' HOUR)  
            END
            ) AS pickup_date,
        MIN(CASE WHEN status = 42 THEN DATE(FROM_UNIXTIME(ctime-3600)+ INTERVAL '9' HOUR) END) AS dropoff_date,
        MIN(CASE WHEN status = 42 THEN TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.station_id') AS INTEGER)) END) AS dropoff_hub_id
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    GROUP BY shipment_id
    ) as order_tracking
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station
ON order_tracking.dropoff_hub_id = CAST(station.id AS INTEGER)
)
,remote_area as
(
select 
    zip_code
    ,is_flash_remote
    ,is_kerry_remote
    ,is_ninjavan_remote
    ,is_jnt_remote
from thopsbi_lof.spx_4pl_remote_district_index
)
,raw_sla as
(
select
    fleet_order.shipment_id
    ,region_mapping.gbkk_upc_ops_region as region 
    ,CASE 
        WHEN buyer_info.location_type = 1 THEN 'Office' 
        ELSE 'Non-Office' 
    END as location_type
    ,buyer_info.buyer_addr_state as buyer_province 
    ,buyer_info.buyer_addr_district as buyer_district 
    ,CASE 
        WHEN buyer_info.buyer_addr_state IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 1 
        ELSE 0 
    END AS is_gbkk
    ,CASE 
        WHEN buyer_info.buyer_addr_state IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') AND pickup_order.seller_addr_state IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 1 
        ELSE 0 
    END AS sla_1_day
    ,CASE 
        WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 THEN 1 
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
        WHEN fleet_order.station_id = 5 or  fleet_order.station_id = 170 THEN 'kerry'
        WHEN station.station_type = 5 THEN 'non-integrated'
        else 'spx' 
    END as delivery_station
    ,if( hour(status_order.handover_time) >=2 and hour(status_order.handover_time) <=6,1,0) as hand_over_after_2am
    ,date(handover_time - interval '6' hour ) as handover_date  
    ,pickup_date_cal.pickup_done_date as pickup_done_date
    ,DATE(status_order.first_assigned_time) as assigned_date
    ,DATE(status_order.delivered_time) as delivered_date
    ,is_flash_remote
    ,is_kerry_remote
    ,is_ninjavan_remote
    ,is_jnt_remote
    ,case 
        when is_flash_remote = 'TRUE' or is_kerry_remote = 'TRUE' or is_ninjavan_remote = 'TRUE' or is_jnt_remote = 'TRUE' then 1 
        else 0 
    end as is_remote
FROM spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live AS fleet_order
LEFT JOIN 
    (
    SELECT 
        shipment_id
        --,min(case when status in (8,13,39,42,112) then FROM_UNIXTIME(ctime-3600) end)  as pick_up_time
        ,min(case when status in (18,89) then FROM_UNIXTIME(ctime-3600) end)  as handover_time 
        ,min(case when status in (2,91,80,81) then FROM_UNIXTIME(ctime-3600) end)  as first_assigned_time
        ,min(case when status in (4,81) then FROM_UNIXTIME(ctime-3600) end)  as delivered_time 
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
    where date(from_unixtime(ctime-3600)) > current_date - interval '40' day 
    group by shipment_id 
    ) as status_order
on fleet_order.shipment_id = status_order.shipment_id
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live AS buyer_info
ON fleet_order.shipment_id = buyer_info.shipment_id
LEFT JOIN spx_mart.shopee_fms_pickup_th_db__pickup_order_tab__reg_daily_s0_live AS pickup_order
ON fleet_order.shipment_id = pickup_order.pickup_order_id
LEFT JOIN thopsbi_lof.spx_index_region_temp AS region_mapping 
ON region_mapping.district = buyer_info.buyer_addr_district
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live AS station 
ON fleet_order.station_id = station.id
LEFT JOIN remote_area
ON try(cast(remote_area.zip_code as int)) = try(cast(buyer_info.buyer_zipcode as int))
LEFT JOIN pickup_date_cal
on pickup_date_cal.shipment_id = fleet_order.shipment_id
where  pickup_date_cal.pickup_done_date > current_date - interval '40' day
),
view_spx as
(
select
    shipment_id
    ,delivery_station
    ,region
    ,location_type
    ,is_gbkk
    ,sla_1_day
    ,is_4pl
    ,hand_over_after_2am
    ,handover_date
    ,pickup_done_date
    ,assigned_date
    ,delivered_date
    ,is_remote
    ,buyer_district
    ,buyer_province
    ,CASE 
        WHEN (is_flash_remote = 'TRUE' and delivery_station = 'flash') or (is_kerry_remote = 'TRUE' and delivery_station = 'kerry') or delivery_station = 'njv'  THEN  sla_d_4_date
        WHEN location_type = 'Office' THEN
            CASE 
                WHEN sla_1_day = 0 THEN sla_office_d2
                WHEN sla_1_day = 1 THEN sla_office_d1 
            END
        WHEN location_type != 'Office' THEN
            CASE 
                WHEN sla_1_day = 0 THEN sla_d_2_date
                WHEN sla_1_day = 1 THEN sla_d_1_date 
            END
    END AS sla_date_spx
from raw_sla
left join sla_precale 
on sla_precale.sla_d_0_date = raw_sla.pickup_done_date 
where is_4pl = 1
),
view_4pl as
(
select
    shipment_id
    ,delivery_station
    ,region
    ,location_type
    ,is_gbkk
    ,sla_1_day
    ,is_4pl
    ,hand_over_after_2am
    ,handover_date
    ,pickup_done_date
    ,assigned_date
    ,delivered_date
    ,is_remote
    ,CASE 
        WHEN (is_flash_remote = 'TRUE' and delivery_station = 'flash') or (is_kerry_remote = 'TRUE' and delivery_station = 'kerry') or delivery_station = 'njv' THEN  sla_d_4_date
        WHEN location_type = 'Office' THEN
            CASE 
                WHEN sla_1_day = 0 THEN sla_office_d2
                WHEN sla_1_day = 1 THEN sla_office_d1 
            END
        WHEN location_type != 'Office' THEN
            CASE 
                WHEN sla_1_day = 0 THEN sla_d_2_date
                WHEN sla_1_day = 1 THEN sla_d_1_date 
            END
        END AS sla_date_4pl
from raw_sla
left join sla_precale 
on sla_precale.sla_d_0_date = raw_sla.handover_date 
where is_4pl = 1
)
,sla_spx as
(
select
    view_spx.*
    ,if(assigned_date <= sla_date_spx,1,0) as is_assigned_ontime_spx
    ,if(delivered_date <= sla_date_spx,1,0) as is_delivered_ontime_spx
from view_spx
)
,sla_4pl as
(
select
    view_4pl.*
    ,if(assigned_date <= sla_date_4pl,1,0) as is_assigned_ontime_4pl
    ,if(delivered_date <= sla_date_4pl,1,0) as is_delivered_ontime_4pl
from view_4pl
)
select
    sla_date_spx
    ,delivery_station
    ,buyer_district
    ,buyer_province
    ,count(shipment_id) as total_4pl_sla_spx
    ,sum(if(is_assigned_ontime_spx = 1 ,1,0)) as ontime_4pl_assigned_spx
    ,sum(if(is_delivered_ontime_spx = 1 ,1,0)) as ontime_4pl_delivered_spx
from sla_spx 
group by
    sla_date_spx
    ,delivery_station
    ,buyer_district
    ,buyer_province
order by 
    sla_date_spx desc 
    ,delivery_station asc
    ,buyer_district asc
    ,buyer_province asc