with station AS (
    SELECT
        CAST(num_col AS INTEGER) station_id
    FROM
        (VALUES
            (SEQUENCE(0, 
                    9000, 
                    1)
            )
        ) AS t1(no_array)
    CROSS JOIN
        UNNEST(no_array) AS t2(num_col)
        
    --UNION SELECT 123 AS xxx
)
,ref AS 
(
SELECT *
FROM temp_date 
CROSS JOIN station
)
,fact_fleet_order as 
(
select  
    fleet_order.shipment_id
    ,fleet_order.station_id
    ,try(split(station_detail.station_name, ' ')[1]) as station_name
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join spx_mart.shopee_fms_th_db__station_tab__th_continuous_s0_live as station_detail
on fleet_order.station_id = station_detail.id
where
    substring(station_detail.station_name, 1, 1) = 'H'
)
,all_driver AS 
(
select  
    driver_id
    ,driver_name
    ,type_suffix
    ,case
        when contract_type in (1, 3) then 'own fleet'
        when contract_type = 4 then 'Subcon'
        else 'Ops sup'
    end as contract_type
    ,case SUBSTR(type_suffix, 3, 1) 
        when 'R' then 0 
        when 'D' then 1 
    end as fleet_type -- 0 is rider, 1 is driver
    ,case   
        when SUBSTR(type_suffix, 1, 2) = 'SC' then 0
        else 1
    end as is_own_fleet -- must to filter driver only by is_own_fleet
from
    (
    select  
        driver_id
        ,driver_name
        ,CAST(contract_type AS INTEGER) AS contract_type
        ,SUBSTR(driver_name, (LENGTH(driver_name) - 2), 3) type_suffix
    from spx_mart.shopee_fms_th_db__driver_tab__th_continuous_s0_live as driver_tab
    ) AS driver_tab
where   
    type_suffix in ('FTR', 'TER', 'FTD', 'TED', 'SCR', 'SCD')
    and 
    case 
        when contract_type in (1, 3) then 'own fleet'
        when contract_type = 4 then 'Subcon'
        else 'Ops sup'
    end not in ('Ops sup')
)
,received_order AS 
(
select  
    fact_fleet_order.shipment_id
    ,fact_fleet_order.station_id
    -- total received no cut off by date
    ,from_unixtime(min(case when order_tracking.status = 1 and order_tracking.station_id = fact_fleet_order.station_id then order_tracking.ctime-3600 end)) as first_hub_received_time
FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
LEFT JOIN fact_fleet_order
ON order_tracking.shipment_id = fact_fleet_order.shipment_id
where   
    order_tracking.status in (1)
GROUP BY 
    fact_fleet_order.shipment_id
    ,fact_fleet_order.station_id
)
,receive_AGG as 
(
select   
    DATE(first_hub_received_time) as received_date 
    ,station_id
    ,count(distinct shipment_id) as total_hub_received -- total received no cut off by date
from received_order
GROUP BY 
    DATE(first_hub_received_time)
    ,station_id
)
,assign_order_track AS 
(
SELECT
    order_raw.*
    ,IF( user_id_temp != 0, user_id_temp, assignment_task.driver_id) AS user_id
FROM 
    (
    SELECT 
        DISTINCT DATE(FROM_UNIXTIME(ctime - 3600)) AS report_assigned -- assigned 1 time per day (count everyday)
        ,shipment_id
        ,FROM_UNIXTIME(ctime - 3600) AS assigned_time
        ,operator
        ,user_id AS user_id_temp
        ,TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.assignment_task_id') AS varchar)) AS assignment_task_id
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    WHERE 
        status IN (2) 
        AND operator NOT LIKE '%SPX%'
    ) AS order_raw
LEFT JOIN spx_mart.shopee_fms_th_db__assignment_task_tab__th_daily_s0_live AS assignment_task
ON assignment_task.assignment_task_id = order_raw.assignment_task_id
)
,assign_order AS 
(
SELECT 
    assign_order_track.shipment_id
    ,assign_order_track.report_assigned
    ,assign_order_track.assigned_time
    ,assign_order_track.user_id
    ,fact_fleet_order.station_id
    ,buyer_info.buyer_addr_district
    ,all_driver.*
FROM assign_order_track
LEFT JOIN fact_fleet_order
ON assign_order_track.shipment_id = fact_fleet_order.shipment_id
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__th_daily_s0_live AS buyer_info
ON assign_order_track.shipment_id = buyer_info.shipment_id
INNER JOIN all_driver 
ON assign_order_track.user_id = all_driver.driver_id
)
,assigned_AGG as 
(
select  
    report_assigned
    ,driver_id
    ,driver_name
    ,CASE fleet_type 
        WHEN 0 THEN '2W' 
        WHEN 1 THEN '4W' 
    END AS fleet_type
    ,station_id
    ,count(distinct shipment_id) as total_assigned
    -- 10.00
    ,count(case when assigned_time <= CAST(report_assigned AS timestamp) + interval '10' hour then shipment_id end) as total_assigned_10_00
from  assign_order
group by 
    report_assigned
    ,driver_id
    ,driver_name
    ,CASE fleet_type 
        WHEN 0 THEN '2W' 
        WHEN 1 THEN '4W' 
    END
    ,station_id-- unitil line 159 rechecked 
)
,deliver_order_track AS 
(
SELECT
    order_raw.*
    ,IF( user_id_temp != 0, user_id_temp, assignment_task.driver_id) AS user_id
    FROM 
    (
    SELECT 
        DISTINCT DATE(FROM_UNIXTIME(ctime - 3600)) AS report_delivered, -- delivered day (count everyday)
        shipment_id
        ,FROM_UNIXTIME(ctime - 3600) AS delivered_time
        ,operator
        ,user_id AS user_id_temp
        ,TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.assignment_task_id') AS varchar)) AS assignment_task_id
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    WHERE 
        status IN (4) 
        AND operator NOT LIKE '%SPX%'
    ) AS order_raw
LEFT JOIN spx_mart.shopee_fms_th_db__assignment_task_tab__th_daily_s0_live AS assignment_task
ON assignment_task.assignment_task_id = order_raw.assignment_task_id
)
,deliver_order AS 
(
SELECT 
    deliver_order_track.shipment_id
    ,deliver_order_track.report_delivered
    ,deliver_order_track.delivered_time
    ,deliver_order_track.user_id
    ,fact_fleet_order.station_id
    ,buyer_info.buyer_addr_district
    ,all_driver.*
FROM deliver_order_track
LEFT JOIN fact_fleet_order
ON deliver_order_track.shipment_id = fact_fleet_order.shipment_id
LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__th_daily_s0_live AS buyer_info
ON deliver_order_track.shipment_id = buyer_info.shipment_id
INNER JOIN all_driver 
ON deliver_order_track.user_id = all_driver.driver_id
)
,delivered_AGG AS 
(
select  
    report_delivered
    ,driver_id
    ,driver_name
    ,CASE fleet_type 
        WHEN 0 THEN '2W' 
        WHEN 1 THEN '4W' 
    END AS fleet_type
    ,station_id
    ,count(distinct shipment_id) as total_delivered
    -- 10.00
    ,count(case when delivered_time <= CAST(report_delivered AS timestamp) + interval '10' hour then shipment_id end) as total_delivered_10_00
from deliver_order
group by 
    report_delivered
    ,driver_id
    ,driver_name
    ,CASE fleet_type 
        WHEN 0 THEN '2W' 
        WHEN 1 THEN '4W' 
    END
    ,station_id
)
,Total_AGG AS 
( 
SELECT 
    assigned_AGG.report_assigned
    ,assigned_AGG.driver_id
    ,assigned_AGG.driver_name
    ,assigned_AGG.fleet_type
    ,assigned_AGG.station_id
    ,assigned_AGG.total_assigned
    ,delivered_AGG.total_delivered
    ,assigned_AGG.total_assigned_10_00
    ,delivered_AGG.total_delivered_10_00
FROM assigned_AGG 
LEFT JOIN delivered_AGG
ON assigned_AGG.report_assigned = delivered_AGG.report_delivered
AND assigned_AGG.driver_id   = delivered_AGG.driver_id
AND assigned_AGG.driver_name = delivered_AGG.driver_name
AND assigned_AGG.station_id  = delivered_AGG.station_id
)
SELECT 
    ref.report_date
   ,TRY(SPLIT(station.station_name,' ')[1]) AS station_name
   ,Total_AGG.driver_id
   ,Total_AGG.driver_name
   ,Total_AGG.fleet_type
   ,Total_AGG.total_assigned
   ,Total_AGG.total_delivered
   ,Total_AGG.total_assigned_10_00
   ,Total_AGG.total_delivered_10_00
FROM ref
LEFt JOIN spx_mart.shopee_fms_th_db__station_tab__th_continuous_s0_live AS station
ON ref.station_id = CAST(station.id AS INTEGER)
LEFT JOIN Total_AGG
ON ref.report_date = Total_AGG.report_assigned
AND ref.station_id = Total_AGG.station_id
WHERE station_name LIKE 'H%'
ORDER BY 
    ref.report_date DESC
    ,TRY(SPLIT(station.station_name,' ')[1])

  

   

