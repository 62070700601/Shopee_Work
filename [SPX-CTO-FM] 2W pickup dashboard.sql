WITH assign_task AS 
(
SELECT
    pickup_point_id
    ,DATE(FROM_UNIXTIME(ctime-3600)) AS task_create_date
    ,MAX_BY(driver_id,ctime) AS lastest_driver
    ,MAX_BY(operator,ctime) AS lastest_operator,
    MAX_BY(pickup_station_id,ctime) AS lastest_assign_station_id
FROM spx_mart.shopee_fms_pickup_th_db__pickup_task_tab__reg_continuous_s0_live
GROUP BY 
    pickup_point_id
    ,DATE(FROM_UNIXTIME(ctime-3600)) 
    -- WHERE operator IN ('apinya.boo@shopee.com','aun.jirapibu@shopee.com')
)
,data_mart AS 
(
select *
from thopsbi_spx.dwd_pub_shipment_info_df_th
)
,fm_mart AS 
(
SELECT *
from thopsbi_spx.dwd_fm_shipment_info_df_th
-- WHERE pickup_district_name = 'อำเภอสามโคก'
)
,on_hold_by_shop AS 
(
SELECT *
FROM thopsbi_spx.spx_mart_temp_table_arrange_to_pickup_v1
WHERE 
    grass_date = (SELECT MAX(grass_date) FROM thopsbi_spx.spx_mart_temp_table_arrange_to_pickup_v1)
    AND pickup_order_id = 'SPXTH02218228575C'

)
,driver_tab AS 
(
SELECT *
    -- CASE 
        -- WHEN contract_type_name = 'SUBCON' THEN 'SUBCON'
        -- WHEN driver_hub_type = 'FM' THEN 'FM'
        -- WHEN vehicle_type_name = '2WH' AND driver_hub_type = 'LM' THEN 'LM_2WH'
        -- WHEN vehicle_type_name = '4WH' AND driver_hub_type = 'LM' THEN 'LM_4WH'
        -- ELSE 'other'
    -- END AS final_driver_type
    ,CASE 
        WHEN ops_fm_driver_tag = 'SUBCON' AND vehicle_type_name = '2WH' THEN 'SUBCON_2W'
        WHEN ops_fm_driver_tag = 'SUBCON' THEN 'SUBCON'
        WHEN ops_fm_driver_tag LIKE 'FM%' THEN 'FM'
        WHEN ops_fm_driver_tag LIKE '%2W' THEN 'LM_2WH'
        WHEN ops_fm_driver_tag LIKE '%4W' THEN 'LM_4WH'
        ELSE 'other'
    END final_driver_type
    -- ops_driver_station_name
FROM thopsbi_spx.dim_driver_info_di_th
WHERE 
    ingestion_timestamp = (SELECT MAX(ingestion_timestamp) FROM thopsbi_spx.dim_driver_info_di_th)
    -- AND driver_id = '18389'
)
,station_agg AS 
(
SELECT 
    SUBSTR(station_code,2) AS sub_station_code
    ,COUNT(station_name) AS total_station
    ,ARRAY_AGG(station_name) AS agg_station
    ,TRY(SPLIT(CASE WHEN MAX(station_name) LIKE 'H%' THEN MAX(station_name) ELSE CONCAT('H',SUBSTR(MAX(station_name),2)) END,' ')[1]) AS  final_hub_name
FROM spx_mart.shopee_fms_th_db__station_tab__reg_daily_s0_live
WHERE 
    station_code LIKE 'H%' 
    OR station_code LIKE 'F%'
GROUP BY 
    SUBSTR(station_code,2)
)
,station_tab AS 
(
SELECT 
    id
    ,station_name
    ,final_hub_name
FROM spx_mart.shopee_fms_th_db__station_tab__reg_daily_s0_live station_tab
LEFT JOIN station_agg
ON SUBSTR(station_tab.station_code,2) = station_agg.sub_station_code
)
-- ,hub_map AS (
--     SELECT pickup_point_id,
--     split_hub AS lm_hub_name
--     FROM thopsbi_spx.fm_pickup_point_map_hub_hlang_2w_project
-- )
,rp_name AS 
(
SELECT 
    email
    -- CASE WHEN type LIKE 'RP%' THEN 'RP' ELSE type END AS type  
    ,MIN(type) AS type
FROM thopsbi_spx.ing_fm_assign_email_type
-- WHERE email = 'tong.bundao@shopee.com'
GROUP BY 
    email
)
,ontime_flag AS 
(
SELECT *
FROM thopsbi_spx.dwd_pub_shipment_sla_df_th
)
,shipment_detail AS 
(
SELECT 
    fm_mart.shipment_id
    ,DATE(fm_mart.pickup_sla_timestamp) AS arrange_date
    ,fm_mart.pickup_district_name
    ,fm_mart.pickup_province_name
    ,CASE 
        WHEN fm_mart.pickup_province_name IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี','จังหวัดสมุทรสาคร') THEN 'GBKK' 
        ELSE 'UPC' 
    END AS pickup_area
    ,fm_mart.fm_hub_inbound_timestamp
    ,data_mart.cancelled_timestamp
    ,fm_mart.pickup_shop_id
    ,fm_mart.pickup_done_timestamp
    ,DATE(fm_mart.pickup_done_timestamp) AS pickup_done_date
    ,DATE(fm_mart.fm_hub_inbound_timestamp) AS fm_inbound_date
    ,CASE 
        WHEN data_mart.cancelled_timestamp IS NOT NULL THEN NULL 
        WHEN fm_mart.pickup_sla_timestamp IS NULL THEN NULL
        WHEN DATE(fm_mart.fm_hub_inbound_timestamp) <= DATE(fm_mart.pickup_sla_timestamp) THEN TRUE
        ELSE FALSE 
    END AS is_pickup_ontime
    ,CASE 
        WHEN fm_mart.fm_hub_inbound_timestamp IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_pickup_infull
    ,CASE 
        WHEN data_mart.is_dropoff = true THEN 'DROP_OFF'
        WHEN data_mart.is_open_service = true THEN 'OSV'
        -- WHEN data_mart.is_bulky = true THEN 'BULKY'
        ELSE 'MKP'
    END AS order_type
    ,CASE 
        WHEN fm_mart.pickup_shop_id LIKE 'NS%' THEN 'NS_SELLER'
        WHEN fm_mart.pickup_shop_id LIKE 'DOP%' AND LOWER(fm_mart.pickup_seller_name) LIKE 'shopee%' THEN 'PS_SELLER'
        WHEN fm_mart.pickup_shop_id LIKE 'DOP%' THEN 'SDOP_SELLER'
        ELSE 'MKP_SELLER'
    END AS seller_type
    ,data_mart.manual_package_weight_in_kg
    ,(COALESCE(data_mart.manual_package_length_in_cm/100,0)) * (COALESCE(data_mart.manual_package_width_in_cm/100,0)) * (COALESCE(data_mart.manual_package_height_in_cm/100,0)) AS volume_m3
    ,fm_mart.pickup_done_driver_id
    ,CASE 
        WHEN fm_mart.dop_type = 'SDOP' THEN 'SDOP' 
        WHEN fm_mart.dop_type = 'Parcel' THEN 'Parcel_shop' 
        ELSE 'Seller' 
    END AS pickup_type
    ,CAST(on_hold_by_shop.on_hold_reason AS INTEGER) AS on_hold_reason
    ,on_hold_by_shop.is_assign_ontime
    ,on_hold_by_shop.is_seller_dropoff_ontime
    ,driver_tab.contract_type_name
    ,driver_tab.fm_fleet_type
    ,driver_tab.vehicle_type_name
    ,fm_mart.pickup_point_id
    -- hub_map.lm_hub_name,
    -- COALESCE(fm_lh_transporting_timestamp,fm_lh_transported_timestamp) AS fm_hub_outbound_time,
    ,fm_hub_outbound_timestamp
    ,on_time_fm_hub_outbound_sla_flag AS is_fm_hub_outbound_ontime
    ,on_time_fm_hub_received_sla_flag AS is_fm_hub_receive_ontime
    ,on_time_fm_hub_packed_sla_flag AS is_fm_hub_packed_ontime
    ,on_time_fm_lh_transporting_sla_flag AS is_fm_hub_lhing_ontime
    ,on_time_fm_lh_transported_sla_flag AS is_fm_hub_lhed_ontime
    ,on_time_pickup_soc_received_sla_flag AS is_soc_receive_ontime
    ,final_driver_type
    ,CASE 
        WHEN final_driver_type = 'FM' THEN 'RP'
        WHEN final_driver_type IN ('LM_4WH','LM_2WH') THEN 'HUB'
        WHEN final_driver_type = 'SUBCON' THEN 'RP'
        WHEN final_driver_type = 'SUBCON_2W' THEN 'RP'
        WHEN rp_name.type LIKE 'H%' THEN 'HUB'
    ELSE 'RP' END AS hub_assign_type
    -------------------
    -- COALESCE(s1.final_hub_name,s2.final_hub_name) AS final_hub_receive_name,
    -------------------
    -- CASE 
    --     WHEN final_driver_type = 'FM' THEN 'HLANG-A'
    --     WHEN final_driver_type IN ('LM_4WH','LM_2WH') THEN 'HLANG-B'
    --     WHEN final_driver_type = 'SUBCON' THEN 'HLANG-A'
    --     WHEN rp_name.type LIKE 'H%' THEN rp_name.type
    -- ELSE 'HLANG-A' END AS final_hub_name
    ,ops_driver_station_name
    ,COALESCE
    (
    CASE 
        WHEN ops_driver_station_name = 'OTHER' THEN COALESCE(s1.final_hub_name,s2.final_hub_name)
        WHEN fm_mart.pickup_district_name IN ('อำเภอเมืองปทุมธานี','อำเภอสามโคก') AND vehicle_type_name LIKE '%4%' THEN 'HPTUM-A' 
        ELSE ops_driver_station_name 
    END
    ,SUBSTR(rp_name.type,6)
    ) AS final_hub_name
    ,COALESCE
    (
    CASE 
        WHEN ops_driver_station_name = 'OTHER' THEN COALESCE(s1.final_hub_name,s2.final_hub_name)
        WHEN fm_mart.pickup_district_name IN ('อำเภอเมืองปทุมธานี','อำเภอสามโคก') AND vehicle_type_name LIKE '%4%' THEN 'HTYBR-A' 
        WHEN COALESCE(ops_driver_station_name,SUBSTR(rp_name.type,6)) IN ('HLANG-B') THEN 'HLANG-A'
        ELSE ops_driver_station_name 
    END
    ,SUBSTR(rp_name.type,6)
    ) AS final_hub_receive_name
    ,assign_task.lastest_operator
    ,assign_task.lastest_assign_station_id
    -- seller >> เมืองประทุม และ สามโคก จะวิ่งเข้า Fปทุม >> ถ้า 4WH ไปรับก็ให้ ไปธัญญะ
FROM fm_mart
LEFT JOIN data_mart
ON fm_mart.shipment_id = data_mart.shipment_id
LEFT JOIN on_hold_by_shop
ON fm_mart.shipment_id = on_hold_by_shop.pickup_order_id
LEFT JOIN driver_tab
ON fm_mart.pickup_done_driver_id = driver_tab.driver_id
LEFT JOIN station_tab AS s1 
ON fm_mart.fm_hub_received_station_id = CAST(s1.id AS VARCHAR)
-- INNER JOIN hub_map
-- ON fm_mart.pickup_point_id = hub_map.pickup_point_id
LEFT JOIN ontime_flag
ON fm_mart.shipment_id = ontime_flag.shipment_id
LEFT JOIN assign_task
ON DATE(fm_mart.pickup_sla_timestamp) = assign_task.task_create_date
AND fm_mart.pickup_point_id = assign_task.pickup_point_id
LEFT JOIN station_tab AS s2 
ON CAST(assign_task.lastest_assign_station_id AS VARCHAR) = CAST(s2.id AS VARCHAR)
LEFT JOIN rp_name
ON assign_task.lastest_operator = rp_name.email
WHERE 
    is_warehouse = false
    AND is_cross_border = false
    AND
    (
    COALESCE(ops_driver_station_name,SUBSTR(rp_name.type,6)) IN ('HLANG-B','HLANG-A','HPTUM-A','HPTUM-B','HYNWA','OTHER','HSMPK-C','HPKNG','HBGNA','HBKPI-B','HSAPA','HBYAI','HTHON')
    OR fm_mart.pickup_district_name IN ('อำเภอเมืองปทุมธานี','อำเเภอสามโคก')
    )
    -- AND fm_mart.pickup_done_driver_id = '18627'
)
-- SELECT *
-- FROM shipment_detail
-- WHERE  shipment_id = 'SPXTH02218228575C'
-- -- WHERE ops_driver_station_name = 'HPTUM-B'
-- -- -- AND final_driver_type = 'SUBCON'
-- -- -- AND hub_assign_type = 'RP'e
,shipment_agg AS 
(
SELECT 
    shipment_detail.*
    ,CASE 
        WHEN on_hold_reason IN (35,74,36,73,37,72,56,70,57,69,58,68,61,65) THEN TRUE 
        ELSE FALSE 
    END AS is_seller_fault
    ,CASE 
        WHEN fm_hub_outbound_timestamp IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_fm_hub_outbound_infull
FROM shipment_detail
WHERE 
    final_hub_name IN ('HLANG-B','HLANG-A','HPTUM-A','HPTUM-B','HYNWA','FTYBR-A','HSMPK-C','HPKNG','HBGNA','HBKPI-B','HSAPA','HBYAI','HTHON')
)
-------------------================= คำอธิบาย มันจะมu hub ที่ pickup done กัย receive แยกกัน แล้ว ก็คนassign มีได้ว่าเป็น RP หรือ HUB ====================--------
,pickup_hub_arrange_agg_1 AS 
(
SELECT 
    arrange_date
    ,final_hub_name AS pickup_hub_name
    ,hub_assign_type
    ,order_type
    ,pickup_shop_id
    ,COUNT(*) AS total_order_arrange
    ,COUNT_IF(is_pickup_ontime) AS arrange_pickup_order_ontime
    ,COUNT_IF(is_pickup_infull) AS arrange_pickup_order_infull
    ,COUNT_IF(NOT is_pickup_ontime) AS arrange_order_miss_pickup
    ,COUNT_IF(NOT is_pickup_ontime AND is_seller_fault) AS arrange_order_miss_pickup_seller_fault
    ,COUNT_IF(NOT is_pickup_ontime AND NOT is_seller_fault) AS arrange_order_miss_pickup_spx_fault
FROM shipment_agg
GROUP BY 
    arrange_date
    ,final_hub_name
    ,hub_assign_type
    ,order_type
    ,pickup_shop_id
)
,pickup_hub_arrange_agg_2 AS 
(
SELECT 
    arrange_date
    ,pickup_hub_name
    ,hub_assign_type
    ,order_type
    ,COUNT(*) AS total_seller_arrange
    ,SUM(total_order_arrange) AS total_order_arrange
    ,SUM(arrange_pickup_order_ontime) AS arrange_pickup_order_ontime
    ,SUM(arrange_pickup_order_infull) AS arrange_pickup_order_infull
    ,CAST(SUM(arrange_pickup_order_ontime) AS DOUBLE)/SUM(total_order_arrange) AS pct_arrange_pickup_order_ontime
    ,SUM(arrange_order_miss_pickup) AS arrange_order_miss_pickup
    ,CAST(SUM(arrange_order_miss_pickup) AS DOUBLE)/SUM(total_order_arrange) AS pct_arrange_order_miss_pickup
    ,SUM(arrange_order_miss_pickup_seller_fault) AS arrange_order_miss_pickup_seller_fault
    ,CAST(SUM(arrange_order_miss_pickup_seller_fault) AS DOUBLE)/SUM(total_order_arrange) AS pct_arrange_order_miss_pickup_seller_fault
    ,SUM(arrange_order_miss_pickup_spx_fault) AS arrange_order_miss_pickup_spx_fault
    ,CAST(SUM(arrange_order_miss_pickup_spx_fault) AS DOUBLE)/SUM(total_order_arrange) AS pct_arrange_order_miss_pickup_spx_fault
    ,COUNT(CASE WHEN arrange_pickup_order_ontime > 0 THEN pickup_shop_id END) AS arrange_seller_pickup_ontime
    ,CAST(COUNT(CASE WHEN arrange_pickup_order_ontime > 0 THEN pickup_shop_id END) AS DOUBLE)/COUNT(*) AS pct_arrange_seller_pickup_ontime
    ,COUNT(CASE WHEN arrange_pickup_order_infull > 0 THEN pickup_shop_id END) AS arrange_seller_pickup_infull
    ,CAST(COUNT(CASE WHEN arrange_pickup_order_infull > 0 THEN pickup_shop_id END) AS DOUBLE)/COUNT(*) AS pct_arrange_seller_pickup_infull
    ,COUNT(CASE WHEN arrange_pickup_order_ontime = 0 THEN pickup_shop_id END) AS arrange_seller_miss_pickup
    ,CAST(COUNT(CASE WHEN arrange_pickup_order_ontime = 0 THEN pickup_shop_id END) AS DOUBLE)/COUNT(*) AS pct_arrange_seller_miss_pickup
    ,COUNT(CASE WHEN arrange_pickup_order_ontime = 0 AND arrange_order_miss_pickup_seller_fault > 0 THEN pickup_shop_id END) AS arrange_seller_miss_pickup_seller_fault
    ,CAST(COUNT(CASE WHEN arrange_pickup_order_ontime = 0 AND arrange_order_miss_pickup_seller_fault > 0 THEN pickup_shop_id END) AS DOUBLE)/COUNT(*) AS pct_arrange_seller_miss_pickup_seller_fault
    ,COUNT(CASE WHEN arrange_pickup_order_ontime = 0 AND arrange_order_miss_pickup_seller_fault = 0 AND arrange_order_miss_pickup_spx_fault > 0 THEN pickup_shop_id END) AS arrange_seller_miss_pickup_spx_fault
    ,CAST(COUNT(CASE WHEN arrange_pickup_order_ontime = 0 AND arrange_order_miss_pickup_seller_fault = 0 AND arrange_order_miss_pickup_spx_fault > 0 THEN pickup_shop_id END) AS DOUBLE)/COUNT(*) AS pct_arrange_seller_miss_pickup_spx_fault
FROM pickup_hub_arrange_agg_1
WHERE 
    arrange_date BETWEEN CURRENT_DATE - INTERVAL '60' DAY AND CURRENT_DATE - INTERVAL '1' DAY
GROUP BY 
    arrange_date
    ,pickup_hub_name
    ,hub_assign_type
    ,order_type
)
,pickup_hub_inbound_agg_1 AS 
(
SELECT 
    fm_inbound_date
    ,final_hub_name AS pickup_hub_name
    ,hub_assign_type
    ,order_type
    ,pickup_shop_id
    ,COUNT(*) AS total_order_inbound
FROM shipment_agg
GROUP BY 
    fm_inbound_date
    ,final_hub_name
    ,hub_assign_type
    ,order_type
    ,pickup_shop_id 
)
,driver_productivity AS 
(
SELECT 
    fm_inbound_date
    ,final_hub_name AS pickup_hub_name
    ,'HUB' AS hub_assign_type
    ,'MKP' AS order_type
    ,COUNT_IF(final_driver_type = 'FM') AS total_inbound_order_fm_driver
    ,COUNT_IF(final_driver_type = 'LM_4WH') AS total_inbound_order_am_4w_driver
    ,COUNT_IF(final_driver_type = 'LM_2WH') AS total_inbound_order_am_2w_driver
    ,COUNT_IF(final_driver_type = 'SUBCON') AS total_inbound_order_subcon
    ,COUNT_IF(final_driver_type = 'SUBCON_2W') AS total_inbound_order_subcon_2w
    -----
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'FM' THEN pickup_done_driver_id END) AS total_driver_fm_driver
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'LM_4WH' THEN pickup_done_driver_id END) AS total_driver_am_4w_driver
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'LM_2WH' THEN pickup_done_driver_id END) AS total_driver_am_2w_driver
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'SUBCON' THEN pickup_done_driver_id END) AS total_driver_subcon_driver
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'SUBCON_2w' THEN pickup_done_driver_id END) AS total_driver_subcon_2w_driver
    -----
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'FM' THEN pickup_point_id END) AS total_driver_fm_pup
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'LM_4WH' THEN pickup_point_id END) AS total_driver_am_4w_pup
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'LM_2WH' THEN pickup_point_id END) AS total_driver_am_2w_pup
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'SUBCON' THEN pickup_point_id END) AS total_driver_subcon_pup
    ,COUNT(DISTINCT CASE WHEN final_driver_type = 'SUBCON_2W' THEN pickup_point_id END) AS total_driver_subcon_2w_pup
FROM shipment_agg
GROUP BY 
    fm_inbound_date
    ,final_hub_name
    ,3
    ,4
)
,pickup_hub_inbound_agg_2 AS 
(
SELECT 
    fm_inbound_date
    ,pickup_hub_name
    ,hub_assign_type
    ,order_type
    ,COUNT(*) AS total_seller_inbound
    ,SUM(total_order_inbound) AS total_order_inbound
FROM pickup_hub_inbound_agg_1
GROUP BY 
    fm_inbound_date
    ,pickup_hub_name
    ,hub_assign_type
    ,order_type
)
,receive_hub_inbound_agg AS 
(
SELECT 
    fm_inbound_date
    ,final_hub_receive_name AS final_hub_name
    ,hub_assign_type
    ,order_type
    ,COUNT(*) AS total_inbound_order_hub_receive
    ,COUNT_IF(is_fm_hub_outbound_infull) AS total_hub_outbound_infull
    ,CAST(COUNT_IF(is_fm_hub_receive_ontime) AS DOUBLE)/COUNT(*) AS pct_hub_receive_ontime
    ,CAST(COUNT_IF(is_fm_hub_packed_ontime) AS DOUBLE)/COUNT(*) AS pct_hub_packed_ontime
    ,CAST(COUNT_IF(is_fm_hub_outbound_ontime) AS DOUBLE)/COUNT(*) AS pct_hub_outbound_ontime
    ,CAST(COUNT_IF(is_fm_hub_lhing_ontime) AS DOUBLE)/COUNT(*) AS pct_hub_lhing_ontime
    ,CAST(COUNT_IF(is_fm_hub_lhed_ontime) AS DOUBLE)/COUNT(*) AS pct_hub_lhed_ontime
    ,CAST(COUNT_IF(is_soc_receive_ontime) AS DOUBLE)/COUNT(*) AS pct_hub_soc_receive_ontime
    ,COUNT_IF(is_fm_hub_lhing_ontime) AS total_hub_lhing_ontime
    ,COUNT_IF(is_fm_hub_lhing_ontime) AS total_hub_lhed_ontime
FROM shipment_agg
GROUP BY 
    fm_inbound_date
    ,final_hub_receive_name
    ,hub_assign_type
    ,order_type
)
,driver_time AS 
(
SELECT 
    fm_inbound_date
    ,final_hub_name
    ,'HUB' AS hub_assign_type
    ,'MKP' AS order_type
    ,AVG(CASE WHEN minute_per_day != 0 THEN minute_per_day END)*1.000/60 AS avg_pickup_hour_per_day
    ,AVG(CASE WHEN minute_per_day != 0 AND final_driver_type = 'FM' THEN minute_per_day END)*1.000/60 AS avg_fm_pickup_hour_per_day
    ,AVG(CASE WHEN minute_per_day != 0 AND final_driver_type = 'LM_4WH' THEN minute_per_day END)*1.000/60 AS avg_am_4w_pickup_hour_per_day
    ,AVG(CASE WHEN minute_per_day != 0 AND final_driver_type = 'LM_2WH' THEN minute_per_day END)*1.000/60 AS avg_am_2w_pickup_hour_per_day
    ,AVG(CASE WHEN minute_per_day != 0 AND final_driver_type = 'SUBCON' THEN minute_per_day END)*1.000/60 AS avg_subcon_pickup_hour_per_day
    ,AVG(CASE WHEN minute_per_day != 0 AND final_driver_type = 'SUBCON_2W' THEN minute_per_day END)*1.000/60 AS avg_subcon_2w_pickup_hour_per_day
FROM 
    (
    SELECT 
        fm_inbound_date
        ,final_hub_name AS final_hub_name
        ,pickup_done_driver_id
        ,final_driver_type
        -- ,hub_assign_type
        -- ,order_type
        ,MIN(pickup_done_timestamp) AS first_pickup_time
        ,MAX(pickup_done_timestamp) AS last_pickup_time
        ,MIN(CASE WHEN final_driver_type = 'FM' THEN pickup_done_timestamp END) AS first_fm_pickup_time
        ,MIN(CASE WHEN final_driver_type = 'LM_4WH' THEN pickup_done_timestamp END) AS first_am_4w_pickup_time
        ,MIN(CASE WHEN final_driver_type = 'LM_2WH' THEN pickup_done_timestamp END) AS first_am_2w_pickup_time
        ,MIN(CASE WHEN final_driver_type = 'SUBCON' THEN pickup_done_timestamp END) AS first_subcon_pickup_time
        -- ,MAX(CASE WHEN final_driver_type = 'FM' THEN pickup_done_timestamp END) AS last_fm_pickup_time
        -- MAX(CASE WHEN final_driver_type = 'LM_4WH' THEN pickup_done_timestamp END) AS last_am_4w_pickup_time,
        -- MAX(CASE WHEN final_driver_type = 'LM_2WH' THEN pickup_done_timestamp END) AS last_am_2w_pickup_time,
        -- MAX(CASE WHEN final_driver_type = 'SUBCON' THEN pickup_done_timestamp END) AS last_subcon_pickup_time,
        -- ,DATE_DIFF('MINUTE',MIN(pickup_done_timestamp),MAX(pickup_done_timestamp)) AS minute_per_day
        -- ,DATE_DIFF('MINUTE',MIN(CASE WHEN final_driver_type = 'FM' THEN pickup_done_timestamp END),MAX(CASE WHEN final_driver_type = 'FM' THEN pickup_done_timestamp END)) AS minute_fm_per_day
        -- ,DATE_DIFF('MINUTE',MIN(CASE WHEN final_driver_type = 'LM_4WH' THEN pickup_done_timestamp END),MAX(CASE WHEN final_driver_type = 'LM_4WH' THEN pickup_done_timestamp END)) AS minute_am_4w_per_day
        -- ,DATE_DIFF('MINUTE',MIN(CASE WHEN final_driver_type = 'LM_2WH' THEN pickup_done_timestamp END),MAX(CASE WHEN final_driver_type = 'LM_2WH' THEN pickup_done_timestamp END)) AS minute_am_2w_per_day
        -- ,DATE_DIFF('MINUTE',MIN(CASE WHEN final_driver_type = 'SUBCON' THEN pickup_done_timestamp END),MAX(CASE WHEN final_driver_type = 'SUBCON' THEN pickup_done_timestamp END)) AS minute_subcon_per_day
    FROM shipment_agg
    GROUP BY 
        fm_inbound_date
        ,final_hub_name
        ,pickup_done_driver_id
        ,final_driver_type
    )
GROUP BY 
    fm_inbound_date
    ,final_hub_name
    ,3
    ,4
)
SELECT *
FROM pickup_hub_arrange_agg_2
LEFT JOIN pickup_hub_inbound_agg_2
ON pickup_hub_arrange_agg_2.arrange_date = pickup_hub_inbound_agg_2.fm_inbound_date
AND pickup_hub_arrange_agg_2.pickup_hub_name = pickup_hub_inbound_agg_2.pickup_hub_name
AND pickup_hub_arrange_agg_2.hub_assign_type = pickup_hub_inbound_agg_2.hub_assign_type
AND pickup_hub_arrange_agg_2.order_type = pickup_hub_inbound_agg_2.order_type
LEFT JOIN receive_hub_inbound_agg
ON pickup_hub_arrange_agg_2.arrange_date = receive_hub_inbound_agg.fm_inbound_date
AND pickup_hub_arrange_agg_2.pickup_hub_name = receive_hub_inbound_agg.final_hub_name
AND pickup_hub_arrange_agg_2.hub_assign_type = receive_hub_inbound_agg.hub_assign_type
AND pickup_hub_arrange_agg_2.order_type = receive_hub_inbound_agg.order_type
LEFT JOIN driver_time
ON pickup_hub_arrange_agg_2.arrange_date = driver_time.fm_inbound_date
AND pickup_hub_arrange_agg_2.pickup_hub_name = driver_time.final_hub_name
AND pickup_hub_arrange_agg_2.hub_assign_type = driver_time.hub_assign_type
AND pickup_hub_arrange_agg_2.order_type = driver_time.order_type
LEFT JOIN driver_productivity
ON pickup_hub_arrange_agg_2.arrange_date = driver_productivity.fm_inbound_date
AND pickup_hub_arrange_agg_2.pickup_hub_name = driver_productivity.pickup_hub_name
AND pickup_hub_arrange_agg_2.hub_assign_type = driver_productivity.hub_assign_type
AND pickup_hub_arrange_agg_2.order_type = driver_productivity.order_type