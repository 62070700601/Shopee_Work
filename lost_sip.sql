WITH station AS 
(
    SELECT
        CAST(id AS INTEGER) AS id
        ,station_name
        ,CASE 
            WHEN id IN (3,242,1784) THEN 'MM'
            WHEN id IN (71,77,82,983,1350,1479,1480) THEN 'MM'
            WHEN id IN (521,522,655,656,712,730,951,952,1419) THEN 'CTO' --4XXXX
            WHEN station_type = 7 THEN 'CTO'
            WHEN station_type = 5 THEN '4PL'
            WHEN station_type = 3 AND station_name LIKE 'H%' THEN 'CTO'
        END AS station_type
        ,CASE 
            WHEN id IN (3,242,1784) THEN 'SOC'
            WHEN id IN (71,77,82,983,1350,1479,1480) THEN 'RC'
            WHEN id IN (521,522,655,656,712,730,951,952,1419) THEN 'FM'
            WHEN station_type = 7 THEN 'FM'
            WHEN station_type = 5 THEN '4PL'
            WHEN station_type = 3 AND station_name LIKE 'H%' THEN 'LM'
        END AS station_sub_type
        ,CASE 
            WHEN LOWER(station_name) LIKE 'par%' OR LOWER(station_name) LIKE 'upc par%' OR (station_name LIKE 'SDOP%') THEN 'SDOP' 
            WHEN station_name LIKE 'Shopee %' OR (station_name LIKE 'P%' AND LOWER(station_name) NOT LIKE 'par%') THEN 'PS' 
            ELSE 'SDOP' 
        END AS sp_type
        -- CASE 
        --     WHEN LOWER(station_name) LIKE 'par%' OR LOWER(station_name) LIKE 'upc par%' OR (station_name LIKE 'SDOP%') THEN 'SDOP' 
        --     WHEN station_name LIKE 'Shopee %' OR (station_name LIKE 'P%' AND LOWER(station_name) NOT LIKE 'par%') THEN 'PS' 
        --     WHEN LOWER(station_name) LIKE 'ship%' OR station_name LIKE 'DSM -%' THEN 'AGGREGATOR' 
        --     ELSE 'HUB' 
        -- END AS sp_sub_type
    FROM spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live
)
,status_index AS 
(
    SELECT 
        status_id
        ,status_name
        ,IF(status_flow = 'forward',1,0) AS status_flow
        ,stuck_main_pic AS lost_pic
        ,stuck_sub_pic1 AS lost_pic_sub1
        ,stuck_sub_pic2 AS lost_pic_sub2
        ,stuck_sub_pic3 AS lost_pic_sub3
        ,NULL AS lost_pic_remark
    FROM dev_thopsbi_lof.spx_analytics_status_map_v2
)
,TO_info AS 
(
	SELECT 
		to_number
		,operator --SOC_Packing operator
		,receiver --not found
		,pickup_station_name AS to_origin
		,dest_station_name AS to_destination
		,station_type AS lh_dest_station_type
		,station_sub_type AS lh_dest_station_sub_type
	FROM spx_mart.shopee_fms_th_db__transport_order_tab__th_continuous_s0_live AS transport_order
	LEFT JOIN station
	ON transport_order.dest_station_name = station.station_name
)
,next_station_cal AS 
(
    SELECT 
        next_station.shipment_id
        ,next_station.next_station_id
        ,station.station_name AS next_station_name
        ,station.station_type AS next_station_type
        ,station.station_sub_type AS next_station_sub_type
    FROM 
    ( 
        SELECT 
            shipment_id
            ,MIN_BY(next_station_id,latest_rn) AS next_station_id
        FROM thopsbi_lof.thspx_fact_order_tracking_di_th
        WHERE 
            next_station_id IS NOT NULL
        GROUP BY 
            shipment_id
    ) next_station
    LEFT JOIN station
    ON CAST(station.id AS VARCHAR) = next_station.next_station_id
)
, latest_status AS 
(
    SELECT 
        latest_track.shipment_id
        ,latest_track.status_id
        ,latest_track.status_date
        ,latest_track.status_station_id
        ,latest_track.status_to_number
        ,status_index.status_name
        ,status_index.status_flow
        ,status_index.lost_pic
        ,status_index.lost_pic_sub1
        ,status_index.lost_pic_sub2
        ,status_index.lost_pic_sub3
        ,status_index.lost_pic_remark
        ,station.station_name
        ,station.station_type
        ,station.station_sub_type
        ,station.sp_type
        ,TO_info.lh_dest_station_sub_type
        ,next_station_cal.next_station_id
        ,next_station_cal.next_station_name
        ,next_station_cal.next_station_type
        ,next_station_cal.next_station_sub_type
    FROM
    (
        SELECT 
            shipment_id
            ,status AS status_id
            ,DATE(FROM_UNIXTIME(ctime-3600)) AS status_date
            ,status_station_id
            ,status_operator
            ,status_to_number
            ,status_linehaul_task_id
            ,ROW_NUMBER() OVER(PARTITION BY shipment_id ORDER BY ctime DESC) AS rank_num
        FROM 
        (
            SELECT 
                shipment_id
                ,status
                ,ctime
                ,IF(station_id > 0, station_id, TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.station_id') AS INTEGER))) AS status_station_id
                ,operator AS status_operator
                ,TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.to_number') AS VARCHAR)) AS status_to_number
                ,TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id') AS VARCHAR)) AS status_linehaul_task_id
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
            WHERE 
                (shipment_id LIKE 'SPXTH%' OR shipment_id LIKE 'TH%')
                AND status NOT IN (11,12)
        /*
            UNION 
            SELECT 
                shipment_id,
                status,
                ctime,
                IF(station_id > 0, station_id, TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.station_id') AS INTEGER))) AS status_station_id,
                operator AS status_operator,
                TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.to_number') AS VARCHAR)) AS status_to_number,
                TRY(CAST(JSON_EXTRACT(JSON_PARSE(content), '$.linehaul_task_id') AS VARCHAR)) AS status_linehaul_task_id

            FROM spx_mart.shopee_ssc_spx_track_archive_th_db__order_tracking_tab__reg_daily_s0_live 
            WHERE (shipment_id LIKE 'SPXTH%' OR shipment_id LIKE 'TH%')
            AND status NOT IN (11,12)
        */
        )
    ) latest_track
    LEFT JOIN status_index
    ON CAST(latest_track.status_id AS VARCHAR) = status_index.status_id
    LEFT JOIN station
    ON latest_track.status_station_id = station.id
    LEFT JOIN TO_info 
    ON latest_track.status_to_number = TO_info.to_number
    LEFT JOIN next_station_cal 
    ON next_station_cal.shipment_id = latest_track.shipment_id
    WHERE 
        rank_num = 1
)
, lost_damage_track AS 
(
    SELECT 
        shipment_id
        ,status_id
        ,CASE 
            WHEN status_id = 11 THEN 'Lost'
            WHEN status_id = 12 THEN 'Damaged'
        END AS lost_damage_type
        ,lost_date       
    FROM
    (
        SELECT 
            shipment_id
            ,status AS status_id
            ,DATE(FROM_UNIXTIME(ctime-3600)) AS lost_date
            ,ROW_NUMBER() OVER(PARTITION BY shipment_id ORDER BY ctime DESC) AS rank_num
        FROM 
        (
            SELECT 
                shipment_id
                ,status
                ,ctime
            FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
            WHERE 
                (shipment_id LIKE 'SPXTH%' OR shipment_id LIKE 'TH%')
                AND status IN (11,12)
            UNION 
            SELECT 
                shipment_id
                ,status
                ,ctime
            FROM spx_mart.shopee_ssc_spx_track_archive_th_db__order_tracking_tab__reg_daily_s0_live 
            WHERE 
                (shipment_id LIKE 'SPXTH%' OR shipment_id LIKE 'TH%')
                AND status IN (11,12)
        )
    )
    WHERE rank_num = 1
)
,raw_data AS 
(
    SELECT 
        fleet_order.shipment_id
        ,fleet_order.cogs
        ,CASE WHEN fleet_order.order_type IN (7,8) THEN 'BKY' ELSE 'NON_BKY' END AS BKY_type
        ,lost_damage_track.lost_date
        ,lost_damage_track.lost_damage_type
        ,latest_status.status_id
        ,latest_status.status_date
        ,latest_status.status_name
        ,latest_status.status_flow
        ,CASE 
            WHEN latest_status.lost_pic = 'sp_type' THEN sp_type --if start with 'Partner' = SDOP, if start with 'Spx shop' = Parcel Shop
            WHEN latest_status.lost_pic = 'is_dop_packing' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO', 'CTO/MM') -- if have 'dop_packing' = SP/CTO, if not have = CTO/MM || pending dop_packing status to live
            WHEN latest_status.lost_pic = 'is_dop_packing2' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO/MM', 'SP/CTO') --if have 'dop_packing' = SP/CTO/MM, if not have = SP/CTO || pending dop_packing status to live
            WHEN latest_status.lost_pic = 'next_station_sub_type' THEN IF(next_station_type IS NOT NULL, next_station_sub_type, 'SOC') --if have order path = use order path, if not base on 'FM path', if default = SOC
            WHEN latest_status.lost_pic = 'current_station_type' THEN station_type --depends on current station, if current station is '4XXXX' = CTO, if not = 'MM'
            WHEN latest_status.lost_pic = 'current_station_sub_type' THEN station_sub_type --1st Sub : if station is 'SOCE', 'SOCW', 'Sorting center' = SOC if station is 'XXRC-A', 'XXRC-B', 'XXRC' = RC
            WHEN latest_status.lost_pic = 'path_attachment' THEN COALESCE(next_station_sub_type, lh_dest_station_sub_type) --depends on order path, if not base on 'Path attachment' || edge case pai gornn
            WHEN latest_status.lost_pic = 'ting_ted' THEN IF(COALESCE(next_station_sub_type, lh_dest_station_sub_type) = 'LM', 'CTO/MM', 'MM') --if ting station is 'SOC' and ted station is 'RC' = 'MM',if ting station is 'SOC' or 'RC' and ted station is 'LM' = 'CTO/MM'
            ELSE latest_status.lost_pic
        END AS lost_pic
        ,sp_type
        ,CASE 
            WHEN latest_status.lost_pic_sub1 = 'sp_type' THEN sp_type --if start with 'Partner' = SDOP, if start with 'Spx shop' = Parcel Shop
            WHEN latest_status.lost_pic_sub1 = 'is_dop_packing' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO', 'CTO/MM') -- if have 'dop_packing' = SP/CTO, if not have = CTO/MM || pending dop_packing status to live
            WHEN latest_status.lost_pic_sub1 = 'is_dop_packing2' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO/MM', 'SP/CTO') --if have 'dop_packing' = SP/CTO/MM, if not have = SP/CTO || pending dop_packing status to live
            WHEN latest_status.lost_pic_sub1 = 'next_station_sub_type' THEN IF(next_station_type IS NOT NULL, next_station_sub_type, 'SOC') --if have order path = use order path, if not base on 'FM path', if default = SOC
            WHEN latest_status.lost_pic_sub1 = 'current_station_type' THEN station_type --depends on current station, if current station is '4XXXX' = CTO, if not = 'MM'
            WHEN latest_status.lost_pic_sub1 = 'current_station_sub_type' THEN station_sub_type --1st Sub : if station is 'SOCE', 'SOCW', 'Sorting center' = SOC if station is 'XXRC-A', 'XXRC-B', 'XXRC' = RC
            WHEN latest_status.lost_pic_sub1 = 'path_attachment' THEN COALESCE(next_station_sub_type, lh_dest_station_sub_type) --depends on order path, if not base on 'Path attachment' || edge case pai gornn
            WHEN latest_status.lost_pic_sub1 = 'ting_ted' THEN IF(COALESCE(next_station_sub_type, lh_dest_station_sub_type) = 'LM', 'CTO/MM', 'MM') --if ting station is 'SOC' and ted station is 'RC' = 'MM',if ting station is 'SOC' or 'RC' and ted station is 'LM' = 'CTO/MM'
            ELSE latest_status.lost_pic_sub1
        END AS lost_pic_sub1
        ,CASE 
            WHEN latest_status.lost_pic_sub2 = 'sp_type' THEN sp_type --if start with 'Partner' = SDOP, if start with 'Spx shop' = Parcel Shop
            WHEN latest_status.lost_pic_sub2 = 'is_dop_packing' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO', 'CTO/MM') -- if have 'dop_packing' = SP/CTO, if not have = CTO/MM || pending dop_packing status to live
            WHEN latest_status.lost_pic_sub2 = 'is_dop_packing2' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO/MM', 'SP/CTO') --if have 'dop_packing' = SP/CTO/MM, if not have = SP/CTO || pending dop_packing status to live
            WHEN latest_status.lost_pic_sub2 = 'next_station_sub_type' THEN IF(next_station_type IS NOT NULL, next_station_sub_type, 'SOC') --if have order path = use order path, if not base on 'FM path', if default = SOC
            WHEN latest_status.lost_pic_sub2 = 'current_station_type' THEN station_type --depends on current station, if current station is '4XXXX' = CTO, if not = 'MM'
            WHEN latest_status.lost_pic_sub2 = 'current_station_sub_type' THEN station_sub_type --1st Sub : if station is 'SOCE', 'SOCW', 'Sorting center' = SOC if station is 'XXRC-A', 'XXRC-B', 'XXRC' = RC
            WHEN latest_status.lost_pic_sub2 = 'path_attachment' THEN COALESCE(next_station_sub_type, lh_dest_station_sub_type) --depends on order path, if not base on 'Path attachment' || edge case pai gornn
            WHEN latest_status.lost_pic_sub2 = 'ting_ted' THEN IF(COALESCE(next_station_sub_type, lh_dest_station_sub_type) = 'LM', 'CTO/MM', 'MM') --if ting station is 'SOC' and ted station is 'RC' = 'MM',if ting station is 'SOC' or 'RC' and ted station is 'LM' = 'CTO/MM'
            ELSE latest_status.lost_pic_sub2
        END AS lost_pic_sub2
        ,CASE 
            WHEN latest_status.lost_pic_sub3 = 'sp_type' THEN sp_type --if start with 'Partner' = SDOP, if start with 'Spx shop' = Parcel Shop
            WHEN latest_status.lost_pic_sub3 = 'is_dop_packing' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO', 'CTO/MM') -- if have 'dop_packing' = SP/CTO, if not have = CTO/MM || pending dop_packing status to live
            WHEN latest_status.lost_pic_sub3 = 'is_dop_packing2' THEN IF(FALSE /*to input dop_packing timestamp*/, 'SP/CTO/MM', 'SP/CTO') --if have 'dop_packing' = SP/CTO/MM, if not have = SP/CTO || pending dop_packing status to live
            WHEN latest_status.lost_pic_sub3 = 'next_station_sub_type' THEN IF(next_station_type IS NOT NULL, next_station_sub_type, 'SOC') --if have order path = use order path, if not base on 'FM path', if default = SOC
            WHEN latest_status.lost_pic_sub3 = 'current_station_type' THEN station_type --depends on current station, if current station is '4XXXX' = CTO, if not = 'MM'
            WHEN latest_status.lost_pic_sub3 = 'current_station_sub_type' THEN station_sub_type --1st Sub : if station is 'SOCE', 'SOCW', 'Sorting center' = SOC if station is 'XXRC-A', 'XXRC-B', 'XXRC' = RC
            WHEN latest_status.lost_pic_sub3 = 'path_attachment' THEN COALESCE(next_station_sub_type, lh_dest_station_sub_type) --depends on order path, if not base on 'Path attachment' || edge case pai gornn
            WHEN latest_status.lost_pic_sub3 = 'ting_ted' THEN IF(COALESCE(next_station_sub_type, lh_dest_station_sub_type) = 'LM', 'CTO/MM', 'MM') --if ting station is 'SOC' and ted station is 'RC' = 'MM',if ting station is 'SOC' or 'RC' and ted station is 'LM' = 'CTO/MM'
            ELSE latest_status.lost_pic_sub3
        END AS lost_pic_sub3
        ,latest_status.lost_pic_remark
    /*
        CASE
            WHEN latest_status.lost_pic_sub1 = 'SDOP/PS' THEN dropoff_type
            WHEN latest_status.lost_pic_sub1 = 'status_station' THEN station_Sub_PIC
            WHEN latest_status.lost_pic_sub1 = 'LH_destination' THEN LH_dest_sub_PIC
            WHEN latest_status.lost_pic_sub1 NOT IN ('status_station','LH_destination') THEN lost_pic_sub1
        END AS lost_pic_sub1,
        CASE
            WHEN latest_status.lost_pic_sub2 = 'SDOP/PS' THEN dropoff_type
            WHEN latest_status.lost_pic_sub2 = 'status_station' THEN station_Sub_PIC
            WHEN latest_status.lost_pic_sub2 = 'LH_destination' THEN LH_dest_sub_PIC
            WHEN latest_status.lost_pic_sub2 NOT IN ('status_station','LH_destination') THEN lost_pic_sub2
        END AS lost_pic_sub2,
        CASE
            WHEN latest_status.lost_pic_sub3 = 'SOC/RC' AND latest_status.status_id IN (43,44,45,46,47,48) THEN LH_dest_sub_PIC
            WHEN latest_status.lost_pic_sub3 = 'SOC/RC' AND latest_status.status_id NOT IN (43,44,45,46,47,48) THEN 'SOC'
        END AS lost_pic_sub3,
        latest_status.lost_pic_remark
    */
    FROM spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_daily_s0_live fleet_order
    INNER JOIN lost_damage_track
    ON fleet_order.shipment_id = lost_damage_track.shipment_id
    LEFT JOIN latest_status
    ON fleet_order.shipment_id = latest_status.shipment_id

)

SELECT 
    YEAR(lost_date) AS "Report Year",
    MONTH(lost_date) AS "Report Month",
    DATE_TRUNC('week',lost_date) AS "Report Week",
    --DATE_FORMAT(lost_date, '%b') AS "Month Name",
    lost_damage_type AS "Lost/Damaged Type",
    status_flow as "Is forward flow",
    --status_id AS "Status ID",
    status_name AS "Status Name",
    lost_pic AS "PIC",
    lost_pic_sub1 AS "1st Sub",
    lost_pic_sub2 AS "2nd Sub",
    lost_pic_sub3 AS "3rd Sub",
    lost_pic_remark AS "Remark",
    COUNT(*) AS "# Orders",
    SUM(cogs) AS "COGS",
    SUM(CASE WHEN BKY_type = 'BKY' THEN IF(cogs<=15000.00,COGS, 15000.00) ELSE IF(cogs<=2000.00,COGS, 2000.00) END) AS "COGS (cap)"
FROM raw_data
WHERE lost_date BETWEEN DATE('2022-01-01') AND CURRENT_DATE
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY 1 DESC,2 DESC,3,4,5 DESC,6,7,8,9,10,11