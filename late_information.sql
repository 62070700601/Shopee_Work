WITH gen_shipment AS (
    SELECT
        shipment_id,
        is_cross_border,
        is_warehouse,
        is_sip,
        is_bulky
FROM thopsbi_spx.dwd_gen_shipment_info_df_th
),

raw AS(
SELECT 
        fact_late_order.shipment_id,
CASE
WHEN fact_gen_shipment.is_cross_border THEN 'CB'
WHEN fact_gen_shipment.is_warehouse THEN 'WH'
WHEN fact_gen_shipment.is_sip THEN 'SIP'
WHEN fact_late_order.is_open_service THEN 'OSV'
WHEN fact_gen_shipment.is_bulky THEN 'BKY'
ELSE 'MKP' 
END AS order_type,
        seller_area_name,
        seller_region_name,
        buyer_area_name,
        buyer_region_name,
        spx_delivery_sla_date,
        dropoff_station_name,
        fm_hub_station_name,
        lm_hub_station_name,
        fm_hub_lh_transporting_timestamp,
        fm_hub_lh_transporting_sla_timestamp,
        on_time_fm_hub_lh_transporting_sla_flag,
        fm_hub_lh_transported_timestamp,
        fm_hub_lh_transported_sla_timestamp,
        on_time_fm_hub_lh_transported_sla_flag,
        pickup_soc_received_timestamp,
        pickup_soc_received_sla_timestamp,
        on_time_pickup_soc_received_sla_flag,
        delivery_soc_lh_transporting_timestamp,
        delivery_soc_lh_transporting_sla_timestamp,
        on_time_delivery_soc_lh_transporting_sla_flag,
        late_type
FROM dev_thopsbi_spx.spx_late_order_info_table_daily AS fact_late_order
LEFT JOIN gen_shipment AS fact_gen_shipment
ON fact_late_order.shipment_id = fact_gen_shipment.shipment_id
WHERE spx_delivery_sla_date BETWEEN date(DATE_TRUNC('day', current_timestamp) - interval '30' day) and date(DATE_TRUNC('day', current_timestamp) + interval '23' hour + interval '59' minute) 
AND NOT is_4pl
)
SELECT
    spx_delivery_sla_date,
    raw.shipment_id
    ,late_type
    
FROM raw
-- WHERE late_type IN ('soc_received_late', 'soc_outbound_late', 'miss_sort', 'geo_fencing_issue', 'delivered_on_time')
where
    late_type = 'geo_fencing_issue'
    and raw.order_type = 'OSV'
    and date(spx_delivery_sla_date) between date(DATE_TRUNC('day', current_timestamp) - interval '30' day) and date(DATE_TRUNC('day', current_timestamp) + interval '23' hour + interval '59' minute) 
order by 
    spx_delivery_sla_date asc