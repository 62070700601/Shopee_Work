with cte as 
(
select 
    order_track.shipment_id
    ,date(from_unixtime(order_track.ctime-3600)) as date_time
    ,order_track.status
    ,staion_table_name.station_name
    ,row_number() over (partition by order_track.shipment_id order by from_unixtime(order_track.ctime-3600) asc) as rank_num
from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
left join spx_mart.dim_spx_station_tab_ri_th_ro as staion_table_name
on order_track.station_id = staion_table_name.station_id
where 
    order_track.status = 1
    and date(from_unixtime(order_track.ctime-3600)) = date('2023-01-07')
    and staion_table_name.station_name = 'HDSKT - ดอยสะเก็ด'
)
select 
    cte.shipment_id
    ,cte.date_time
    ,soc_ting.date_time as soc_ting_timestamp
    ,soc_ted.date_time as soc_ted_timestamp
    -- ,is_buyer_office_location
    ,cte.station_name
    -- ,thopsbi_spx.dwd_pub_shipment_info_df_th.is_buyer_office_location
from cte
left join
(
    select 
        shipment_id
        ,max(from_unixtime(ctime-3600)) as date_time
        -- ,row_number() over (partition by shipment_id order by from_unixtime(ctime-3600) desc)
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
    where 
        status = 15
        and date(from_unixtime(order_track.ctime-3600)) = date('2023-01-07')
    group by 
        shipment_id
) as soc_ting
on cte.shipment_id = soc_ting.shipment_id
left join 
(
    select 
        order_track.shipment_id
        ,max(from_unixtime(order_track.ctime-3600)) as date_time
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_track
    where 
        status = 36
        and date(from_unixtime(order_track.ctime-3600)) = date('2023-01-07')
    group by 
        order_track.shipment_id
) as soc_ted
on cte.shipment_id = soc_ted.shipment_id
left join thopsbi_spx.spx_analytics_sla_precal_date as date_dict
on cte.date_time = cast(date_dict.report_date as timestamp )
left join thopsbi_spx.dwd_pub_shipment_info_df_th
on cte.shipment_id = thopsbi_spx.dwd_pub_shipment_info_df_th.shipment_id
where 
    rank_num = 1
    and thopsbi_spx.dwd_pub_shipment_info_df_th.is_buyer_office_location != True 
    -- and date_dict.is_holiday_office != '1'