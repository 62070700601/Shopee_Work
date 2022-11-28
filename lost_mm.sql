with fmhub_pickup_done as 
(
    select 
        shipment_id
        ,status_time
        -- ,station_name
    from 
    (
        select 
            shipment_id
            ,station_id
            ,from_unixtime(ctime-3600) as status_time
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 39
            and date(from_unixtime(ctime-3600)) between date('2022-07-01 00:00:00.000') and date('2022-09-20 23:59:59.000')
    )
    where 
        rank_num = 1
    
    group by 
        shipment_id
        ,status_time
)
,soc_pickup_done as 
(
    select 
        shipment_id
        ,status_time
    from 
    (
        select 
            shipment_id
            ,station_id
            ,from_unixtime(ctime-3600) as status_time
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) asc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 13
            and date(from_unixtime(ctime-3600)) between date('2022-07-01 00:00:00.000') and date('2022-09-20 23:59:59.000')
    )
    where 
      rank_num = 1
    
    group by 
        shipment_id
        ,status_time
)
,lost as 
(
    select 
        shipment_id
        ,first_soc_received_timestamp as first_soc_received
        ,lost_timestamp
    from thopsbi_spx.dwd_pub_shipment_info_df_th
    where 
        date(lost_timestamp) between date('2022-08-01 00:00:00.000') and date('2022-09-20 23:59:59.000')
        and latest_status_name = 'Lost'
    ORDER BY
        lost_timestamp asc
)
select 
    lost.shipment_id
    ,fmhub_pickup_done.status_time as fmhub_pickup_don_timestammp
    ,soc_pickup_done.status_time as soc_pickup_done_timestamp
    ,lost.first_soc_received
from lost
left join fmhub_pickup_done
on lost.shipment_id = fmhub_pickup_done.shipment_id
left join soc_pickup_done
on lost.shipment_id = soc_pickup_done.shipment_id
where 
    date(lost.lost_timestamp) between date('2022-08-01 00:00:00.000') and date('2022-09-20 23:59:59.000')
order by lost_timestamp asc