-- นับ Return soc receivied d-1 สเตชั่น soce
with Return_SOC_Received as 
(
    select 
        shipment_id
        ,Return_SOC_Received_Time
        ,Return_SOC_received_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_Received_Time
            ,operator as Return_SOC_received_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 58
            and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',current_date ) - interval '1' day) and date(DATE_TRUNC('day',current_timestamp) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            and operator = 'gee.boonprat@shopee.com'
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
        shipment_id
        ,Return_SOC_Received_Time
        ,Return_SOC_received_operator
)
,Pub_shipment_order_type as
(
    select
        shipment_id
        ,case
            when order_type_name = 'NON_SHOPEE_MARKETPLACE_STANDARD' then 'OSV'
            when order_type_name = 'Shopee Xpress' then 'WH'
            when order_type_name = 'CB' then 'CB'
            else 'MKP'
        end as order_type_name
        ,order_type_id as order_type
        ,seller_region_name as return_to_seller_region
    from thopsbi_spx.dwd_pub_shipment_info_df_th 
    group by 
        shipment_id
        ,case
            when order_type_name = 'NON_SHOPEE_MARKETPLACE_STANDARD' then 'OSV'
            when order_type_name = 'Shopee Xpress' then 'WH'
            when order_type_name = 'CB' then 'CB'
            else 'MKP'
        end
        ,order_type_id
        ,seller_region_name
)
,Return_SOC_packed as 
(
     select 
        shipment_id
        ,Return_SOC_packed_Time
        ,Return_SOC_packed_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_packed_Time
            ,operator as Return_SOC_packed_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 60
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
        shipment_id
        ,Return_SOC_packed_Time
        ,Return_SOC_packed_operator
)
,Return_SOC_LHPacked as 
(
    select 
        shipment_id
        ,Return_SOC_LHPacked_Time
        ,Return_SOC_LHPacked_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_LHPacked_Time
            ,operator as Return_SOC_LHPacked_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 62
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            -- Return_SOC_LHPacked = 62
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
          shipment_id
        ,Return_SOC_LHPacked_Time
        ,Return_SOC_LHPacked_operator
)
,Return_SOC_LHTransporting as 
(
      select 
        shipment_id
        ,Return_SOC_LHTransporting_Time
        ,Return_SOC_LHTransporting_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_LHTransporting_Time
            ,operator as Return_SOC_LHTransporting_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 64
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            -- Return_SOC_LHTransporting = 64
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
     group by 
        shipment_id
        ,Return_SOC_LHTransporting_Time
        ,Return_SOC_LHTransporting_operator
)
,Return_SOC_LHTransported as
(
    select 
        shipment_id
        ,Return_SOC_LHTransported_Time
        ,Return_SOC_LHTransported_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_LHTransported_Time
            ,operator as Return_SOC_LHTransported_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 65
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            -- Return_SOC_LHTransported = 65
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
        shipment_id
        ,Return_SOC_LHTransported_Time
        ,Return_SOC_LHTransported_operator
)
,Return_SOC_returning as 
(
     select 
        shipment_id
        ,Return_SOC_returning_Time
        ,Return_SOC_returning_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_returning_Time
            ,operator as Return_SOC_returning_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 63
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            -- Return_SOC_returning = 63
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
        shipment_id
        ,Return_SOC_returning_Time
        ,Return_SOC_returning_operator
)
,Return_SOC_returned as 
( 
     select 
        shipment_id
        ,Return_SOC_returned_Time
        ,Return_SOC_returned_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_returned_Time
            ,operator as Return_SOC_returned_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 6
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            -- Return_SOC_returned = 6
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
        shipment_id
        ,Return_SOC_returned_Time
        ,Return_SOC_returned_operator
)
,Return_SOC_Assigning as 
(
    select 
        shipment_id
        ,Return_SOC_Assigning_Time
        ,Return_SOC_Assigning_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_SOC_Assigning_Time
            ,operator as Return_SOC_Assigning_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 117
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            -- Return_SOC_returned = 6
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
        shipment_id
        ,Return_SOC_Assigning_Time
        ,Return_SOC_Assigning_operator
)
,Return_Failed as 
(
     select 
        shipment_id
        ,Return_Failed_Time
        ,Return_Failed_operator
    from 
    (
        select 
            shipment_id
            ,FROM_UNIXTIME(ctime-3600) as Return_Failed_Time
            ,operator as Return_Failed_operator
            ,row_number() over (partition by shipment_id order by FROM_UNIXTIME(ctime-3600) desc) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 14
            -- and date(FROM_UNIXTIME(ctime-3600)) between date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) - interval '1' day) and date(DATE_TRUNC('day',FROM_UNIXTIME(ctime-3600)) + interval '23' hour + interval '59' minute + interval '59' second)
            and station_id = 3
            -- Return_SOC_returned = 6
        order by 
            FROM_UNIXTIME(ctime-3600) desc
    )
    where 
        row_number = 1 
    group by 
        shipment_id
        ,Return_Failed_Time
        ,Return_Failed_operator
)
select 
    Return_SOC_Received.shipment_id 
    ,Pub_shipment_order_type.order_type
    ,Pub_shipment_order_type.order_type_name
    ,Pub_shipment_order_type.return_to_seller_region
    ,Return_SOC_Received_Time
    ,Return_SOC_packed_Time
    ,Return_SOC_LHPacked_Time
    ,Return_SOC_LHTransporting_Time
    ,Return_SOC_LHTransported_Time
    ,Return_SOC_returning_Time
    ,Return_SOC_returned_Time
    ,Return_SOC_Assigning_Time
    ,Return_Failed_Time
    ,Return_SOC_received_operator
    ,Return_SOC_LHPacked_operator
    ,Return_SOC_LHTransporting_operator
from Return_SOC_Received
left join Pub_shipment_order_type
on Return_SOC_Received.shipment_id = Pub_shipment_order_type.shipment_id
left join Return_SOC_packed
on Return_SOC_Received.shipment_id = Return_SOC_packed.shipment_id
left join Return_SOC_LHPacked
on Return_SOC_Received.shipment_id = Return_SOC_LHPacked.shipment_id
left join Return_SOC_LHTransporting
on Return_SOC_Received.shipment_id = Return_SOC_LHTransporting.shipment_id
left join Return_SOC_LHTransported
on Return_SOC_Received.shipment_id = Return_SOC_LHTransported.shipment_id
left join Return_SOC_returning
on Return_SOC_Received.shipment_id = Return_SOC_returning.shipment_id
left join Return_SOC_returned
on Return_SOC_Received.shipment_id = Return_SOC_returned.shipment_id
left join Return_SOC_Assigning 
on Return_SOC_Received.shipment_id = Return_SOC_Assigning.shipment_id
left join Return_Failed
on Return_SOC_Received.shipment_id = Return_Failed.shipment_id
where   
    date(Return_SOC_Received_Time) between date(DATE_TRUNC('day',current_date ) - interval '1' day) and date(DATE_TRUNC('day',current_timestamp) + interval '23' hour + interval '59' minute + interval '59' second)
group by 
    Return_SOC_Received.shipment_id 
    ,Pub_shipment_order_type.order_type
    ,Pub_shipment_order_type.order_type_name
    ,Pub_shipment_order_type.return_to_seller_region
    ,Return_SOC_Received_Time
    ,Return_SOC_packed_Time
    ,Return_SOC_LHPacked_Time
    ,Return_SOC_LHTransporting_Time
    ,Return_SOC_LHTransported_Time
    ,Return_SOC_returning_Time
    ,Return_SOC_returned_Time
    ,Return_SOC_Assigning_Time
    ,Return_Failed_Time
    ,Return_SOC_received_operator
    ,Return_SOC_LHPacked_operator
    ,Return_SOC_LHTransporting_operator
order by 
    Return_SOC_Received_Time asc