with lost_cog_amount as
(
    select 
        date_time
        ,shipment_id
        ,cogs_amount
    from 
    (
        select 
            order_tracking.shipment_id
            ,date(FROM_UNIXTIME(order_tracking.ctime-3600)) as date_time
            ,order_tracking.status
            ,pub_shipment.cogs_amount
            ,row_number() over (partition by order_tracking.shipment_id order by FROM_UNIXTIME(order_tracking.ctime-3600) desc) as rank_num
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live as order_tracking
        left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
        on order_tracking.shipment_id = pub_shipment.shipment_id
        where 
            order_tracking.status = 11
            -- and order_tracking.shipment_id = 'SPXTH02273438988A'
           
    )
    where 
        rank_num = 1
        and status = 11
        and cogs_amount < 5000
        and date_time between date '2022-08-01 00:00:00.000' and date '2022-10-31 00:00:00.000'
        
    group by 
        date_time
        ,shipment_id
        ,cogs_amount
    order by 
        date_time asc
)
select
    shipment_id
    ,cogs_amount
    -- shipment_id
    -- ,manual_package_length_in_cm
    -- ,manual_package_width_in_cm
    -- ,if(manual_package_length_in_cm >= manual_package_width_in_cm,manual_package_length_in_cm,manual_package_width_in_cm) as max_number_width_length
    -- ,case 
    --     when if(manual_package_length_in_cm >= manual_package_width_in_cm,manual_package_length_in_cm,manual_package_width_in_cm) between 0.00 and 0.50 then '0.00 - 0.50 cm'
    --     when if(manual_package_length_in_cm >= manual_package_width_in_cm,manual_package_length_in_cm,manual_package_width_in_cm) between 0.51 and 25.00 then '0.51 - 25.00 cm'
    --     when if(manual_package_length_in_cm >= manual_package_width_in_cm,manual_package_length_in_cm,manual_package_width_in_cm) between 25.01 and 35.00 then '25.01 - 35.00 cm'
    --     when if(manual_package_length_in_cm >= manual_package_width_in_cm,manual_package_length_in_cm,manual_package_width_in_cm) between 25.01 and 35.00 then '25.01 - 35.00 cm'
    --     when if(manual_package_length_in_cm >= manual_package_width_in_cm,manual_package_length_in_cm,manual_package_width_in_cm) > 35.00 then '35.01 cm'
    -- end Range_Max_Number_width_length
from thopsbi_spx.dwd_pub_shipment_info_df_th
where 
    cogs_amount < 5000
    -- and shipment_id = 'SPXTH02333595395A'

-- limit 100