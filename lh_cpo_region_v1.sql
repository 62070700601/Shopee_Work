with raw_lh_type as (
 select 
    fleet_order.shipment_id
/*   
    ,CASE 
        WHEN pickup.seller_province IS NOT NULL THEN pickup.seller_province
        ELSE dropoff.seller_province 
    END AS seller_province 
*/
    ,buyer_info.buyer_addr_state as buyer_province
    ,buyer_info.buyer_addr_district as buyer_district
    ,CASE 
        WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 
        THEN 1 ELSE 0 
    END AS is_4pl
    ,station.station_name as lm_station_name
    ,soc_lh_transported
    ,rc_lh_transported
    ,rc_received
    ,soc_received
    ,return_transported
    ,fm_lh_transported
    ,shuttle_rc_station.shuttle_rc_station 
    ,max_rc_received
    ,max_soc_received
    ,lh_driver_tab.lh_driver
    --,raw_hub_type.station_type
    ,CAST(IF(manual_package_length IS NOT NULL and manual_package_length != 0, manual_package_length, if(sls_package_length = 0,7,sls_package_length)) AS double)/100 as lenght_parcel
    ,CAST(IF(manual_package_width IS NOT NULL and manual_package_length != 0, manual_package_width, if(sls_package_width = 0,7,sls_package_width)) AS double)/100 as width_parcel
    ,CAST(IF(manual_package_height IS NOT NULL and manual_package_length != 0, manual_package_height, if(sls_package_height = 0,7,sls_package_height)) AS double)/100 AS height_parcel
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join 
    (
    select
        shipment_id
        ,min(case when status in (15,36) and station_id in (3,242) then from_unixtime(ctime-3600) end) as soc_lh_transported 
        ,max(case when status in (15,36) and station_id in (71,77,82,983,1479,1480) then from_unixtime(ctime-3600) end) as rc_lh_transported 
        ,min(case when status in (47,48) then from_unixtime(ctime-3600) end) as fm_lh_transported 
        ,min(case when status in (10,56,57,64,65,82,83,84,85,86,87,95) then from_unixtime(ctime-3600) end) as return_transported     
        ,min(case when status = 8 and station_id in (71,77,82,983,1479,1480)  then from_unixtime(ctime-3600) end) as rc_received 
        ,min(case when status = 8 and station_id in (71,77,82,983,1479,1480)  then from_unixtime(ctime-3600) end) as soc_received 
        ,max(case when status = 8 and station_id in (71,77,82,983,1479,1480)  then from_unixtime(ctime-3600) end) as max_rc_received 
        ,max(case when status = 8 and station_id in (71,77,82,983,1479,1480)  then from_unixtime(ctime-3600) end) as max_soc_received 
    from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    where 
        date(from_unixtime(ctime-3600)) >= date('2021-01-12') and date(from_unixtime(ctime-3600)) <= date('2022-02-28')
    group by
        shipment_id 
    ) as order_track 
on order_track.shipment_id = fleet_order.shipment_id
left join
    (
    select 
        shipment_id
        ,station.station_name as shuttle_rc_station 
    from 
        (
        select 
            shipment_id 
            ,station_id as rc_station_id 
            ,row_number() over(partition by shipment_id order by ctime desc) as row_number 
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 8 
            and station_id in (71,77,82,983,1479,1480) 
            and date(from_unixtime(ctime-3600)) >= date('2021-01-12') 
            and date(from_unixtime(ctime-3600)) <= date('2022-02-28')
        ) as rc_rec_track 
    left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station 
    on rc_rec_track.rc_station_id = station.id
    where 
        row_number = 1 
    ) as shuttle_rc_station
    on shuttle_rc_station.shipment_id = fleet_order.shipment_id

LEFT JOIN spx_mart.shopee_fleet_order_th_db__buyer_info_tab__reg_continuous_s0_live  AS buyer_info
ON  fleet_order.shipment_id = buyer_info.shipment_id

LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as station 
ON fleet_order.station_id = station.id

left join 
    (
    select 
        shipment_id
        ,lh_task_id
    from 
        (
        select 
            shipment_id
            ,try(cast(json_extract(json_parse(content),'$.linehaul_task_id') as varchar)) as lh_task_id
            ,row_number() over(partition by shipment_id order by ctime) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status in (45,46) 
            and date(from_unixtime(ctime-3600)) >= date('2021-01-12') 
            and date(from_unixtime(ctime-3600)) <= date('2022-02-28')
        ) as order_track
    where row_number = 1
    ) as lh_task_am 
on lh_task_am.shipment_id = fleet_order.shipment_id
left join
    (
    select
        lh_task.task_number
        ,lh_task.driver_id as lh_driver_id  
        ,driver_tab.driver_name as lh_driver
    from spx_mart.shopee_fms_th_db__line_haul_task_tab__th_continuous_s0_live as lh_task
    left join spx_mart.shopee_fms_th_db__driver_tab__th_continuous_s0_live as driver_tab
    on lh_task.driver_id = driver_tab.driver_id
    ) as lh_driver_tab
on lh_driver_tab.task_number = lh_task_am.lh_task_id  
/* left join 
    (
    select 
        shipment_id
        ,station_id
    from 
        (
        select 
            shipment_id
            ,station_id
            ,row_number() over(partition by shipment_id order by ctime) as row_number
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 42 
        )
    where 
        row_number = 1 
    ) as fm_rec_track 
on fm_rec_track.shipment_id = fleet_order.shipment_id
left join raw_hub_type 
on cast(raw_hub_type.id as int) = cast(fm_rec_track.station_id as int) 
*/
)
,region_map as 
(
select 
    *
    ,buyer_region.lh_region as buyer_region
    --,seller_region.lh_region as seller_region 
    ,case 
        when 
            date(soc_lh_transported) between date('2022-01-01') and date('2022-01-31') 
            or date(rc_lh_transported) between date('2022-01-01') and date('2022-01-31')
            or date(return_transported) between date('2022-01-01') and date('2022-01-31')
            or(date (fm_lh_transported) between date('2022-01-01') and date('2022-01-31') and lh_driver like 'LH%' ) then '2022-01-01' 
        else null 
    end as report_month 
    ,case 
        when buyer_region.lh_region in ('NORTH','EAST','WEST','CENTRAL','GBKK') and is_4pl = 0 and soc_lh_transported is not null and (max_rc_received < max_soc_received or max_rc_received is null) then 1 
        else 0 
    end as is_route_type_direct 
    ,case 
        when buyer_region.lh_region in ('NORTH','CENTRAL','NORTHEAST','SOUTH') and is_4pl = 0 and rc_lh_transported > soc_received then 1 
        else 0 
    end as is_route_type_shuttle
    ,case 
        when buyer_region.lh_region in ('NORTH','CENTRAL','NORTHEAST','SOUTH') and is_4pl = 0 and rc_lh_transported > soc_received then 1 
        else 0 
    end as is_route_type_transit
    ,case when lh_driver like 'LH%' and buyer_region.lh_region != 'GBKK' then 1 else 0 end as is_route_type_am  
    ,case when return_transported is not null then 1 else 0 end as is_route_type_reverse 
    ,case 
        when buyer_region.lh_region in ('NORTH') and shuttle_rc_station in ('NORC-A','NORC-B') and rc_lh_transported > soc_received then shuttle_rc_station
        when buyer_region.lh_region in ('NORTHEAST') and shuttle_rc_station in ('NERC-A','NERC-B') and rc_lh_transported > soc_received then shuttle_rc_station
        when buyer_region.lh_region in ('SOUTH') and shuttle_rc_station in ('SORC-A','SORC-B') and rc_lh_transported > soc_received then shuttle_rc_station
        when buyer_region.lh_region in ('CENTRAL') and shuttle_rc_station in ('CERC') and rc_lh_transported > soc_received then shuttle_rc_station
        ELSE 
            case 
               when  buyer_region.lh_region in ('NORTH') then 'NORC-A'
               when  buyer_region.lh_region in ('NORTHEAST') then 'NERC-A'
               when  buyer_region.lh_region in ('SOUTH') then 'SORC-A'
               when  buyer_region.lh_region in ('CENTRAL') then 'CERC'
            end 
    end as shuttle_rc_station_final
    ,lenght_parcel*height_parcel*width_parcel as total_dimension
from  raw_lh_type
left join thopsbi_lof.spx_index_region_temp as buyer_region
on buyer_region.province = raw_lh_type.buyer_province
and buyer_region.district = raw_lh_type.buyer_district   
)
,route_map as
(
select 
    *
    ,case when is_route_type_direct = 1 then 'SOC' else '-' end as direct_origin_station 
    ,case when is_route_type_direct = 1 then lm_station_name else '-' end as direct_destination_station 
    ,case when is_route_type_transit = 1 then shuttle_rc_station_final else '-' end as transit_origin_station
    ,case when is_route_type_transit = 1 then lm_station_name else '-' end as transit_destination_station 
    ,case when is_route_type_shuttle = 1 then 'SOCE' else '-' end  as shuttle_origin_station 
    ,case when is_route_type_shuttle = 1 then shuttle_rc_station_final else '-' end as shuttle_destination_station 
from region_map
where 
    report_month is not null 
)
,volume_table as
(
select 
    report_month
    ,sum(case when is_route_type_direct = 1 and is_4pl = 0 and buyer_region = 'NORTH' then total_dimension else 0 end) as direct_north 
    ,sum(case when is_route_type_direct = 1 and is_4pl = 0 and buyer_region = 'EAST' then total_dimension else 0 end) as direct_east 
    ,sum(case when is_route_type_direct = 1 and is_4pl = 0 and buyer_region = 'WEST' then total_dimension else 0 end) as direct_west 
    ,sum(case when is_route_type_direct = 1 and is_4pl = 0 and buyer_region = 'CENTRAL' then total_dimension else 0 end) as direct_central 
    ,sum(case when is_route_type_direct = 1 and is_4pl = 0 and buyer_region = 'GBKK' then total_dimension else 0 end) as direct_gbkk 
    ,sum(case when is_route_type_shuttle = 1 and buyer_region = 'NORTH' then total_dimension else 0 end) as shuttle_north 
    ,sum(case when is_route_type_shuttle = 1 and buyer_region = 'CENTRAL' then total_dimension else 0 end) as shuttle_central 
    ,sum(case when is_route_type_shuttle = 1 and buyer_region = 'NORTHEAST' then total_dimension else 0 end) as shuttle_northeast  
    ,sum(case when is_route_type_shuttle = 1 and buyer_region = 'SOUTH' then total_dimension else 0 end) as shuttle_south 
    ,sum(case when is_route_type_transit = 1 and buyer_region = 'NORTH' then total_dimension else 0 end) as transit_north 
    ,sum(case when is_route_type_transit = 1 and buyer_region = 'CENTRAL' then total_dimension else 0 end) as transit_central 
    ,sum(case when is_route_type_transit = 1 and buyer_region = 'NORTHEAST' then total_dimension else 0 end) as transit_northeast  
    ,sum(case when is_route_type_transit = 1 and buyer_region = 'SOUTH' then total_dimension else 0 end) as transit_south 
    ,sum(case when is_route_type_reverse = 1 and buyer_region = 'GBKK' then total_dimension else 0 end) as return_gbkk
    ,sum(case when is_route_type_reverse = 1 and buyer_region = 'CENTRAL' then total_dimension else 0 end) as return_central 
    ,sum(case when is_route_type_reverse = 1 and buyer_region = 'EAST' then total_dimension else 0 end) as return_east 
    ,sum(case when is_route_type_reverse = 1 and buyer_region = 'WEST' then total_dimension else 0 end) as return_west
    ,sum(case when is_route_type_reverse = 1 and buyer_region = 'NORTH' then total_dimension else 0 end) as return_north
    ,sum(case when is_route_type_reverse = 1 and buyer_region = 'NORTHEAST' then total_dimension else 0 end) as return_northeast
    ,sum(case when is_route_type_reverse = 1 and buyer_region = 'SOUTH' then total_dimension else 0 end) as return_south 
    ,sum(case when is_route_type_am = 1 and buyer_region = 'CENTRAL' then total_dimension else 0 end) as am_central 
    ,sum(case when is_route_type_am = 1 and buyer_region = 'EAST' then total_dimension else 0 end) as am_east 
    ,sum(case when is_route_type_am = 1 and buyer_region = 'WEST' then total_dimension else 0 end) as am_west
    ,sum(case when is_route_type_am = 1 and buyer_region = 'NORTH' then total_dimension else 0 end) as am_north
    ,sum(case when is_route_type_am = 1 and buyer_region = 'NORTHEAST' then total_dimension else 0 end) as am_northeast
    ,sum(case when is_route_type_am = 1 and buyer_region = 'SOUTH' then total_dimension else 0 end) as am_south 
    ,sum(case when buyer_region = 'GBKK' then total_dimension else 0 end) as gbkk_lh_volume 
    ,sum(case when buyer_region = 'CENTRAL' then total_dimension else 0 end) as central_lh_volume 
    ,sum(case when buyer_region = 'EAST' then total_dimension else 0 end) as east_lh_volume
    ,sum(case when buyer_region = 'WEST' then total_dimension else 0 end) as west_lh_volume 
    ,sum(case when buyer_region = 'NORTH' then total_dimension else 0 end) as north_lh_volume
    ,sum(case when buyer_region = 'NORTHEAST' then total_dimension else 0 end) as northeast_lh_volume
    ,sum(case when buyer_region = 'SOUTH' then total_dimension else 0 end) as south_lh_volume    
from route_map
group by
    report_month
)
,lh_cost_cal as
(
select 
    shipment_id 
    ,route_map.report_month
    ,buyer_region
    ,is_route_type_direct
    ,is_route_type_shuttle
    ,is_route_type_transit
    ,is_route_type_reverse
    ,is_route_type_am
    ,direct_origin_station
    ,direct_destination_station
    ,shuttle_origin_station
    ,shuttle_destination_station
    ,transit_origin_station
    ,transit_destination_station
    ,case 
        when is_route_type_direct = 1 then 
            case
                when buyer_region = 'NORTH' then cast(direct_cost as double )*total_dimension/direct_north 
                when buyer_region = 'EAST' then cast(direct_cost as double )*total_dimension/direct_east
                when buyer_region = 'WEST' then cast(direct_cost as double )*total_dimension/direct_west
                when buyer_region = 'CENTRAL' then cast(direct_cost as double )*total_dimension/direct_central
                when buyer_region = 'GBKK' then cast(direct_cost as double )*total_dimension/direct_gbkk 
                else 0 
            end
        else 0 
    end as direct_cost
    ,case 
        when is_route_type_shuttle = 1 then 
            case
                when buyer_region = 'NORTH' then cast(shuttle_cost as double )*total_dimension/shuttle_north 
                when buyer_region = 'CENTRAL' then cast(shuttle_cost as double )*total_dimension/shuttle_central
                when buyer_region = 'NORTHEAST' then cast(shuttle_cost as double )*total_dimension/shuttle_northeast 
                when buyer_region = 'SOUTH' then cast(shuttle_cost as double )*total_dimension/shuttle_south
                else 0 
            end
        else 0 
    end as shuttle_cost
    ,case 
        when is_route_type_transit = 1  then 
        case
            when buyer_region = 'NORTH' then cast(transit_cost as double )*total_dimension/transit_north 
            when buyer_region = 'CENTRAL' then cast(transit_cost as double )*total_dimension/transit_central
            when buyer_region = 'NORTHEAST' then cast(transit_cost as double )*total_dimension/transit_northeast 
            when buyer_region = 'SOUTH' then cast(transit_cost as double )*total_dimension/transit_south
            else 0 
        end
        else 0 
    end as transit_cost
    ,case 
        when buyer_region = 'GBKK' then cast(adhoc_cost as double)*total_dimension/gbkk_lh_volume
        when buyer_region = 'CENTRAL' then cast(adhoc_cost as double)*total_dimension/central_lh_volume
        when buyer_region = 'WEST' then cast(adhoc_cost as double)*total_dimension/west_lh_volume
        when buyer_region = 'EAST' then cast(adhoc_cost as double)*total_dimension/east_lh_volume
        when buyer_region = 'NORTH' then cast(adhoc_cost as double)*total_dimension/north_lh_volume
        when buyer_region = 'NORTHEAST' then cast(adhoc_cost as double)*total_dimension/northeast_lh_volume
        when buyer_region = 'SOUTH' then cast(adhoc_cost as double)*total_dimension/south_lh_volume
    end as adhoc_cost 
    ,case 
        when buyer_region = 'GBKK' then cast(unused_cost as double)*total_dimension/gbkk_lh_volume
        when buyer_region = 'CENTRAL' then cast(unused_cost as double)*total_dimension/central_lh_volume
        when buyer_region = 'WEST' then cast(unused_cost as double)*total_dimension/west_lh_volume
        when buyer_region = 'EAST' then cast(unused_cost as double)*total_dimension/east_lh_volume
        when buyer_region = 'NORTH' then cast(unused_cost as double)*total_dimension/north_lh_volume
        when buyer_region = 'NORTHEAST' then cast(unused_cost as double)*total_dimension/northeast_lh_volume
        when buyer_region = 'SOUTH' then cast(unused_cost as double)*total_dimension/south_lh_volume
    end as unused_cost 
    ,case 
        when is_route_type_am = 1 then 
            case 
                when buyer_region = 'CENTRAL' then cast(am_cost as double)*total_dimension/am_central
                when buyer_region = 'WEST' then cast(am_cost as double)*total_dimension/am_west
                when buyer_region = 'EAST' then cast(am_cost as double)*total_dimension/am_east
                when buyer_region = 'NORTH' then cast(am_cost as double)*total_dimension/am_north
                when buyer_region = 'NORTHEAST' then cast(am_cost as double)*total_dimension/am_northeast
                when buyer_region = 'SOUTH' then cast(am_cost as double)*total_dimension/am_south
            end 
    end as am_cost 
    ,case 
        when is_route_type_reverse = 1 then 
            case 
                when buyer_region = 'GBKK' then cast(return_cost as double)*total_dimension/return_gbkk
                when buyer_region = 'CENTRAL' then cast(return_cost as double)*total_dimension/return_central
                when buyer_region = 'WEST' then cast(return_cost as double)*total_dimension/return_west
                when buyer_region = 'EAST' then cast(return_cost as double)*total_dimension/return_east
                when buyer_region = 'NORTH' then cast(return_cost as double)*total_dimension/return_north 
                when buyer_region = 'NORTHEAST' then cast(return_cost as double)*total_dimension/return_northeast
                when buyer_region = 'SOUTH' then cast(return_cost as double)*total_dimension/return_south
            end 
    end as return_cost 
from route_map
left join dev_thopsbi_lof.spx_lh_cost_ingest_v2 as lh_cost_table 
on route_map.report_month = lh_cost_table.report_month
and lh_cost_table.lh_region = route_map.buyer_region
left join volume_table
on volume_table.report_month = route_map.report_month
)
select
    report_month
    ,buyer_region
    ,direct_origin_station
    ,direct_destination_station
    ,shuttle_origin_station
    ,shuttle_destination_station
    ,transit_origin_station
    ,transit_destination_station
    ,sum(direct_cost)/sum(case when is_route_type_direct = 1 then 1 else 0 end) as direct_cpo
    ,sum(shuttle_cost)/sum(case when is_route_type_shuttle = 1 then 1 else 0 end) as shuttle_cpo 
    ,sum(transit_cost)/sum(case when is_route_type_transit = 1 then 1 else 0 end) as transit_cpo
    ,sum(return_cost)/sum(case when is_route_type_reverse = 1 then 1 else 0 end ) as reverse_cpo 
    ,sum(am_cost)/sum(case when is_route_type_am = 1 then 1 else 0 end ) as am_cpo 
    ,sum(adhoc_cost)/count(*) as adhoc_cpo
    ,sum(unused_cost)/count(*) as unused_cpo
    ,sum(direct_cost) as direct_cost
    ,sum(shuttle_cost) as  shuttle_cost 
    ,sum(transit_cost) as transit_cost
    ,sum(am_cost) as am_cost
    ,sum(return_cost) as return_cost
    ,sum(adhoc_cost) as adhoc_cost
    ,sum(unused_cost) as unused_cost
    ,sum(case when is_route_type_direct = 1 then 1 else 0 end) as direct_volume 
    ,sum(case when is_route_type_shuttle = 1 then 1 else 0 end) as shuttle_volume 
    ,sum(case when is_route_type_transit = 1 then 1 else 0 end)  as transit_volume 
    ,sum(case when is_route_type_reverse = 1 then 1 else 0 end ) as reverse_volume
    ,sum(case when is_route_type_am = 1 then 1 else 0 end ) as all_mile_volume 
    ,count(*) as lh_volume
from lh_cost_cal
group by 1,2,3,4,5,6,7,8
