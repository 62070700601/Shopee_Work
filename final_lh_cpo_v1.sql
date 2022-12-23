
/* 
    --DROP TABLE IF EXISTS dev_thopsbi_lof.spx_analytics_cost_lh_cpo_v1;
    create table dev_thopsbi_lof.spx_analytics_cost_lh_cpo_v1
    (
     shipment_id                  VARCHAR 
    ,lh_report_date               DATE
    ,lh_report_month              DATE    
    ,buyer_region_name            VARCHAR
    ,is_route_type_direct         BOOLEAN
    ,is_route_type_shuttle        BOOLEAN
    ,is_route_type_transit        BOOLEAN
    ,direct_origin_station        VARCHAR
    ,direct_destination_station   VARCHAR
    ,shuttle_origin_station       VARCHAR
    ,shuttle_destination_station  VARCHAR
    ,transit_origin_station       VARCHAR
    ,transit_destination_station  VARCHAR
    ,total_lh_cost DECIMAL(16,7)
    ,direct_cost   DECIMAL(16,7)
    ,shuttle_cost  DECIMAL(16,7)
    ,transit_cost  DECIMAL(16,7)
    ,adhoc_cost    DECIMAL(16,7)
    ,unused_cost   DECIMAL(16,7)     
    ,am_cost       DECIMAL(16,7)
    ,reverse_cost   DECIMAL(16,7)
    ,other_cost    DECIMAL(16,7)  
    ,ingestion_timestamp TIMESTAMP
    ,partition_date DATE
   )
   WITH
   (
      FORMAT = 'Parquet',
      PARTITIONED_BY = array['partition_date']
   );
 */

insert into dev_thopsbi_lof.spx_analytics_cost_lh_cpo_v1 
with raw_lh_type as 
(
select 
    shipment_id
    ,buyer_region_name
    ,"4pl_flag" as is_4pl
    ,first_lm_hub_station_name
    ,case when first_soc_lh_transporting_timestamp is not null then first_soc_lh_transporting_timestamp else first_soc_lh_transported_timestamp end as soc_lh_timestamp 
    ,case when first_rc_lh_transporting_timestamp is not null then first_rc_lh_transporting_timestamp else first_rc_lh_transported_timestamp end as rc_lh_timestamp
    ,first_rc_received_timestamp
    ,first_soc_received_timestamp
    ,fm_lh_transported_timestamp
    ,first_soc_station_name
    ,delivery_rc_station_name
    ,latest_rc_received_timestamp
    ,latest_soc_received_timestamp
    ,delivered_timestamp
from thopsbi_lof.dwd_thspx_pub_shipment_info_di_th
)
,region_map as 
(
    select 
    *
    ,case 
        when date(soc_lh_timestamp) between date('2022-02-01') and date('2022-02-28') then date('2022-02-01')
        when date(rc_lh_timestamp) between date('2022-02-01') and date('2022-02-28') then date('2022-02-01')
    end as report_month 
    ,case 
        when date(soc_lh_timestamp) between date('2022-02-01') and date('2022-02-28') then date(soc_lh_timestamp)
        when date(rc_lh_timestamp) between date('2022-02-01') and date('2022-02-28') then date(rc_lh_timestamp)
    end as lh_report_date
    ,case 
        when buyer_region_name in ('NORTH','EAST','WEST','CENTRAL','GBKK') and is_4pl = false  and soc_lh_timestamp is not null and delivery_rc_station_name is null then 1 
        else 0 
    end as is_route_type_direct 
    ,case 
        when buyer_region_name in ('NORTH','CENTRAL','NORTHEAST','SOUTH') and is_4pl = false and delivery_rc_station_name is not null and soc_lh_timestamp is not null then 1 
        else 0 
    end as is_route_type_shuttle
    ,case when buyer_region_name in ('NORTH','CENTRAL','NORTHEAST','SOUTH') and is_4pl = false and delivery_rc_station_name is not null and rc_lh_timestamp is not null  then 1 
        else 0 
    end as is_route_type_transit
    from raw_lh_type
)
,route_map as
(
select 
    *
    ,case when is_route_type_direct = 1 then first_soc_station_name else null end as direct_origin_station 
    ,case when is_route_type_direct = 1 then first_lm_hub_station_name else null  end as direct_destination_station 
    ,case when is_route_type_transit = 1 then delivery_rc_station_name else null  end as transit_origin_station
    ,case when is_route_type_transit = 1 then first_lm_hub_station_name else null  end as transit_destination_station 
    ,case when is_route_type_shuttle = 1 then first_soc_station_name else null  end  as shuttle_origin_station 
    ,case when is_route_type_shuttle = 1 then delivery_rc_station_name else null end as shuttle_destination_station 
from region_map
where report_month is not null 
)
,volume_table as
(
select 
    report_month
    ,sum(case when is_route_type_direct = 1 and is_4pl = false  and buyer_region_name = 'NORTH' then 1 else 0 end) as direct_north 
    ,sum(case when is_route_type_direct = 1 and is_4pl = false and buyer_region_name = 'EAST' then 1 else 0 end) as direct_east 
    ,sum(case when is_route_type_direct = 1 and is_4pl = false and buyer_region_name = 'WEST' then 1 else 0 end) as direct_west 
    ,sum(case when is_route_type_direct = 1 and is_4pl = false and buyer_region_name = 'CENTRAL' then 1 else 0 end) as direct_central 
    ,sum(case when is_route_type_direct = 1 and is_4pl = false and buyer_region_name = 'GBKK' then 1 else 0 end) as direct_gbkk 

    ,sum(case when is_route_type_shuttle = 1 and buyer_region_name = 'NORTH' then 1 else 0 end) as shuttle_north 
    ,sum(case when is_route_type_shuttle = 1 and buyer_region_name = 'CENTRAL' then 1 else 0 end) as shuttle_central 
    ,sum(case when is_route_type_shuttle = 1 and buyer_region_name = 'NORTHEAST' then 1 else 0 end) as shuttle_northeast  
    ,sum(case when is_route_type_shuttle = 1 and buyer_region_name = 'SOUTH' then 1 else 0 end) as shuttle_south 

    ,sum(case when is_route_type_transit = 1 and buyer_region_name = 'NORTH' then 1 else 0 end) as transit_north 
    ,sum(case when is_route_type_transit = 1 and buyer_region_name = 'CENTRAL' then 1 else 0 end) as transit_central 
    ,sum(case when is_route_type_transit = 1 and buyer_region_name = 'NORTHEAST' then 1 else 0 end) as transit_northeast  
    ,sum(case when is_route_type_transit = 1 and buyer_region_name = 'SOUTH' then 1 else 0 end) as transit_south 

    ,sum(case when buyer_region_name = 'GBKK' then 1 else 0 end) as gbkk_lh_volume 
    ,sum(case when buyer_region_name = 'CENTRAL' then 1 else 0 end) as central_lh_volume 
    ,sum(case when buyer_region_name = 'EAST' then 1 else 0 end) as east_lh_volume
    ,sum(case when buyer_region_name = 'WEST' then 1 else 0 end) as west_lh_volume 
    ,sum(case when buyer_region_name = 'NORTH' then 1 else 0 end) as north_lh_volume
    ,sum(case when buyer_region_name = 'NORTHEAST' then 1 else 0 end) as northeast_lh_volume
    ,sum(case when buyer_region_name = 'SOUTH' then 1 else 0 end) as south_lh_volume    
from route_map
group by
    report_month
)
,lh_cost_cal as
(
select 
    shipment_id 
    ,delivered_timestamp
    ,lh_report_date
    ,is_4pl
    ,route_map.report_month as lh_report_month
    ,buyer_region_name
    ,case when is_route_type_direct = 1 then true else false end as is_route_type_direct
    ,case when is_route_type_shuttle = 1 then true  else false  end as is_route_type_shuttle
    ,case when is_route_type_transit = 1 then true else false end as is_route_type_transit
    ,direct_origin_station
    ,direct_destination_station
    ,shuttle_origin_station
    ,shuttle_destination_station
    ,transit_origin_station
    ,transit_destination_station
    ,case when is_route_type_direct = 1  then 
        case
            when buyer_region_name = 'NORTH' then cast(direct_cost as double )*1/direct_north 
            when buyer_region_name = 'EAST' then cast(direct_cost as double )*1/direct_east
            when buyer_region_name = 'WEST' then cast(direct_cost as double )*1/direct_west
            when buyer_region_name = 'CENTRAL' then cast(direct_cost as double )*1/direct_central
            when buyer_region_name = 'GBKK' then cast(direct_cost as double )*1/direct_gbkk 
            else 0 
        end
        else 0 
    end as direct_cost
    ,case when is_route_type_shuttle = 1 then 
        case
            when buyer_region_name = 'NORTH' then cast(shuttle_cost as double )*1/shuttle_north 
            when buyer_region_name = 'CENTRAL' then cast(shuttle_cost as double )*1/shuttle_central
            when buyer_region_name = 'NORTHEAST' then cast(shuttle_cost as double )*1/shuttle_northeast 
            when buyer_region_name = 'SOUTH' then cast(shuttle_cost as double )*1/shuttle_south
            else 0 
        end
        else 0 
    end as shuttle_cost
    ,case 
        when is_route_type_transit = 1  then 
        case
            when buyer_region_name = 'NORTH' then cast(transit_cost as double )*1/transit_north 
            when buyer_region_name = 'CENTRAL' then cast(transit_cost as double )*1/transit_central
            when buyer_region_name = 'NORTHEAST' then cast(transit_cost as double )*1/transit_northeast 
            when buyer_region_name = 'SOUTH' then cast(transit_cost as double )*1/transit_south
            else 0 
        end
        else 0 
    end as transit_cost
    ,case 
        when buyer_region_name = 'GBKK' then cast(adhoc_cost as double)*1/gbkk_lh_volume
        when buyer_region_name = 'CENTRAL' then cast(adhoc_cost as double)*1/central_lh_volume
        when buyer_region_name = 'WEST' then cast(adhoc_cost as double)*1/west_lh_volume
        when buyer_region_name = 'EAST' then cast(adhoc_cost as double)*1/east_lh_volume
        when buyer_region_name = 'NORTH' then cast(adhoc_cost as double)*1/north_lh_volume
        when buyer_region_name = 'NORTHEAST' then cast(adhoc_cost as double)*1/northeast_lh_volume
        when buyer_region_name = 'SOUTH' then cast(adhoc_cost as double)*1/south_lh_volume
        else 0
    end as adhoc_cost 
    ,case 
        when buyer_region_name = 'GBKK' then cast(unused_cost as double)*1/gbkk_lh_volume
        when buyer_region_name = 'CENTRAL' then cast(unused_cost as double)*1/central_lh_volume
        when buyer_region_name = 'WEST' then cast(unused_cost as double)*1/west_lh_volume
        when buyer_region_name = 'EAST' then cast(unused_cost as double)*1/east_lh_volume
        when buyer_region_name = 'NORTH' then cast(unused_cost as double)*1/north_lh_volume
        when buyer_region_name = 'NORTHEAST' then cast(unused_cost as double)*1/northeast_lh_volume
        when buyer_region_name = 'SOUTH' then cast(unused_cost as double)*1/south_lh_volume
        else 0
    end as unused_cost 
    ,case 
        when buyer_region_name = 'GBKK' then cast(am_cost as double)*1/gbkk_lh_volume
        when buyer_region_name = 'CENTRAL' then cast(am_cost as double)*1/central_lh_volume
        when buyer_region_name = 'WEST' then cast(am_cost as double)*1/west_lh_volume
        when buyer_region_name = 'EAST' then cast(am_cost as double)*1/east_lh_volume
        when buyer_region_name = 'NORTH' then cast(am_cost as double)*1/north_lh_volume
        when buyer_region_name = 'NORTHEAST' then cast(am_cost as double)*1/northeast_lh_volume
        when buyer_region_name = 'SOUTH' then cast(am_cost as double)*1/south_lh_volume
        else 0
    end as am_cost 
    ,case 
        when buyer_region_name = 'GBKK' then cast(return_cost as double)*1/gbkk_lh_volume
        when buyer_region_name = 'CENTRAL' then cast(return_cost as double)*1/central_lh_volume
        when buyer_region_name = 'WEST' then cast(return_cost as double)*1/west_lh_volume
        when buyer_region_name = 'EAST' then cast(return_cost as double)*1/east_lh_volume
        when buyer_region_name = 'NORTH' then cast(return_cost as double)*1/north_lh_volume
        when buyer_region_name = 'NORTHEAST' then cast(return_cost as double)*1/northeast_lh_volume
        when buyer_region_name = 'SOUTH' then cast(return_cost as double)*1/south_lh_volume
        else 0
    end as return_cost 
    ,case 
        when buyer_region_name = 'GBKK' then cast(other_cost as double)*1/gbkk_lh_volume
        when buyer_region_name = 'CENTRAL' then cast(other_cost as double)*1/central_lh_volume
        when buyer_region_name = 'WEST' then cast(other_cost as double)*1/west_lh_volume
        when buyer_region_name = 'EAST' then cast(other_cost as double)*1/east_lh_volume
        when buyer_region_name = 'NORTH' then cast(other_cost as double)*1/north_lh_volume
        when buyer_region_name = 'NORTHEAST' then cast(other_cost as double)*1/northeast_lh_volume
        when buyer_region_name = 'SOUTH' then cast(other_cost as double)*1/south_lh_volume
        else 0 
      end as other_cost 
from route_map
left join dev_thopsbi_lof.spx_lh_cost_ingest_v2 as lh_cost_table 
on route_map.report_month = cast(lh_cost_table.report_month as date)
and lh_cost_table.lh_region = route_map.buyer_region_name
left join volume_table
on volume_table.report_month = route_map.report_month
),
agg_lh_cost as
(
select 
    shipment_id   
    ,lh_report_date  
    ,lh_report_month  
    ,buyer_region_name  
    ,is_route_type_direct
    ,is_route_type_shuttle  
    ,is_route_type_transit  
    ,direct_origin_station  
    ,direct_destination_station  
    ,shuttle_origin_station  
    ,shuttle_destination_station  
    ,transit_origin_station  
    ,transit_destination_station  
    ,direct_cost + shuttle_cost + transit_cost + adhoc_cost + unused_cost + am_cost + return_cost + other_cost as total_lh_cost  
    ,direct_cost  
    ,shuttle_cost   
    ,transit_cost   
    ,adhoc_cost    
    ,unused_cost        
    ,am_cost    
    ,return_cost as reverse_cost 
    ,other_cost   
from lh_cost_cal
)
select 
    cast(shipment_id as VARCHAR) 
    ,cast(lh_report_date as DATE)
    ,CAST(lh_report_month AS DATE)    
    ,CAST(buyer_region_name AS VARCHAR)
    ,CAST(is_route_type_direct AS BOOLEAN)
    ,CAST(is_route_type_shuttle AS BOOLEAN)
    ,CAST(is_route_type_transit AS BOOLEAN)
    ,CAST(direct_origin_station AS VARCHAR)
    ,CAST(direct_destination_station AS VARCHAR)
    ,CAST(shuttle_origin_station AS VARCHAR)
    ,CAST(shuttle_destination_station AS VARCHAR)
    ,CAST(transit_origin_station AS VARCHAR)
    ,CAST(transit_destination_station AS VARCHAR)
    ,CAST(total_lh_cost AS  DECIMAL(16,7))
    ,CAST(direct_cost AS  DECIMAL(16,7))
    ,CAST(shuttle_cost AS  DECIMAL(16,7))
    ,CAST(transit_cost AS  DECIMAL(16,7))
    ,CAST(adhoc_cost AS  DECIMAL(16,7)) 
    ,CAST(unused_cost AS  DECIMAL(16,7))     
    ,CAST(am_cost AS  DECIMAL(16,7)) 
    ,CAST(reverse_cost AS  DECIMAL(16,7)) 
    ,CAST(other_cost AS DECIMAL(16,7))  
    ,CAST(CURRENT_TIMESTAMP + INTERVAL '-1' HOUR AS TIMESTAMP) AS ingestion_timestamp
    ,date('2022-02-01') AS partition_date
from agg_lh_cost
/* select 
    sum(total_lh_cost) as total_lh_cost 
    ,sum(direct_cost) as direct_cost
    ,sum(shuttle_cost) as  shuttle_cost 
    ,sum(transit_cost) as transit_cost
    ,sum(am_cost) as am_cost
    ,sum(reverse_cost) as return_cost
    ,sum(adhoc_cost) as adhoc_cost
    ,sum(unused_cost) as unused_cost
    ,sum(other_cost) as other_cost 

from dev_thopsbi_lof.spx_analytics_cost_lh_cpo_v1 
where lh_report_month = date('2022-06-01')
 */
--- run final output 
 /* select
    lh_report_month
    ,buyer_region_name
    --,direct_origin_station
    --,direct_destination_station
    --,shuttle_origin_station
    --,shuttle_destination_station
    --,transit_origin_station
    --,transit_destination_station 
    



    ,sum(direct_cost + shuttle_cost + transit_cost + reverse_cost + am_cost + adhoc_cost + unused_cost)/sum(case when lh_report_month is not null  then 1 else 0 end) as overall_cpo_lh
    ,sum(direct_cost)/sum(case when lh_report_month is not null  then 1 else 0 end) as direct_cpo_lh
    ,sum(shuttle_cost)/sum(case when lh_report_month is not null  then 1 else 0 end) as shuttle_cpo_lh
    ,sum(transit_cost)/sum(case when lh_report_month is not null  then 1 else 0 end) as transit_cpo_lh
    ,sum(reverse_cost)/sum(case when lh_report_month is not null  then 1 else 0 end ) as reverse_cpo_lh
    ,sum(am_cost)/sum(case when lh_report_month is not null  then 1 else 0 end ) as am_cpo_lh
    ,sum(adhoc_cost)/sum(case when lh_report_month is not null then 1 else 0 end ) as adhoc_cpo_lh
    ,sum(unused_cost)/sum(case when lh_report_month is not null then 1 else 0 end ) as unused_cpo_lh
    ,sum(other_cost)/sum(case when lh_report_month is not null  then 1 else 0 end ) as other_cpo_lh

    ,try(cast(sum(direct_cost) as double)/sum(case when is_route_type_direct = true and lh_report_month is not null then 1 else 0 end)) as direct_cpo_actual_vol
    ,try(sum(shuttle_cost)/sum(case when is_route_type_shuttle = true and  lh_report_month is not null then 1 else 0 end)) as shuttle_cpo_actual_vol
    ,try(sum(transit_cost)/sum(case when is_route_type_transit = true and  lh_report_month is not null then 1 else 0 end)) as transit_cpo_actual_vol  


    ,sum(direct_cost) as direct_cost
    ,sum(shuttle_cost) as  shuttle_cost 
    ,sum(transit_cost) as transit_cost
    ,sum(am_cost) as am_cost
    ,sum(reverse_cost) as return_cost
    ,sum(adhoc_cost) as adhoc_cost
    ,sum(unused_cost) as unused_cost
    ,sum(other_cost) as other_cost 

    
    ,sum(case when lh_report_month is not null then 1 else 0 end) as total_lh_volume 
    ,sum(case when is_route_type_direct = true and lh_report_month is not null then 1 else 0 end) as direct_volume
    ,sum(case when is_route_type_shuttle = true and  lh_report_month is not null then 1 else 0 end) as shuttle_volume 
    ,sum(case when is_route_type_transit = true and  lh_report_month is not null then 1 else 0 end) as transit_volume


    --,sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31')  and raw_lh_type.is_4pl = false  then 1 else 0 end) as spx_e2e_volume
    --,sum(direct_cost + shuttle_cost + transit_cost + reverse_cost + am_cost + adhoc_cost + unused_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end) as overall_cpo_del
    --,sum(direct_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end) as direct_cpo_del
    --,sum(shuttle_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end) as shuttle_cpo_del
    --,sum(transit_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end) as transit_cpo_del
    --,sum(reverse_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end ) as reverse_cpo_del
    --,sum(am_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end ) as am_cpo_del
    --,sum(adhoc_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end ) as adhoc_cpo_del
    --,sum(unused_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end ) as unused_cpo_del
    --,sum(other_cost)/sum(case when raw_lh_type.delivered_timestamp between date('2022-05-01') and date('2022-05-31') and raw_lh_type.is_4pl = false then 1 else 0 end ) as other_cpo_del

    


from dev_thopsbi_lof.spx_analytics_cost_lh_cpo_v1 
group by 1,2   */












