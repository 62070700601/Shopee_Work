with raw_soc_track as
(
select 
    fleet_order.shipment_id
    ,case 
        when non_shopee_flag = true then 'OSV'
        when marketplace_flag = true then 'MKP'
        when cross_border_flag = true then 'CB'
        when warehouse_flag = true then 'WH'
    END AS order_type
    ,case 
        when soc_rec_track.station_id = 3 then 'SOCE'
        when soc_rec_track.station_id = 242 then 'SOCW'
        when return_timestamp is not null then 'SOCE'
    end as soc_station 
    ,CASE 
        WHEN station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1 THEN 1 ELSE 0 
    END AS is_4pl
    ,case 
        when hour(soc_receive_timestamp) >= 6 and hour(soc_receive_timestamp) < 17 then 'rec-day-shift'
        when hour(soc_receive_timestamp) >= 17 then 'rec-night-shift'
    end as receive_shift
    ,case 
        when hour(pack_machine_time_stamp) >= 6 and hour(pack_machine_time_stamp) < 17 then 'pack-day-shift'
        when hour(pack_machine_time_stamp) >= 17 then 'pack-night-shift'
    end as pack_asm_shift
    ,case when hour(pack_manual_time_stamp) >= 6 and hour(pack_manual_time_stamp) < 17 then 'pack-day-shift'
      when hour(pack_manual_time_stamp) >= 17 then 'pack-night-shift'
    end as pack_manual_shift
    ,case 
        when hour(outbound_timestamp) >= 6 and hour(outbound_timestamp) < 17 then 'outbound-day-shift'
        when hour(outbound_timestamp) >= 17 then 'outbound-night-shift'
    end as outbound_shift
    ,case 
        when hour(return_timestamp) >= 6 and hour(return_timestamp) < 17 then 'return-day-shift'
        when hour(return_timestamp) >= 17 then 'return-night-shift'
    end as return_shift
    ,case 
        when hand_over_timestamp is not null and (station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1) then 
            case 
                when hour(hand_over_timestamp) >= 6 and hour(pack_time_stamp) < 17 then 'hand4pl-day-shift'
                when hour(hand_over_timestamp) >= 17 then 'hand4pl-night-shift'
            end 
        when hand_over_timestamp is null and (station.station_type = 5 OR station.station_name LIKE '%4PL%' OR fleet_order.channel_id > 1) then 
            case 
                when hour(pack_time_stamp) >= 6 and hour(pack_time_stamp) < 17 then 'hand4pl-day-shift'
                when hour(pack_time_stamp) >= 17 then 'hand4pl-night-shift'
            end 
    end as hand_4pl_shift 
    ,case 
        when lost_damage_timestamp is not null then 1 
        else 0 
    end as lost_damage_soc 
    ,case 
        when date(soc_receive_timestamp) between date('2022-01-01') and date('2022-01-31') then '2022-01-01'
        when date(return_timestamp) between date('2022-01-01') and date('2022-01-31') then '2022-01-01'
    end as report_month
    ,soc_receive_timestamp
    ,pack_time_stamp
    ,pack_machine_time_stamp
    ,pack_manual_time_stamp
    ,hand_over_timestamp
    ,outbound_timestamp
    ,return_timestamp
    ,non_shopee_flag
    ,cross_border_flag
    ,warehouse_flag
    ,marketplace_flag
FROM spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live AS fleet_order
LEFT JOIN spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live AS station 
ON fleet_order.station_id = station.id
LEFT JOIN 
    (
    select 
        shipment_id
        ,station_id   
        ,soc_receive_timestamp
    from 
        (
        SELECT 
            shipment_id
            ,station_id 
            ,from_unixtime(ctime-3600) as soc_receive_timestamp
            ,row_number() over (partition by shipment_id order by ctime ) as row_num 
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
        where 
            status = 8 
            and station_id in (3,242) and date(from_unixtime(ctime-3600) - interval '6' hour) between date('2022-01-01') and date('2022-01-31')
        ) 
        where row_num = 1 
    ) as soc_rec_track
on fleet_order.shipment_id = soc_rec_track.shipment_id
left join 
    (
    SELECT 
        shipment_id
        ,min(case when status = 33 and station_id in (3,242) then from_unixtime(ctime - 3600) end) as pack_time_stamp
        ,min(case when status = 33 and station_id in (3,242) and operator in ('IND000001', 'DWS000001', 'CAB000001','MQB000001') then from_unixtime(ctime - 3600) end) as pack_machine_time_stamp
        ,min(case when status = 33 and station_id in (3,242) and  operator not in ('IND000001', 'DWS000001', 'CAB000001','MQB000001') then from_unixtime(ctime - 3600) end) as pack_manual_time_stamp  
        ,min(case when status = 35 then from_unixtime(ctime - 3600) end) as hand_over_timestamp 
        ,min(case when status in (35,15,36) and station_id in (3,242) then from_unixtime(ctime-3600) end) as outbound_timestamp
        ,min(case when status = 58 and station_id in (3,242) then from_unixtime(ctime-3600) end ) as return_timestamp
        ,min(case when status in (11,12)  then from_unixtime(ctime-3600) end ) as lost_damage_timestamp
    FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
    where 
        date(from_unixtime(ctime-3600)) between date('2022-01-01') and date('2022-01-31')
    group by 
        shipment_id 
    ) as soc_status 
on fleet_order.shipment_id = soc_status.shipment_id
left join 
    (
    select 
        shipment_id
        ,non_shopee_flag
        ,cross_border_flag
        ,warehouse_flag
        ,marketplace_flag
    from thopsbi_lof.dwd_thspx_pub_shipment_info_di_th  
    ) as ana_dtm
on fleet_order.shipment_id = ana_dtm.shipment_id
)
,soc_volume_type as
(
select 
    case 
        when date(soc_receive_timestamp) between date('2022-01-01') and date('2022-01-31') then '2022-01-01'
        when date(return_timestamp) between date('2022-01-01') and date('2022-01-31') then '2022-01-01'
    end as report_month
    ,soc_station
    ,count(*) as total_soc_process_order 
    ,sum(case when receive_shift = 'rec-day-shift' and order_type != 'WH' then 1 else 0 end) as inbound_day_shift 
    ,sum(case when receive_shift = 'rec-night-shift' and order_type != 'WH' then 1 else 0 end) as inbound_night_shift
    ,sum(case when receive_shift = 'rec-day-shift' and order_type = 'WH' then 1 else 0 end) as wh_day_shift 
    ,sum(case when receive_shift = 'rec-night-shift' and order_type = 'WH' then 1 else 0 end) as wh_night_shift
    ,sum(case when pack_asm_shift = 'pack-day-shift'  then 1 else 0 end) as pack_asm_day_shift
    ,sum(case when pack_asm_shift = 'pack-night-shift'  then 1 else 0 end) as pack_asm_night_shift
    ,sum(case when pack_manual_shift = 'pack-day-shift'  then 1 else 0 end) as pack_manual_day_shift
    ,sum(case when pack_manual_shift = 'pack-night-shift'  then 1 else 0 end) as pack_manual_night_shift
    ,sum(case when outbound_shift = 'outbound-day-shift'  then 1 else 0 end) as outbound_day_shift
    ,sum(case when outbound_shift = 'outbound-night-shift'  then 1 else 0 end) as outbound_night_shift
    ,sum(case when return_shift = 'return-day-shift'  then 1 else 0 end) as return_day_shift
    ,sum(case when return_shift = 'return-night-shift'  then 1 else 0 end) as return_night_shift
    ,sum(case when hand_4pl_shift = 'hand4pl-day-shift'  then 1 else 0 end) as hand_4pl_day_shift
    ,sum(case when hand_4pl_shift = 'hand4pl-night-shift' then 1 else 0 end) as hand_4pl_night_shift
    ,sum(case when receive_shift = 'rec-day-shift' or pack_asm_shift = 'pack-day-shift' or pack_manual_shift = 'pack-day-shift' or outbound_shift = 'outbound-day-shift' or return_shift = 'return-day-shift' or hand_4pl_shift = 'hand4pl-day-shift' then 1 else 0 end) as day_shift_order 
    ,sum(case when receive_shift = 'rec-night-shift' or pack_asm_shift = 'pack-night-shift' or pack_manual_shift = 'pack-night-shift' or outbound_shift = 'outbound-night-shift' or return_shift = 'return-night-shift' or hand_4pl_shift = 'hand4pl-night-shift' then 1 else 0 end) as night_shift_order
    ,sum(case when lost_damage_soc is not null then 1 else 0 end) as lost_damage_volume
from raw_soc_track
group by 1,2
)
,agg_soc_volume_type as
(
select 
  *
  ,inbound_day_shift + inbound_night_shift as ot_inbound 
  ,wh_day_shift + wh_night_shift as ot_wh 
  ,pack_asm_day_shift + pack_asm_night_shift as ot_pack_asm 
  ,pack_manual_day_shift + pack_manual_night_shift as ot_pack_manual 
  ,outbound_day_shift + outbound_night_shift as ot_ob 
  ,return_day_shift + return_night_shift as ot_return
  ,hand_4pl_day_shift + hand_4pl_night_shift as ot_hand_4pl
from soc_volume_type
)
,soc_cost as
(
select 
    shipment_id
    ,is_4pl
    ,order_type
    ,raw_soc_track.report_month
    ,raw_soc_track.soc_station
    ,receive_shift
    ,pack_asm_shift
    ,pack_manual_shift
    ,hand_4pl_shift
    ,outbound_shift
    ,return_shift
    ,lost_damage_soc
    --- mpw day shift 
    ,case 
        when receive_shift = 'rec-day-shift' and order_type != 'WH' then cast(day_sub_con_inbound as double)/inbound_day_shift 
        else 0 
    end as day_sub_con_inbound
    ,case 
        when receive_shift = 'rec-day-shift' and order_type = 'WH' then cast(day_sub_con_wh_onsite as double)/wh_day_shift 
        else 0 
    end as day_sub_con_wh_onsite
    ,case 
        when pack_asm_shift = 'pack-day-shift'  then cast(day_sub_con_cbs as double)/pack_asm_day_shift 
        else 0 
    end as day_sub_con_cbs
    ,case 
        when outbound_shift = 'outbound-day-shift' then cast(day_sub_con_ob_sorting as double)/(outbound_day_shift + hand_4pl_day_shift) 
        else 0 
    end as day_sub_con_ob_sorting
    ,case 
        when outbound_shift = 'outbound-day-shift' then cast(day_sub_con_ob_dispatch as double)/outbound_day_shift 
        else 0 
    end as day_sub_con_ob_dispatch
    ,case 
        when hand_4pl_shift = 'hand4pl-day-shift'  then cast(day_sub_con_4pl as double)/hand_4pl_day_shift 
        else 0 
    end as day_sub_con_4pl
    ,case 
        when pack_manual_shift = 'pack-day-shift'  then cast(day_sub_con_manual_sorting_area as double)/pack_manual_day_shift 
        else 0 
    end as day_sub_con_manual_sorting_area
    ,case 
        when return_shift = 'return-day-shift'  then cast(day_sub_con_return as double)/return_day_shift 
        else 0 
    end as day_sub_con_return
    ,case 
        when receive_shift = 'rec-day-shift' or pack_asm_shift = 'pack-day-shift' or pack_manual_shift = 'pack-day-shift' or outbound_shift = 'outbound-day-shift' or return_shift = 'return-day-shift' or hand_4pl_shift = 'hand4pl-day-shift' then cast(day_sub_con_rework as double)/day_shift_order 
        else 0 
    end as day_sub_con_rework
    ,cast(day_sub_con_admin as double)/day_shift_order as admin_day_shift
    ,cast(day_sub_con_tech as double)/day_shift_order as tech_day_shift 
    --- mpw night shift      
    ,case 
        when receive_shift = 'rec-night-shift' and order_type != 'WH' then cast(night_sub_con_inbound as double)/inbound_night_shift 
        else 0 
    end as night_sub_con_inbound
    ,case 
        when receive_shift = 'rec-night-shift' and order_type = 'WH' then cast(night_sub_con_wh_onsite as double)/wh_night_shift 
        else 0 
    end as night_sub_con_wh_onsite
    ,case 
        when pack_asm_shift = 'pack-night-shift' then cast(night_sub_con_cbs as double)/pack_asm_night_shift 
        else 0 
    end as night_sub_con_cbs
    ,case 
        when outbound_shift = 'outbound-night-shift' then cast(night_sub_con_ob_sorting as double)/(outbound_night_shift + hand_4pl_night_shift)  
        else 0 
    end as night_sub_con_ob_sorting
    ,case 
        when outbound_shift = 'outbound-night-shift' then cast(night_sub_con_ob_dispatch as double)/outbound_night_shift 
        else 0 
    end as night_sub_con_ob_dispatch
    ,case 
        when hand_4pl_shift = 'hand4pl-night-shift'  then cast(night_sub_con_4pl as double)/hand_4pl_night_shift 
        else 0 
    end as night_sub_con_4pl
    ,case 
        when pack_manual_shift = 'pack-night-shift' then cast(night_sub_con_manual_sorting_area as double)/pack_manual_night_shift 
        else 0 
    end as night_sub_con_manual_sorting_area
    ,case 
        when return_shift = 'return-night-shift'  then cast(night_sub_con_return as double)/return_night_shift 
        else 0 
    end as night_sub_con_return
    ,case 
        when receive_shift = 'rec-day-shift' or pack_asm_shift = 'pack-night-shift' or pack_manual_shift = 'pack-night-shift' or outbound_shift = 'outbound-night-shift' or return_shift = 'return-night-shift' or hand_4pl_shift = 'hand4pl-night-shift' then cast(night_sub_con_rework as double)/night_shift_order 
        else 0 
    end as night_sub_con_rework
    ,cast(night_sub_con_admin as double)/night_shift_order as admin_night_shift
    ,cast(night_sub_con_tech as double)/night_shift_order as tech_night_shift 

    --- mpw ot
    ,case 
        when receive_shift is not null and order_type != 'WH' then cast(ot_sub_con_inbound as double)/ot_inbound 
        else 0 
    end as ot_sub_con_inbound 
    ,case when receive_shift is not null and order_type = 'WH' then cast(ot_sub_con_wh_onsite as double)/ot_wh 
        else 0 
    end as ot_sub_con_wh_onsite
    ,case when pack_asm_shift is not null then cast(ot_sub_con_cbs as double)/ot_pack_asm 
        else 0 
    end as ot_sub_con_cbs
    ,case 
        when outbound_shift is not null then cast(ot_sub_con_ob_sorting as double)/(ot_ob + ot_hand_4pl)  
        else 0 
    end as ot_sub_con_ob_sorting
    ,case 
        when outbound_shift is not null then cast(ot_sub_con_ob_dispatch as double)/ot_ob 
        else 0 
    end as ot_sub_con_ob_dispatch
    ,case 
        when hand_4pl_shift is not null then cast(ot_sub_con_4pl as double)/ot_hand_4pl 
        else 0 
    end as ot_sub_con_4pl
    ,case 
        when pack_manual_shift is not null then cast(ot_sub_con_manual_sorting_area as double)/ot_pack_manual 
        else 0 
    end as ot_sub_con_manual_sorting_area
    ,case 
        when return_shift is not null then cast(ot_sub_con_return as double)/ot_return 
        else 0 
    end as ot_sub_con_return
    ,case when receive_shift is not null or pack_asm_shift is not null or pack_manual_shift is not null or outbound_shift is not null or return_shift is not null or hand_4pl_shift is not null then cast(ot_sub_con_rework as double)/total_soc_process_order 
        else 0 
    end as ot_sub_con_rework
    ,cast(ot_sub_con_admin as double)/total_soc_process_order as admin_ot_shift
    ,cast(ot_sub_con_tech as double)/total_soc_process_order as tech_ot_shift 

    --- spx_staff
    ,cast(spx_staff_cost as double)/total_soc_process_order as spx_staff_cost
    ,cast(spx_staff_cost_wage as double)/total_soc_process_order as spx_staff_cost_wage
    ,cast(spx_staff_cost_ot as double)/total_soc_process_order as spx_staff_cost_ot
    ,cast(spx_staff_cost_incentive as double)/total_soc_process_order as spx_staff_cost_incentive
    
    --- non headcount 
    ,cast(depreciation as double)/total_soc_process_order as depreciation
    ,cast(rental as double)/total_soc_process_order as rental
    ,cast(g_and_a_cost as double)/total_soc_process_order as g_and_a_cost
    ,cast(lost_and_damage as double)/lost_damage_volume as lost_and_damage
from raw_soc_track
left join thopsbi_lof.spx_soc_cost_ingest  as soc_cost_table
on raw_soc_track.soc_station = soc_cost_table.soc_station
and raw_soc_track.report_month = soc_cost_table.report_month
left join agg_soc_volume_type
on raw_soc_track.soc_station = agg_soc_volume_type.soc_station
and raw_soc_track.report_month = agg_soc_volume_type.report_month
    where raw_soc_track.report_month  is not null 
)
,group_soc_cost as
(
select 
    *
    ,(day_sub_con_inbound + day_sub_con_wh_onsite + day_sub_con_cbs + day_sub_con_ob_sorting + day_sub_con_ob_dispatch + day_sub_con_4pl + day_sub_con_manual_sorting_area + day_sub_con_return + day_sub_con_rework + admin_day_shift + tech_day_shift) as total_subcon_day_shift 
    ,(night_sub_con_inbound + night_sub_con_wh_onsite + night_sub_con_cbs + night_sub_con_ob_sorting + night_sub_con_ob_dispatch + night_sub_con_4pl + night_sub_con_manual_sorting_area + night_sub_con_return + night_sub_con_rework + admin_night_shift + tech_night_shift) as total_subcon_night_shift
    ,(ot_sub_con_inbound + ot_sub_con_wh_onsite + ot_sub_con_cbs + ot_sub_con_ob_sorting + ot_sub_con_ob_dispatch + ot_sub_con_4pl + ot_sub_con_manual_sorting_area + ot_sub_con_return + ot_sub_con_rework + admin_ot_shift + tech_ot_shift) as ot_subcon
    ,(day_sub_con_inbound + night_sub_con_inbound + ot_sub_con_inbound) as total_subcon_inbound_cost 
    ,(day_sub_con_wh_onsite + night_sub_con_wh_onsite + ot_sub_con_wh_onsite) as total_subcon_wh_onsite
    ,(day_sub_con_cbs + night_sub_con_cbs + ot_sub_con_cbs) as total_subcon_cbs 
    ,(day_sub_con_ob_sorting + night_sub_con_ob_sorting + ot_sub_con_ob_sorting) as total_subcon_ob_sorting 
    ,(day_sub_con_ob_dispatch + night_sub_con_ob_dispatch + ot_sub_con_ob_dispatch) as total_subcon_ob_dispatch 
    ,(day_sub_con_4pl + night_sub_con_4pl + ot_sub_con_4pl) as total_subcon_4pl 
    ,(day_sub_con_manual_sorting_area + night_sub_con_manual_sorting_area + ot_sub_con_manual_sorting_area) as total_subcon_manual_sorting_area 
    ,(day_sub_con_return + night_sub_con_return + ot_sub_con_return) as total_subcon_return 
    ,(day_sub_con_rework + night_sub_con_rework + ot_sub_con_rework) as total_subcon_rework 
    ,(admin_day_shift + admin_night_shift + admin_ot_shift) as total_subcon_admin 
    ,(tech_day_shift + tech_night_shift + tech_ot_shift) as total_subcon_tech 
from soc_cost
)
select 
    report_month
    --,order_type
    --,is_4pl
    --- subcon by shift --- 
   /*  ,cast(sum(total_subcon_day_shift) as double)/count(*) as subcon_day_shift_cpo 
    ,cast(sum(total_subcon_night_shift) as double)/count(*) as subcon_night_shift_cpo 
    ,cast(sum(ot_subcon) as double)/count(*) as subcon_ot_cpo  */

     --- subcon by activity ---
    ,cast(sum(total_subcon_inbound_cost) as double)/count(*) as subcon_inbound_cpo
    ,cast(sum(total_subcon_wh_onsite) as double)/count(*) as subcon_wh_onsite_cpo 
    ,cast(sum(total_subcon_cbs) as double)/count(*) as subcon_cbs_cpo 
    ,cast(sum(total_subcon_ob_sorting) as double)/count(*) as subcon_ob_sorting 
    ,cast(sum(total_subcon_ob_dispatch) as double)/count(*) as subcon_ob_dispatch_cpo 
    ,cast(sum(total_subcon_4pl) as double)/count(*) as subcon_4pl_cpo 
    ,cast(sum(total_subcon_manual_sorting_area) as double)/count(*) as subcon_manual_sorting_cpo 
    ,cast(sum(total_subcon_return) as double)/count(*) as subcon_return_cpo 
    ,cast(sum(total_subcon_rework) as double)/count(*) as subcon_rework_cpo 
    ,cast(sum(total_subcon_admin) as double)/count(*) as subcon_admin_cpo 
    ,cast(sum(total_subcon_tech) as double)/count(*) as subcon_tech_cpo 

    ,cast(sum(spx_staff_cost_wage) as double)/count(*) as wage_staff_cpo  
    ,cast(sum(spx_staff_cost_ot) as double)/count(*) as ot_staff_cpo  
    ,cast(sum(spx_staff_cost_incentive) as double)/count(*) as incentive_staff_cpo 

    ,cast(sum(depreciation) as double)/count(*) as depreciation_cpo 
    ,cast(sum(rental) as double)/count(*) as rental_cpo 
    ,cast(sum(g_and_a_cost) as double)/count(*) as g_and_a_cpo 
    ,cast(sum(lost_and_damage) as double)/count(*) as lost_damange_cpo 


from group_soc_cost where soc_station = 'SOCE'
group by 
    report_month
order by 
    report_month desc 



