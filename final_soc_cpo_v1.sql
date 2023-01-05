insert into dev_thopsbi_lof.spx_analytics_cost_soc_cpo_v1
with raw_soc_track as
(
select 
    fleet_order.shipment_id
    ,case  
        when is_bulky = true then 'BK'
        when is_open_service = true then 'OSV'
        when is_marketplace = true then 'MKP'
        when is_cross_border = true then 'CB'
        when is_warehouse = true then 'WH'
    END AS order_type
    ,case 
        when first_soc_station_name is not null then first_soc_station_name
        when return_timestamp is not null then 'SOCE'
    end as soc_station 
    ,case 
        when is_4pl = true then 1 
        else 0 
    end as is_4pl
    ,case 
        when hour(first_soc_received_timestamp) >= 6 and hour(first_soc_received_timestamp) < 14 then 'rec-day-shift'
        when hour(first_soc_received_timestamp) >= 14 or hour(first_soc_received_timestamp) < 6 then 'rec-night-shift'
    end as receive_shift
    ,case 
        when first_soc_packed_operator_name in ('IND000001','CAB000001','DWS000001') and first_soc_packing_timestamp is not null then 
            case
                --when hour(first_soc_packing_timestamp) >= 6 and hour(first_soc_packing_timestamp) < 14  then 'pack-day-shift'
                --when hour(first_soc_packing_timestamp) >= 14 or hour(first_soc_packing_timestamp) < 6 then 'pack-night-shift'
                when first_soc_packing_timestamp is not null then 'pack-night-shift'
            end 
        when first_soc_packed_operator_name not in ('IND000001','CAB000001','DWS000001') then 
            case
            --when hour(first_soc_packed_timestamp) >= 6 and hour(first_soc_packed_timestamp) < 14  then 'pack-day-shift'
            --when hour(first_soc_packed_timestamp) >= 14 or hour(first_soc_packed_timestamp) < 6 then 'pack-night-shift'
                when first_soc_packing_timestamp is not null then 'pack-night-shift'
            end 
    end as pack_asm_shift
    ,case 
        when first_soc_packed_operator_name not in ('IND000001','CAB000001','DWS000001') and first_soc_packing_timestamp is not null then 
            case
            --when hour(first_soc_packing_timestamp) >= 6 and hour(first_soc_packing_timestamp) < 14  then 'pack-day-shift'
            --when hour(first_soc_packing_timestamp) >= 14 or hour(first_soc_packing_timestamp) < 6 then 'pack-night-shift'
                when first_soc_packing_timestamp is not null then 'pack-night-shift'
            end 
        when first_soc_packed_operator_name not in ('IND000001','CAB000001','DWS000001') then 
            case
            --when hour(first_soc_packed_timestamp) >= 6 and hour(first_soc_packed_timestamp) < 14  then 'pack-day-shift'
            --when hour(first_soc_packed_timestamp) >= 14 or hour(first_soc_packed_timestamp) < 6 then 'pack-night-shift'
                when first_soc_packing_timestamp is not null then 'pack-night-shift'
            end 
    end as pack_manual_shift
    ,case 
        when first_soc_lh_packing_timestamp is not null then 
            case 
            --when hour(first_soc_lh_packing_timestamp) >= 6 and hour(first_soc_lh_packing_timestamp) < 14 then 'outbound-day-shift'
            --when hour(first_soc_lh_packing_timestamp) >= 14 or hour(first_soc_lh_packing_timestamp) < 6 then 'outbound-night-shift'
                when first_soc_lh_packed_timestamp is not null then 'outbound-night-shift'
            end 
        when first_soc_lh_packed_timestamp is not null then 
            case 
            --when hour(first_soc_lh_packed_timestamp) >= 6 and hour(first_soc_lh_packed_timestamp) < 14 then 'outbound-day-shift'
            --when hour(first_soc_lh_packed_timestamp) >= 14 or hour(first_soc_lh_packed_timestamp) < 6 then 'outbound-night-shift'
                when first_soc_lh_packed_timestamp is not null then 'outbound-night-shift'
            end 
    end as outbound_shift
    ,case 
        when hour(return_timestamp) >= 6 and hour(return_timestamp) < 14 then 'return-day-shift'
        when hour(return_timestamp) >= 14 or  hour(return_timestamp) < 6 then 'return-night-shift'
    end as return_shift

    ,case 
        when "_4pl_handover_timestamp" is not null and is_4pl = true then 
            case 
                --when hour(_4pl_handover_timestamp) >= 6 and hour(_4pl_handover_timestamp) < 14 then 'hand4pl-day-shift'
                --when hour(_4pl_handover_timestamp) >= 14 or hour(_4pl_handover_timestamp) < 6  then 'hand4pl-night-shift'
                when _4pl_handover_timestamp is not null then 'hand4pl-night-shift'
            end 
        when is_4pl = true then 
            case 
            --when hour(first_soc_packed_timestamp) >= 6 and hour(first_soc_packed_timestamp) < 14 then 'hand4pl-day-shift'
            --when hour(first_soc_packed_timestamp) >= 14 or hour(first_soc_packed_timestamp) < 6 then 'hand4pl-night-shift'
                when first_soc_packed_timestamp is not null then 'hand4pl-night-shift'
            end 
    end as hand_4pl_shift 
    ,case 
        when (lost_timestamp is not null and lost_station_name like 'SOC%') or (damaged_timestamp is not null and damaged_station_name like 'SOC%') then 1 else 0 
    end as lost_damage_soc 
    ,case 
        when date(first_soc_received_timestamp) between date('2022-09-01') and date('2022-09-30') then '2022-09-01'
        when date(return_timestamp) between date('2022-09-01') and date('2022-09-30') then '2022-09-01'
    end as report_month
    ,case 
        when date(first_soc_received_timestamp) between date('2022-09-01') and date('2022-09-30') then date(first_soc_received_timestamp) 
        when date(return_timestamp) between date('2022-09-01') and date('2022-09-30') then date(return_timestamp)
    end as report_date
    ,is_open_service as non_shopee_flag
    ,is_cross_border as cross_border_flag
    ,is_warehouse as warehouse_flag
    ,is_marketplace as marketplace_flag
    ,first_soc_received_timestamp
    ,return_timestamp
from thopsbi_spx.dwd_pub_shipment_info_df_th as fleet_order 
left join 
    (
        SELECT 
            shipment_id
            ,min(case when status = 58 and station_id in (3,242) then from_unixtime(ctime-3600) end ) as return_timestamp
        FROM spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live   
        group by 1 
    ) as return_track  
on fleet_order.shipment_id = return_track.shipment_id
)
,soc_volume_type as
(
select 
    case 
        when date(first_soc_received_timestamp) between date('2022-09-01') and date('2022-09-30') then '2022-09-01'
        when date(return_timestamp) between date('2022-09-01') and date('2022-09-30') then '2022-09-01'
    end as report_month
    ,soc_station
    ,count(*) as total_soc_process_order 
    ,sum(case when receive_shift = 'rec-day-shift' then 1 else 0 end) as inbound_day_shift 
    ,sum(case when receive_shift = 'rec-night-shift' then 1 else 0 end) as inbound_night_shift
 
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
    ,sum(case when lost_damage_soc = 1 then 1 else 0 end) as lost_damage_volume

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
    --,return_day_shift + return_night_shift as ot_return
    ,hand_4pl_day_shift + hand_4pl_night_shift as ot_hand_4pl
from soc_volume_type
)
,soc_cost as
(
select 
    shipment_id
    ,is_4pl
    ,order_type
    ,report_date as soc_report_date
    ,raw_soc_track.report_month as soc_report_month
    ,raw_soc_track.soc_station
    ,receive_shift
    ,pack_asm_shift
    ,pack_manual_shift
    ,hand_4pl_shift
    ,outbound_shift
    ,return_shift
    ,lost_damage_soc
      --- mpw day shift 
    ,try(case when receive_shift = 'rec-day-shift' then cast(day_sub_con_inbound as double)/inbound_day_shift else 0 end) as day_sub_con_inbound
    ,try(case when receive_shift = 'rec-day-shift' and order_type = 'WH'  then cast(day_sub_con_wh_onsite as double)/wh_day_shift else 0 end) as day_sub_con_wh_onsite
    ,try(case when pack_asm_shift = 'pack-day-shift'  then cast(day_sub_con_cbs as double)/pack_asm_day_shift else 0 end) as day_sub_con_cbs
    ,try(case when outbound_shift = 'outbound-day-shift' then cast(day_sub_con_ob_sorting as double)/outbound_day_shift else 0 end) as day_sub_con_ob_sorting
    ,try(case when outbound_shift = 'outbound-day-shift' then cast(day_sub_con_ob_dispatch as double)/outbound_day_shift else 0 end) as day_sub_con_ob_dispatch
    ,try(case when hand_4pl_shift = 'hand4pl-day-shift'  then cast(day_sub_con_4pl as double)/hand_4pl_day_shift else 0 end) as day_sub_con_4pl
    ,try(case when pack_manual_shift = 'pack-day-shift'  then cast(day_sub_con_manual_sorting_area as double)/pack_manual_day_shift else 0 end) as day_sub_con_manual_sorting_area
    --,case when return_shift = 'return-day-shift'  then cast(day_sub_con_return as double)/return_day_shift else 0 end as day_sub_con_return
    ,try(case when receive_shift = 'rec-day-shift' or pack_asm_shift = 'pack-day-shift' or pack_manual_shift = 'pack-day-shift' or outbound_shift = 'outbound-day-shift' or return_shift = 'return-day-shift' or hand_4pl_shift = 'hand4pl-day-shift' then cast(day_sub_con_recovery as double)/day_shift_order else 0 end) as day_sub_con_recovery
    ,try(case when receive_shift = 'rec-day-shift' or pack_asm_shift = 'pack-day-shift' or pack_manual_shift = 'pack-day-shift' or outbound_shift = 'outbound-day-shift' or return_shift = 'return-day-shift' or hand_4pl_shift = 'hand4pl-day-shift' then cast(day_sub_con_admin as double)/day_shift_order else 0 end) as admin_day_shift
    ,try(case when receive_shift = 'rec-day-shift' or pack_asm_shift = 'pack-day-shift' or pack_manual_shift = 'pack-day-shift' or outbound_shift = 'outbound-day-shift' or return_shift = 'return-day-shift' or hand_4pl_shift = 'hand4pl-day-shift' then cast(day_sub_con_tech as double)/day_shift_order else 0 end) as tech_day_shift
    --- mpw night shift      
    ,try(case when receive_shift = 'rec-night-shift' then cast(night_sub_con_inbound as double)/inbound_night_shift else 0 end) as night_sub_con_inbound
    ,try(case when receive_shift = 'rec-night-shift' and order_type = 'WH' then cast(night_sub_con_wh_onsite as double)/wh_night_shift else 0 end) as night_sub_con_wh_onsite
    ,try(case when pack_asm_shift = 'pack-night-shift' then cast(night_sub_con_cbs as double)/pack_asm_night_shift else 0 end) as night_sub_con_cbs
    ,try(case when outbound_shift = 'outbound-night-shift' then cast(night_sub_con_ob_sorting as double)/outbound_night_shift  else 0 end) as night_sub_con_ob_sorting
    ,try(case when outbound_shift = 'outbound-night-shift' then cast(night_sub_con_ob_dispatch as double)/outbound_night_shift else 0 end)  as night_sub_con_ob_dispatch
    ,try(case when hand_4pl_shift = 'hand4pl-night-shift'  then cast(night_sub_con_4pl as double)/hand_4pl_night_shift else 0 end) as night_sub_con_4pl
    ,try(case when pack_manual_shift = 'pack-night-shift'  then cast(night_sub_con_manual_sorting_area as double)/pack_manual_night_shift else 0 end) as night_sub_con_manual_sorting_area
    --,case when return_shift = 'return-night-shift'  then cast(night_sub_con_return as double)/return_night_shift else 0 end as night_sub_con_return
    ,try(case when receive_shift = 'rec-night-shift' or pack_asm_shift = 'pack-night-shift' or pack_manual_shift = 'pack-night-shift' or outbound_shift = 'outbound-night-shift' or return_shift = 'return-night-shift' or hand_4pl_shift = 'hand4pl-night-shift' then cast(night_sub_con_recovery as double)/night_shift_order else 0 end) as night_sub_con_recovery
    ,try(case when receive_shift = 'rec-night-shift' or pack_asm_shift = 'pack-night-shift' or pack_manual_shift = 'pack-night-shift' or outbound_shift = 'outbound-night-shift' or return_shift = 'return-night-shift' or hand_4pl_shift = 'hand4pl-night-shift' then cast(night_sub_con_admin as double)/night_shift_order else 0 end) as admin_night_shift
    ,try(case when receive_shift = 'rec-night-shift' or pack_asm_shift = 'pack-night-shift' or pack_manual_shift = 'pack-night-shift' or outbound_shift = 'outbound-night-shift' or return_shift = 'return-night-shift' or hand_4pl_shift = 'hand4pl-night-shift' then cast(night_sub_con_tech as double)/night_shift_order else 0 end) as tech_night_shift
    --- mpw ot
    ,try(case when receive_shift is not null then cast(ot_sub_con_inbound as double)/ot_inbound else 0 end) as ot_sub_con_inbound 
    ,try(case when receive_shift is not null and order_type = 'WH' then cast(ot_sub_con_wh_onsite as double)/ot_wh else 0 end) as ot_sub_con_wh_onsite
    ,try(case when pack_asm_shift is not null then cast(ot_sub_con_cbs as double)/ot_pack_asm else 0 end) as ot_sub_con_cbs
    ,try(case when outbound_shift is not null then cast(ot_sub_con_ob_sorting as double)/(ot_ob + ot_hand_4pl)  else 0 end) as ot_sub_con_ob_sorting
    ,try(case when outbound_shift is not null then cast(ot_sub_con_ob_dispatch as double)/ot_ob else 0 end) as ot_sub_con_ob_dispatch
    ,try(case when hand_4pl_shift is not null then cast(ot_sub_con_4pl as double)/ot_hand_4pl else 0 end) as ot_sub_con_4pl
    ,try(case when pack_manual_shift is not null then cast(ot_sub_con_manual_sorting_area as double)/ot_pack_manual else 0 end) as ot_sub_con_manual_sorting_area
    --,case when return_shift is not null then cast(ot_sub_con_return as double)/ot_return else 0 end as ot_sub_con_return
    --,case when receive_shift is not null or pack_asm_shift is not null or pack_manual_shift is not null or outbound_shift is not null or return_shift is not null or hand_4pl_shift is not null then cast(ot_sub_con_recovery as double)/total_soc_process_order else 0 end as ot_sub_con_recovery
    ,try(cast(ot_sub_con_recovery as double)/total_soc_process_order) as ot_sub_con_recovery
    ,try(cast(ot_sub_con_admin as double)/total_soc_process_order) as admin_ot_shift
    ,try(cast(ot_sub_con_tech as double)/total_soc_process_order) as tech_ot_shift 

    --- spx_staff
    ,try(cast(spx_staff_cost as double)/total_soc_process_order) as spx_staff_cost
    ,try(cast(spx_staff_cost_wage as double)/total_soc_process_order) as spx_staff_cost_wage
    ,try(cast(spx_staff_cost_ot as double)/total_soc_process_order) as spx_staff_cost_ot
    ,try(cast(spx_staff_cost_incentive as double)/total_soc_process_order) as spx_staff_cost_incentive
    
    --- non headcount 
    ,try(cast(depreciation as double)/total_soc_process_order) as depreciation
    ,try(cast(rental as double)/total_soc_process_order) as rental
    ,try(cast(g_and_a_cost as double)/total_soc_process_order) as g_and_a_cost
    ,try(cast(lost_and_damage as double)/total_soc_process_order) as lost_and_damage

from raw_soc_track
left join thopsbi_lof.spx_soc_cost_ingest  as soc_cost_table
on raw_soc_track.soc_station = soc_cost_table.soc_station
and raw_soc_track.report_month = soc_cost_table.report_month
left join agg_soc_volume_type
on raw_soc_track.soc_station = agg_soc_volume_type.soc_station
and raw_soc_track.report_month = agg_soc_volume_type.report_month
where raw_soc_track.report_month  is not null 
),
group_soc_cost as
(
    select 
        *
        ,(day_sub_con_inbound + day_sub_con_wh_onsite + day_sub_con_cbs + day_sub_con_ob_sorting + day_sub_con_ob_dispatch + day_sub_con_4pl + day_sub_con_manual_sorting_area + day_sub_con_recovery + admin_day_shift + tech_day_shift) as total_subcon_day_shift 
        ,(night_sub_con_inbound + night_sub_con_wh_onsite + night_sub_con_cbs + night_sub_con_ob_sorting + night_sub_con_ob_dispatch + night_sub_con_4pl + night_sub_con_manual_sorting_area + night_sub_con_recovery + admin_night_shift + tech_night_shift) as total_subcon_night_shift
        ,(ot_sub_con_inbound + ot_sub_con_wh_onsite + ot_sub_con_cbs + ot_sub_con_ob_sorting + ot_sub_con_ob_dispatch + ot_sub_con_4pl + ot_sub_con_manual_sorting_area + ot_sub_con_recovery + admin_ot_shift + tech_ot_shift) as total_ot_subcon
        ,(day_sub_con_inbound + night_sub_con_inbound + ot_sub_con_inbound) as total_subcon_inbound_cost 
        ,(day_sub_con_wh_onsite + night_sub_con_wh_onsite + ot_sub_con_wh_onsite) as total_subcon_wh_onsite
        ,(day_sub_con_cbs + night_sub_con_cbs + ot_sub_con_cbs) as total_subcon_cbs 
        ,(day_sub_con_ob_sorting + night_sub_con_ob_sorting + ot_sub_con_ob_sorting) as total_subcon_ob_sorting 
        ,(day_sub_con_ob_dispatch + night_sub_con_ob_dispatch + ot_sub_con_ob_dispatch) as total_subcon_ob_dispatch 
        ,(day_sub_con_4pl + night_sub_con_4pl + ot_sub_con_4pl) as total_subcon_4pl 
        ,(day_sub_con_manual_sorting_area + night_sub_con_manual_sorting_area + ot_sub_con_manual_sorting_area) as total_subcon_manual_sorting_area 
        ,(day_sub_con_recovery + night_sub_con_recovery + ot_sub_con_recovery) as total_subcon_recovery  
        ,(admin_day_shift + admin_night_shift + admin_ot_shift) as total_subcon_admin 
        ,(tech_day_shift + tech_night_shift + tech_ot_shift) as total_subcon_tech 
    from soc_cost
)
,final_group_soc_cost as
(
select 
    shipment_id
    ,is_4pl
    ,order_type
    ,soc_report_month 
    ,soc_report_date
    ,soc_station 
    ,receive_shift
    ,pack_asm_shift
    ,pack_manual_shift
    ,hand_4pl_shift
    ,outbound_shift
    ,return_shift
    ,lost_damage_soc as lost_and_damage_at_soc_flag
    ,if(total_subcon_day_shift is null,0,total_subcon_day_shift) + if(total_subcon_night_shift is null,0,total_subcon_night_shift) + if(total_ot_subcon is null,0,total_ot_subcon) + if(spx_staff_cost is null,0,spx_staff_cost) + if(depreciation is null,0,depreciation) + if(rental is null,0,rental) + if(g_and_a_cost is null,0,g_and_a_cost) + if(lost_and_damage is null,0,lost_and_damage) as total_soc_cost
    ,total_subcon_day_shift 
    ,total_subcon_night_shift 
    ,total_ot_subcon 
    ,total_subcon_inbound_cost 
    ,total_subcon_wh_onsite 
    ,total_subcon_cbs 
    ,total_subcon_ob_sorting 
    ,total_subcon_ob_dispatch 
    ,total_subcon_4pl 
    ,total_subcon_manual_sorting_area 
    ,total_subcon_recovery 
    ,total_subcon_admin 
    ,total_subcon_tech 

     --- mpw day shift 
    ,day_sub_con_inbound
    ,day_sub_con_wh_onsite
    ,day_sub_con_cbs
    ,day_sub_con_ob_sorting
    ,day_sub_con_ob_dispatch
    ,day_sub_con_4pl
    ,day_sub_con_manual_sorting_area
    ,day_sub_con_recovery
    ,admin_day_shift
    ,tech_day_shift 

     --- mpw night shift      
    ,night_sub_con_inbound
    ,night_sub_con_wh_onsite
    ,night_sub_con_cbs
    ,night_sub_con_ob_sorting
    ,night_sub_con_ob_dispatch
    ,night_sub_con_4pl
    ,night_sub_con_manual_sorting_area
    ,night_sub_con_recovery
    ,admin_night_shift
    ,tech_night_shift 

    --- mpw ot
    ,ot_sub_con_inbound 
    ,ot_sub_con_wh_onsite
    ,ot_sub_con_cbs
    ,ot_sub_con_ob_sorting
    ,ot_sub_con_ob_dispatch
    ,ot_sub_con_4pl
    ,ot_sub_con_manual_sorting_area
    ,ot_sub_con_recovery
    ,admin_ot_shift
    ,tech_ot_shift 
    --- spx_staff
    ,spx_staff_cost
    ,spx_staff_cost_wage
    ,spx_staff_cost_ot
    ,spx_staff_cost_incentive
    --- non headcount 
    ,depreciation
    ,rental
    ,g_and_a_cost
    ,lost_and_damage
from group_soc_cost
)
select 
    shipment_id
    ,try(cast(is_4pl as INTEGER))
    ,try(cast(order_type as varchar))
    ,try(cast(soc_report_month as DATE))
    ,try(cast(soc_report_date  as DATE))
    ,try(cast(soc_station  as varchar))
    ,try(cast(receive_shift as varchar))
    ,try(cast(pack_asm_shift as varchar))
    ,try(cast(pack_manual_shift as varchar))
    ,try(cast(hand_4pl_shift as varchar))
    ,try(cast(outbound_shift as varchar))
    ,try(cast(return_shift as varchar))
    ,try(cast(lost_and_damage_at_soc_flag  as INTEGER))
    ,try(cast(total_soc_cost  as decimal(16,7)))
    ,try(cast(total_subcon_day_shift as decimal(16,7)))
    ,try(cast(total_subcon_night_shift as decimal(16,7)))
    ,try(cast(total_ot_subcon as decimal(16,7)))
    ,try(cast(total_subcon_inbound_cost as decimal(16,7)))
    ,try(cast(total_subcon_wh_onsite as decimal(16,7)))
    ,try(cast(total_subcon_cbs as decimal(16,7)))
    ,try(cast(total_subcon_ob_sorting as decimal(16,7)))
    ,try(cast(total_subcon_ob_dispatch as decimal(16,7)))
    ,try(cast(total_subcon_4pl as decimal(16,7)))
    ,try(cast(total_subcon_manual_sorting_area as decimal(16,7)))
    ,try(cast(total_subcon_recovery as decimal(16,7)))
    ,try(cast(total_subcon_admin as decimal(16,7)))
    ,try(cast(total_subcon_tech as decimal(16,7)))

     --- mpw day shift 
    ,try(cast(day_sub_con_inbound as decimal(16,7)))
    ,try(cast(day_sub_con_wh_onsite as decimal(16,7)))
    ,try(cast(day_sub_con_cbs as decimal(16,7)))
    ,try(cast(day_sub_con_ob_sorting as decimal(16,7)))
    ,try(cast(day_sub_con_ob_dispatch as decimal(16,7)))
    ,try(cast(day_sub_con_4pl as decimal(16,7)))
    ,try(cast(day_sub_con_manual_sorting_area as decimal(16,7)))
    ,try(cast(day_sub_con_recovery as decimal(16,7)))
    ,try(cast(admin_day_shift as decimal(16,7)))
    ,try(cast(tech_day_shift  as decimal(16,7)))

    --- mpw night shift      
    ,try(cast(night_sub_con_inbound as decimal(16,7)))
    ,try(cast(night_sub_con_wh_onsite as decimal(16,7)))
    ,try(cast(night_sub_con_cbs as decimal(16,7)))
    ,try(cast(night_sub_con_ob_sorting as decimal(16,7)))
    ,try(cast(night_sub_con_ob_dispatch as decimal(16,7)))
    ,try(cast(night_sub_con_4pl as decimal(16,7)))
    ,try(cast(night_sub_con_manual_sorting_area as decimal(16,7)))
    ,try(cast(night_sub_con_recovery as decimal(16,7)))
    ,try(cast(admin_night_shift as decimal(16,7)))
    ,try(cast(tech_night_shift  as decimal(16,7)))

    --- mpw ot
    ,try(cast(ot_sub_con_inbound  as decimal(16,7)))
    ,try(cast(ot_sub_con_wh_onsite as decimal(16,7)))
    ,try(cast(ot_sub_con_cbs as decimal(16,7)))
    ,try(cast(ot_sub_con_ob_sorting as decimal(16,7)))
    ,try(cast(ot_sub_con_ob_dispatch as decimal(16,7)))
    ,try(cast(ot_sub_con_4pl as decimal(16,7)))
    ,try(cast(ot_sub_con_manual_sorting_area as decimal(16,7)))
    ,try(cast(ot_sub_con_recovery as decimal(16,7)))
    ,try(cast(admin_ot_shift as decimal(16,7)))
    ,try(cast(tech_ot_shift  as decimal(16,7)))

    --- spx_staff
    ,try(cast(spx_staff_cost as decimal(16,7)))
    ,try(cast(spx_staff_cost_wage as decimal(16,7)))
    ,try(cast(spx_staff_cost_ot as decimal(16,7)))
    ,try(cast(spx_staff_cost_incentive as decimal(16,7)))
    
    --- non headcount 
    ,try(cast(depreciation as decimal(16,7)))
    ,try(cast(rental as decimal(16,7)))
    ,try(cast(g_and_a_cost as decimal(16,7)))
    ,try(cast(lost_and_damage as decimal(16,7)))
    ,CAST(CURRENT_TIMESTAMP + INTERVAL '-1' HOUR AS TIMESTAMP) AS ingestion_timestamp
    ,date('2022-09-01') AS partition_date

from final_group_soc_cost 

 
    --DROP TABLE IF EXISTS dev_thopsbi_lof.spx_analytics_cost_soc_cpo_v1
   /*  create table dev_thopsbi_lof.spx_analytics_cost_soc_cpo_v1
    (
    shipment_id varchar 
    ,is_4pl  INTEGER
    ,order_type  varchar
    ,soc_report_month  DATE
    ,soc_report_date   DATE
    ,soc_station   varchar
    ,receive_shift  varchar
    ,pack_asm_shift  varchar
    ,pack_manual_shift  varchar
    ,hand_4pl_shift  varchar
    ,outbound_shift  varchar
    ,return_shift  varchar
    ,lost_and_damage_at_soc_flag   INTEGER
    ,total_soc_cost   decimal(16,7)
    ,total_subcon_day_shift  decimal(16,7)
    ,total_subcon_night_shift  decimal(16,7)
    ,total_ot_subcon  decimal(16,7)
    ,total_subcon_inbound_cost  decimal(16,7)
    ,total_subcon_wh_onsite  decimal(16,7)
    ,total_subcon_cbs  decimal(16,7)
    ,total_subcon_ob_sorting  decimal(16,7)
    ,total_subcon_ob_dispatch  decimal(16,7)
    ,total_subcon_4pl  decimal(16,7)
    ,total_subcon_manual_sorting_area  decimal(16,7)
    ,total_subcon_recovery  decimal(16,7)
    ,total_subcon_admin  decimal(16,7)
    ,total_subcon_tech  decimal(16,7)

     --- mpw day shift 
      ,day_sub_con_inbound  decimal(16,7)
      ,day_sub_con_wh_onsite  decimal(16,7)
      ,day_sub_con_cbs  decimal(16,7)
      ,day_sub_con_ob_sorting  decimal(16,7)
      ,day_sub_con_ob_dispatch  decimal(16,7)
      ,day_sub_con_4pl  decimal(16,7)
      ,day_sub_con_manual_sorting_area  decimal(16,7)
      ,day_sub_con_recovery  decimal(16,7)
      ,admin_day_shift  decimal(16,7)
      ,tech_day_shift   decimal(16,7)

     --- mpw night shift      
      ,night_sub_con_inbound  decimal(16,7)
      ,night_sub_con_wh_onsite  decimal(16,7)
      ,night_sub_con_cbs  decimal(16,7)
      ,night_sub_con_ob_sorting  decimal(16,7)
      ,night_sub_con_ob_dispatch  decimal(16,7)
      ,night_sub_con_4pl  decimal(16,7)
      ,night_sub_con_manual_sorting_area  decimal(16,7)
      ,night_sub_con_recovery  decimal(16,7)
      ,admin_night_shift  decimal(16,7)
      ,tech_night_shift   decimal(16,7)

    --- mpw ot
      ,ot_sub_con_inbound   decimal(16,7)
      ,ot_sub_con_wh_onsite  decimal(16,7)
      ,ot_sub_con_cbs  decimal(16,7)
      ,ot_sub_con_ob_sorting  decimal(16,7)
      ,ot_sub_con_ob_dispatch  decimal(16,7)
      ,ot_sub_con_4pl  decimal(16,7)
      ,ot_sub_con_manual_sorting_area  decimal(16,7)
      ,ot_sub_con_recovery  decimal(16,7)
      ,admin_ot_shift  decimal(16,7)
      ,tech_ot_shift   decimal(16,7)

    --- spx_staff
     ,spx_staff_cost  decimal(16,7)
     ,spx_staff_cost_wage  decimal(16,7)
     ,spx_staff_cost_ot  decimal(16,7)
     ,spx_staff_cost_incentive  decimal(16,7)
    
    --- non headcount 
     ,depreciation  decimal(16,7)
     ,rental  decimal(16,7)
     ,g_and_a_cost  decimal(16,7)
     ,lost_and_damage decimal(16,7)

    ,ingestion_timestamp TIMESTAMP
    ,partition_date DATE     
     )
   WITH
   (
      FORMAT = 'Parquet',
      PARTITIONED_BY = array['partition_date']
   );
 */


-----------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------

/* delete from dev_thopsbi_lof.spx_analytics_cost_soc_cpo_v1
where partition_date = date('2022-07-01') */


-----------------------------------------------------------------------------------------------------





/* select 
    soc_report_month
    --,soc_station
    ,sum(total_soc_cost) as total_soc_cost
    ,sum(total_subcon_day_shift + total_subcon_night_shift + total_ot_subcon) as total_subcon_cost 
    ,sum(spx_staff_cost_wage + spx_staff_cost_incentive + spx_staff_cost_ot) as total_staff_cost   
    ,sum(spx_staff_cost) as spx_staff_cost  
    ,sum(depreciation) as depreciation_cost 
    ,sum(rental) as rental_cost
    ,sum(g_and_a_cost) as g_and_a_cost  
    ,sum(lost_and_damage) as lost_damange_cost  




from dev_thopsbi_lof.spx_analytics_cost_soc_cpo_v1 
where  date(soc_report_month) = date('2022-07-01') 
group by 1
order by 2

 */










/* 
select 
    soc_report_month
    ,soc_station
    ,sum(total_soc_cost) as total_soc_cost
    ,sum(total_subcon_day_shift) as total_subcon_day_shift
    ,sum(total_subcon_night_shift) as total_subcon_night_shift
    ,sum(total_ot_subcon) as total_ot_subcon
    ,sum(spx_staff_cost_incentive) as spx_staff_cost_incentive
    ,sum(depreciation) as depreciation
    ,sum(rental) as rental
    ,sum(g_and_a_cost) as g_and_a_cost
    ,sum(lost_and_damage) as lost_and_damage
    --,is_4pl
    ,sum(total_soc_cost) as total_soc_cost 
    ,cast(sum(total_soc_cost) as double)/count(*) as total_soc_cpo
    --- subcon by shift --- 
    ,cast(sum(total_subcon_day_shift) as double)/count(*) as subcon_day_shift_cpo 
    ,cast(sum(total_subcon_night_shift) as double)/count(*) as subcon_night_shift_cpo 
    ,cast(sum(total_ot_subcon) as double)/count(*) as subcon_ot_cpo  

     --- subcon by activity ---
    ,cast(sum(total_subcon_inbound_cost) as double)/count(*) as subcon_inbound_cpo
    ,cast(sum(total_subcon_wh_onsite) as double)/count(*) as subcon_wh_onsite_cpo 
    ,cast(sum(total_subcon_cbs) as double)/count(*) as subcon_cbs_cpo 
    ,cast(sum(total_subcon_ob_sorting) as double)/count(*) as subcon_ob_sorting 
    ,cast(sum(total_subcon_ob_dispatch) as double)/count(*) as subcon_ob_dispatch_cpo 
    ,cast(sum(total_subcon_4pl) as double)/count(*) as subcon_4pl_cpo 
    ,cast(sum(total_subcon_manual_sorting_area) as double)/count(*) as subcon_manual_sorting_cpo 
    ,cast(sum(total_subcon_recovery) as double)/count(*) as subcon_recovery_cpo 
    ,cast(sum(total_subcon_admin) as double)/count(*) as subcon_admin_cpo 
    ,cast(sum(total_subcon_tech) as double)/count(*) as subcon_tech_cpo 

    ,cast(sum(spx_staff_cost_wage) as double)/count(*) as wage_staff_cpo  
    ,cast(sum(spx_staff_cost_ot) as double)/count(*) as ot_staff_cpo  
    ,cast(sum(spx_staff_cost_incentive) as double)/count(*) as incentive_staff_cpo 

    ,cast(sum(depreciation) as double)/count(*) as depreciation_cpo 
    ,cast(sum(rental) as double)/count(*) as rental_cpo 
    ,cast(sum(g_and_a_cost) as double)/count(*) as g_and_a_cpo 
    ,cast(sum(lost_and_damage) as double)/count(*) as lost_damange_cpo 

    
    
    --- subcon cost activity
    ,sum(total_subcon_inbound_cost) as total_inbound_cost   
    ,sum(total_subcon_wh_onsite) as total_subcon_wh_onsite
    ,sum(total_subcon_cbs) as total_subcon_cbs
    ,sum(total_subcon_ob_sorting) as total_subcon_ob_sorting
    ,sum(total_subcon_ob_dispatch) as total_subcon_ob_dispatch
    ,sum(total_subcon_4pl) as total_subcon_4pl
    ,sum(total_subcon_manual_sorting_area) as total_subcon_manual_sorting_area
    ,sum(total_subcon_recovery) as total_subcon_recovery
    ,sum(total_subcon_admin) as total_subcon_admin
    ,sum(total_subcon_tech) as total_subcon_tech

    --- subcon cost by shift 
    ,sum(total_subcon_day_shift) as total_subcon_day_shift
    ,sum(total_subcon_night_shift) as total_subcon_night_shift
    ,sum(total_ot_subcon) as total_ot_subcon

    --- volume 
    ,count(*) as total_process_soc_volume 
    ,sum(total_soc_cost) as total_soc_cost
    ,sum(case when receive_shift = 'rec-night-shift' then 1 else 0 end) as night_sub_con_inbound_volume 
    ,sum(case when receive_shift = 'rec-night-shift' and order_type = 'WH' then 1 else 0 end) as night_sub_con_wh_onsite_volume 
    ,sum(case when pack_asm_shift = 'pack-night-shift' then 1 else 0 end) as night_sub_con_cbs
    ,sum(case when outbound_shift = 'outbound-night-shift' then 1 else 0 end) as night_sub_con_ob_sorting_volume
    ,sum(case when outbound_shift = 'outbound-night-shift' then 1 else 0 end) as night_sub_con_ob_dispatch
    ,sum(case when hand_4pl_shift = 'hand4pl-night-shift'  then 1 else 0 end) as night_sub_con_4pl
    ,sum(case when pack_manual_shift = 'pack-night-shift'  then 1 else 0 end) as night_sub_con_manual_sorting_area

    ,sum(case when receive_shift = 'rec-day-shift' then 1 else 0 end) as day_sub_con_inbound_volume 
    ,sum(case when receive_shift = 'rec-day-shift' and order_type = 'WH' then 1 else 0 end) as day_sub_con_wh_onsite_volume 
    ,sum(case when pack_asm_shift = 'pack-day-shift' then 1 else 0 end) as day_sub_con_cbs
    ,sum(case when outbound_shift = 'outbound-day-shift' then 1 else 0 end) as day_sub_con_ob_sorting_volume
    ,sum(case when outbound_shift = 'outbound-day-shift' then 1 else 0 end) as day_sub_con_ob_dispatch
    ,sum(case when hand_4pl_shift = 'hand4pl-day-shift'  then 1 else 0 end) as day_sub_con_4pl
    ,sum(case when pack_manual_shift = 'pack-day-shift'  then 1 else 0 end) as day_sub_con_manual_sorting_area

    


from dev_thopsbi_lof.spx_analytics_cost_soc_cpo_v1 
where  soc_report_month = date('2022-06-01') --and soc_station = 'SOCE'
group by 1,2
order by 1 desc,2 */

/*
 select 
sum(total_soc_cost) as total_soc_cost
,count(*) as total_soc_volume 
,sum(case when soc_report_month = date('2022-01-01') then 1 else 0 end) as total_soc_volume_overview
from dev_thopsbi_lof.spx_analytics_cost_soc_cpo_v1 
where soc_report_month = date('2022-01-01')
 */











 










