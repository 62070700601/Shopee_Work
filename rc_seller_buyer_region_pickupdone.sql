/*
universe fmhub_pickup_done 
30 วัน ยึดตาม status ctime ของ fmhub_pickup_done_date
rc_pickup_station 
ดึง station id map with station name ออกมาแมพกะลิ้ง https://docs.google.com/spreadsheets/d/1mS0qXIEDjiFoZh8wQfbroMqznULzZnIJoY4i92ef4ks/edit#gid=722060761
เปลี่ยนจาก HXXX - เป็น FXXX 
ทำมาอีก 1 คอลัมเป็น rc_pickup_station ถ้า pickup station ตรงกะอันนี้ให้ขึ้น NORC-A/NORC-B/SORC-A/SORC-B/CERC/NERC-A/NERC-B ถ้าไม่ตรงเป็นใส่ค่า else = SOC

destination hub name 
แมพกะลิ้ง https://docs.google.com/spreadsheets/d/1mS0qXIEDjiFoZh8wQfbroMqznULzZnIJoY4i92ef4ks/edit#gid=722060761
ทำมาอีก 1 คอลัมเป็น rc_delivery_station ว่า ถ้า destination ตรงกะอันนี้ให้ขึ้น NORC-A/NORC-B/SORC-A/SORC-B/CERC/NERC-A/NERC-B ถ้าไม่ตรงเป็นใส่ค่า else = ตามโซนของพัสดุว่าเป็น GBKK หรือ UPC

เมื่อเราแมพทุกอย่างแล้ว ควรจะมี
- pickup_date
- pickup station
- rc_pickup_station 
- destination hub
- rc_delivery_station 
เราสามารถเอามาทำเพิ่ม 1 คอลัมได้มั้ย เป็น order path เราโดยการจับ rc_pickup_station > rc_delivery_station 
เช่น 
SORC-A > NERC-B
SOC > NORC-A
CERC > UPC
SORC-B > GBKK
*/
with cte as 
(
select
    fleet_order.shipment_id
    -- ,order_track.pickup_station_id
    -- ,date(FROM_UNIXTIME(fleet_order.pickup_time-3600)) as pickup_date
    ,COALESCE(date_FMHub_Pickup_Done,date_FMHub_Pickup_Handedover) as pickup_date
    ,staion_table_name.station_name as pickup_station_name   
    ,split_part(staion_table_name.station_name,' ',1) as pickup_station_name_split
    -- rc_pickup_station
    ,case
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FPHIT-A','FPHIT-B','FWTNG','FKPET','FTAKK','FMSOD','FNANN','FPJIT','FPBUN','FLOMS','FPRAE','FLOEI','FTHAI','FSWKL','FUTTA') then 'NORC-A'
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FSRPI','FCMAI-A','FCMAI-B','FSSAI','FMRIM','FDSKT','FCDAO','FFRNG','FSTNG','FDONG','FSANK','FPAAN','FCRAI','FMSAI','FMJUN','FPYAO','FLPNG','FLPUN') then 'NORC-B'
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FKRAT-A','FKRAT-B','FKRAT-C','FNSUG','FCOCH','FPAKC','FPIMY','FBUAY','FSKIU','FDKTD','FPTCI','FKNBR','FSNEN','FPHUK','FCYPM','FBRAM','FLPMT','FSTUK','FNRNG','FPKCI','FYASO','FSSKT','FSRIN','FSKPM','FPSAT','FUBON-A','FUBON-B','FWRIN','FDUDM') then 'NERC-A'
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FKKAN-B','FKKAN-A','FBPAI','FCPAE','FKLSN','FYTAD','FNKPN','FTPNM','FMKAM','FKSPS','FKTWC','FMDHN','FROET','FSKON','FNKAI','FPSAI','FUDON-A','FUDON-B') then 'NERC-B' 
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FPPIN','FSMUI','FSRAT','FKDIT','FBNSN','FKRBI','FCPON','FPTIL','FSAWE','FTSNG','FCOUD','FNKSI','FTYAI','FTSLA','FSICN','FTLNG','FPHKT-A','FPHKT-B','FRNNG') then 'SORC-A' 
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FHYAI-B','FHYAI-A','FSKLA','FSDAO','FTANG','FNARA','FPTNI','FKGPO','FMYOR','FYLNG','FPATL','FKUKN','FYALA','FRMAN','FSTUN') then 'SORC-B' 
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FSWAN','FTAKI','FBPIN','FAYUT','FSENA','FAUTH','FWNOI','FLOPB','FKSRG','FCBDN','FPTNK','FKKOI','FSRBR','FBAMO','FPTBT','FPTBT','FNKAE','FSING','FTONG') then 'CERC'
        else pub_shipment.seller_region_name
    end as rc_pickup_station
    ,staion_table_name2.station_name as destination_name
    ,split_part(staion_table_name2.station_name,' ',1) as destination_name_split
    ,case
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') then 'NORC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') then 'NORC-B'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') then 'NERC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') then 'NERC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') then 'SORC-A' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') then 'SORC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') then 'CERC'
        else pub_shipment.buyer_region_name
    end as rc_delivery_station
from spx_mart.shopee_fleet_order_th_db__fleet_order_tab__reg_continuous_s0_live as fleet_order
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name
on fleet_order.pickup_station_id = staion_table_name.id
left join spx_mart.shopee_fms_th_db__station_tab__th_daily_s0_live as staion_table_name2
on fleet_order.station_id = staion_table_name2.id
left join thopsbi_spx.dwd_pub_shipment_info_df_th as pub_shipment
on fleet_order.shipment_id = pub_shipment.shipment_id
left join 
    (
        select 
            shipment_id
            ,min(if(status = 39,date(FROM_UNIXTIME(ctime-3600)),null)) as date_FMHub_Pickup_Done
            ,min(if(status = 40,date(FROM_UNIXTIME(ctime-3600)),null)) as date_FMHub_Pickup_Handedover
        from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live 
        group by 
            shipment_id
    ) as order_track
on fleet_order.shipment_id = order_track.shipment_id
where    
    -- COALESCE(date_FMHub_Pickup_Done,date_FMHub_Pickup_Handedover) between date(DATE_TRUNC('day', current_timestamp) - interval '41' day) and date(DATE_TRUNC('day', current_timestamp) )
    COALESCE(date_FMHub_Pickup_Done,date_FMHub_Pickup_Handedover) between date('2022-10-12') and date('2022-11-11')
    -- and pub_shipment.is_4pl = false
    -- fleet_order.shipment_id not like 'TH%'
    -- fleet_order.shipment_id = 'SPXTH025964952574'
group by
        fleet_order.shipment_id
    ,COALESCE(date_FMHub_Pickup_Done,date_FMHub_Pickup_Handedover) 
    ,staion_table_name.station_name
    ,split_part(staion_table_name.station_name,' ',1) 
    ,case
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FPHIT-A','FPHIT-B','FWTNG','FKPET','FTAKK','FMSOD','FNANN','FPJIT','FPBUN','FLOMS','FPRAE','FLOEI','FTHAI','FSWKL','FUTTA') then 'NORC-A'
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FSRPI','FCMAI-A','FCMAI-B','FSSAI','FMRIM','FDSKT','FCDAO','FFRNG','FSTNG','FDONG','FSANK','FPAAN','FCRAI','FMSAI','FMJUN','FPYAO','FLPNG','FLPUN') then 'NORC-B'
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FKRAT-A','FKRAT-B','FKRAT-C','FNSUG','FCOCH','FPAKC','FPIMY','FBUAY','FSKIU','FDKTD','FPTCI','FKNBR','FSNEN','FPHUK','FCYPM','FBRAM','FLPMT','FSTUK','FNRNG','FPKCI','FYASO','FSSKT','FSRIN','FSKPM','FPSAT','FUBON-A','FUBON-B','FWRIN','FDUDM') then 'NERC-A'
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FKKAN-B','FKKAN-A','FBPAI','FCPAE','FKLSN','FYTAD','FNKPN','FTPNM','FMKAM','FKSPS','FKTWC','FMDHN','FROET','FSKON','FNKAI','FPSAI','FUDON-A','FUDON-B') then 'NERC-B' 
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FPPIN','FSMUI','FSRAT','FKDIT','FBNSN','FKRBI','FCPON','FPTIL','FSAWE','FTSNG','FCOUD','FNKSI','FTYAI','FTSLA','FSICN','FTLNG','FPHKT-A','FPHKT-B','FRNNG') then 'SORC-A' 
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FHYAI-B','FHYAI-A','FSKLA','FSDAO','FTANG','FNARA','FPTNI','FKGPO','FMYOR','FYLNG','FPATL','FKUKN','FYALA','FRMAN','FSTUN') then 'SORC-B' 
        when split_part(staion_table_name.station_name,' ',1) = any (values 'FSWAN','FTAKI','FBPIN','FAYUT','FSENA','FAUTH','FWNOI','FLOPB','FKSRG','FCBDN','FPTNK','FKKOI','FSRBR','FBAMO','FPTBT','FPTBT','FNKAE','FSING','FTONG') then 'CERC'
        else pub_shipment.seller_region_name
    end
    ,staion_table_name2.station_name
    ,split_part(staion_table_name2.station_name,' ',1)
    ,case
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPHIT-A','HPHIT-B','HWTNG','HKPET','HTAKK','HMSOD','HNANN','HPJIT','HPBUN','HLOMS','HPRAE','HLOEI','HTHAI','HSWKL','HUTTA') then 'NORC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSRPI','HCMAI-A','HCMAI-B','HSSAI','HMRIM','HDSKT','HCDAO','HFRNG','HSTNG','HDONG','HSANK','HPAAN','HCRAI','HMSAI','HMJUN','HPYAO','HLPNG','HLPUN') then 'NORC-B'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKRAT-A','HKRAT-B','HKRAT-C','HNSUG','HCOCH','HPAKC','HPIMY','HBUAY','HSKIU','HDKTD','HPTCI','HKNBR','HSNEN','HPHUK','HCYPM','HBRAM','HLPMT','HSTUK','HNRNG','HPKCI','HYASO','HSSKT','HSRIN','HSKPM','HPSAT','HUBON-A','HUBON-B','HWRIN','HDUDM') then 'NERC-A'
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HKKAN-B','HKKAN-A','HBPAI','HCPAE','HKLSN','HYTAD','HNKPN','HTPNM','HMKAM','HKSPS','HKTWC','HMDHN','HROET','HSKON','HNKAI','HPSAI','HUDON-A','HUDON-B') then 'NERC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HPPIN','HSMUI','HSRAT','HKDIT','HBNSN','HKRBI','HCPON','HPTIL','HSAWE','HTSNG','HCOUD','HNKSI','HTYAI','HTSLA','HSICN','HTLNG','HPHKT-A','HPHKT-B','HRNNG') then 'SORC-A' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HHYAI-B','HHYAI-A','HSKLA','HSDAO','HTANG','HNARA','HPTNI','HKGPO','HMYOR','HYLNG','HPATL','HKUKN','HYALA','HRMAN','HSTUN') then 'SORC-B' 
        when split_part(staion_table_name2.station_name,' ',1) = any (values 'HSWAN','HTAKI','HBPIN','HAYUT','HSENA','HAUTH','HWNOI','HLOPB','HKSRG','HCBDN','HPTNK','HKKOI','HSRBR','HBAMO','HPTBT','HNKAE','HSING','HTONG') then 'CERC'
        else pub_shipment.buyer_region_name
    end
)
select 
    -- shipment_id
    pickup_date
    -- ,pickup_station_name   
    -- ,pickup_station_name_split
    -- ,rc_pickup_station
    -- ,destination_name
    -- ,destination_name_split
    -- ,rc_delivery_station
    ,CONCAT(rc_pickup_station, ' > ',rc_delivery_station) as path
    ,count(CONCAT(rc_pickup_station, ' > ',rc_delivery_station)) as count_path
from cte
where 
    pickup_date is not null 
    and CONCAT(rc_pickup_station, ' > ',rc_delivery_station) is not null 
group by
    pickup_date
    ,CONCAT(rc_pickup_station, ' > ',rc_delivery_station)
order by
    pickup_date asc
    ,CONCAT(rc_pickup_station, ' > ',rc_delivery_station) asc