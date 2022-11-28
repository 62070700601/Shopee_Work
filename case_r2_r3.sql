select 
    date(exceptional_time) as SLA_date
    ,fleet_order.shipment_id
    -- ,fleet_order .latest_soc_received_timestamp
    ,latest_sc_time.status_time as latest_soc_received_timestamp
    ,case when exceptional_time is not null and fleet_order.is_4pl = true then 'r3'
          when  exceptional_time is not null then 'r2'
          end as r2_r3_type 
    ,wrong_destination as Wrong_hub_station
    ,coalesce(latest_awb_station_name,'Flash') as Correct_hub_station
    ,case when split_part(wrong_destination,' ',1) = any (values  split_part('HBBON - บางบอน',' ',1)
                                                ,split_part('HBGNA-A - บางนา-A',' ',1)
                                                ,split_part('HBGNA-B - บางนา-B',' ',1)
                                                ,split_part('HBKAE-A - บางแค-A',' ',1)
                                                ,split_part('HBKAE-B - บางแค-B',' ',1)
                                                ,split_part('HBKEN-A - บางเขน-A',' ',1)
                                                ,split_part('HBKEN-B - บางเขน-B',' ',1)
                                                ,split_part('HBKPI-A - บางกะปิ-A',' ',1)
                                                ,split_part('HBKPI-B - บางกะปิ-B',' ',1)
                                                ,split_part('HBKTH-A - บางขุนเทียน-A',' ',1)
                                                ,split_part('HBKTH-B - เขตบางขุนเทียน-B',' ',1)
                                                ,split_part('HBKUM - บึงกุ่ม',' ',1)
                                                ,split_part('HBPAT - บางพลัด',' ',1)
                                                ,split_part('HBSUE - บางซื่อ',' ',1)
                                                ,split_part('HDDNG - ดินแดง',' ',1)
                                                ,split_part('HDONM - ดอนเมือง',' ',1)
                                                ,split_part('HHKWG-A - ห้วยขวาง-A',' ',1)
                                                ,split_part('HHKWG-B - ห้วยขวาง-B',' ',1)
                                                ,split_part('HJJAK-A - จตุจักร-A',' ',1)
                                                ,split_part('HJJAK-B - จตุจักร-B',' ',1)
                                                ,split_part('HJJAK-C - เขตจตุุจักร-C',' ',1)
                                                ,split_part('HJOMT-A - จอมทอง-A',' ',1)
                                                ,split_part('HJOMT-B - จอมทอง-B',' ',1)
                                                ,split_part('HKSAN - เขตคลองสาน',' ',1)
                                                ,split_part('HKSWA-A - คลองสามวา-A',' ',1)
                                                ,split_part('HKSWA-B - คลองสามวา-B',' ',1)
                                                ,split_part('HKYAO - คันนายาว',' ',1)
                                                ,split_part('HLAEM - บางคอแหลม',' ',1)
                                                ,split_part('HLANG-A - วังทองหลาง-A',' ',1)
                                                ,split_part('HLANG-B - เขตวังทองหลาง B',' ',1)
                                                ,split_part('HLDKB - ลาดกระบัง',' ',1)
                                                ,split_part('HLKSI - หลักสี่',' ',1)
                                                ,split_part('HMBRI - มีนบุรี',' ',1)
                                                ,split_part('HNJOK-A - หนองจอก-A',' ',1)
                                                ,split_part('HNJOK-B - หนองจอก-B',' ',1)
                                                ,split_part('HNKAM - หนองแขม',' ',1)
                                                ,split_part('HPASI-A - ภาษีเจริญ-A',' ',1)
                                                ,split_part('HPASI-B - ภาษีเจริญ-B',' ',1)
                                                ,split_part('HPKNG - เขตพระโขนง',' ',1)
                                                ,split_part('HPRAO-A - ลาดพร้าว-A',' ',1)
                                                ,split_part('HPRAO-B - ลาดพร้าว-B',' ',1)
                                                ,split_part('HPRAP - ป้อมปราบศัตรูพ่าย',' ',1)
                                                ,split_part('HPWAN - เขตปทุมวัน',' ',1)
                                                ,split_part('HPWET-A - ประเวศ-A',' ',1)
                                                ,split_part('HPWET-B - ประเวศ-B',' ',1)
                                                ,split_part('HPYTH-A - พญาไท-A',' ',1)
                                                ,split_part('HPYTH-B - พญาไท-B',' ',1)
                                                ,split_part('HRBRN - ราษฎร์บูรณะ',' ',1)
                                                ,split_part('HRCTW - ราชเทวี',' ',1)
                                                ,split_part('HSAMP - สัมพันธวงศ์',' ',1)
                                                ,split_part('HSAPA - สะพานสูง',' ',1)
                                                ,split_part('HSATH - สาทร',' ',1)
                                                ,split_part('HBRAK - บางรัก',' ',1)
                                                ,split_part('HSLNG-A - สวนหลวง-A',' ',1)
                                                ,split_part('HSLNG-B - เขตสวนหลวง-B',' ',1)
                                                ,split_part('HSMAI-A - สายไหม-A',' ',1)
                                                ,split_part('HSMAI-B - สายไหม-B',' ',1)
                                                ,split_part('HTAWI - ทวีวัฒนา',' ',1)
                                                ,split_part('HTHON - ธนบุรี',' ',1)
                                                ,split_part('HBKKY - บางกอกใหญ่',' ',1)
                                                ,split_part('HTLCH - ตลิ่งชัน',' ',1)
                                                ,split_part('HTOEI - คลองเตย',' ',1)
                                                ,split_part('HWTNA - วัฒนา',' ',1)
                                                ,split_part('HYNWA - ยานนาวา',' ',1)
                                                ,split_part('HBBTG-A - บางบัวทอง',' ',1)
                                                ,split_part('HBBTG-B - บางบัวทอง-B',' ',1)
                                                ,split_part('HBYAI - บางใหญ่',' ',1)
                                                ,split_part('HGUAI - อำเภอบางกรวย',' ',1)
                                                ,split_part('HKRET-A - ปากเกร็ด-A',' ',1)
                                                ,split_part('HKRET-B - ปากเกร็ด-B',' ',1)
                                                ,split_part('HKRET-C - ปากเกร็ด-C',' ',1)
                                                ,split_part('HNONT-A - นนทบุรี-A',' ',1)
                                                ,split_part('HNONT-B - นนทบุรี-B',' ',1)
                                                ,split_part('HNONT-C - นนทบุรี-C',' ',1)
                                                ,split_part('HKLNG-A - คลองหลวง-A',' ',1)
                                                ,split_part('HKLNG-B - คลองหลวง-B',' ',1)
                                                ,split_part('HKLNG-C - คลองหลวง-C',' ',1)
                                                ,split_part('HLDLK - ลาดหลุมแก้ว',' ',1)
                                                ,split_part('HLUKA-A - ลำลูกกา-A',' ',1)
                                                ,split_part('HLUKA-B - ลำลูกกา-B',' ',1)
                                                ,split_part('HPTUM-A - ปทุมธานี-A',' ',1)
                                                ,split_part('HPTUM-B - ปทุมธานี-B',' ',1)
                                                ,split_part('HTYBR-A - ธัญบุรี-A',' ',1)
                                                ,split_part('HTYBR-B - ธัญบุรี-B',' ',1)
                                                ,split_part('HBGBO - บางบ่อ',' ',1)
                                                ,split_part('HBPLI-A - บางพลี-A',' ',1)
                                                ,split_part('HBPLI-B - บางพลี-B',' ',1)
                                                ,split_part('HBPLI-C - บางพลี-C',' ',1)
                                                ,split_part('HBSAO - บางเสาธง',' ',1)
                                                ,split_part('HPAPD - พระประแดง',' ',1)
                                                ,split_part('HSMJD - อำเภอพระสมุทรเจดีย์',' ',1)
                                                ,split_part('HSMPK-A - สมุทรปราการ-A',' ',1)
                                                ,split_part('HSMPK-B - สมุทรปราการ-B',' ',1)
                                                ,split_part('HSMPK-C - สมุทรปราการ-C',' ',1)
                                                ,split_part('HSAKN-A - สมุุทรสาคร-A',' ',1)
                                                ,split_part('HSAKN-B - สมุุทรสาคร-B',' ',1)
                                                ,split_part('HBPAW - อำเภอบ้านแพ้ว',' ',1)
                                                ,split_part('HKTBN - อำเภอกระทุ่มแบน',' ',1)
                                        ) then 'GBKK' 
                                        when wrong_destination = any (values 
                                                'Kerry'
                                                ,'4PL-Kerry (non-int)'
                                                ,'4PL-Kerry (R3)'
                                                ,'Flash'
                                                ,'Ninja van' 
                                        ) then '4PL'
                                        ELSE 'UPC' END AS Wrong_hub_zone

,case when split_part(coalesce(latest_awb_station_name,'Flash'),' ',1) = any (values  split_part('HBBON - บางบอน',' ',1)
                                                ,split_part('HBGNA-A - บางนา-A',' ',1)
                                                ,split_part('HBGNA-B - บางนา-B',' ',1)
                                                ,split_part('HBKAE-A - บางแค-A',' ',1)
                                                ,split_part('HBKAE-B - บางแค-B',' ',1)
                                                ,split_part('HBKEN-A - บางเขน-A',' ',1)
                                                ,split_part('HBKEN-B - บางเขน-B',' ',1)
                                                ,split_part('HBKPI-A - บางกะปิ-A',' ',1)
                                                ,split_part('HBKPI-B - บางกะปิ-B',' ',1)
                                                ,split_part('HBKTH-A - บางขุนเทียน-A',' ',1)
                                                ,split_part('HBKTH-B - เขตบางขุนเทียน-B',' ',1)
                                                ,split_part('HBKUM - บึงกุ่ม',' ',1)
                                                ,split_part('HBPAT - บางพลัด',' ',1)
                                                ,split_part('HBSUE - บางซื่อ',' ',1)
                                                ,split_part('HDDNG - ดินแดง',' ',1)
                                                ,split_part('HDONM - ดอนเมือง',' ',1)
                                                ,split_part('HHKWG-A - ห้วยขวาง-A',' ',1)
                                                ,split_part('HHKWG-B - ห้วยขวาง-B',' ',1)
                                                ,split_part('HJJAK-A - จตุจักร-A',' ',1)
                                                ,split_part('HJJAK-B - จตุจักร-B',' ',1)
                                                ,split_part('HJJAK-C - เขตจตุุจักร-C',' ',1)
                                                ,split_part('HJOMT-A - จอมทอง-A',' ',1)
                                                ,split_part('HJOMT-B - จอมทอง-B',' ',1)
                                                ,split_part('HKSAN - เขตคลองสาน',' ',1)
                                                ,split_part('HKSWA-A - คลองสามวา-A',' ',1)
                                                ,split_part('HKSWA-B - คลองสามวา-B',' ',1)
                                                ,split_part('HKYAO - คันนายาว',' ',1)
                                                ,split_part('HLAEM - บางคอแหลม',' ',1)
                                                ,split_part('HLANG-A - วังทองหลาง-A',' ',1)
                                                ,split_part('HLANG-B - เขตวังทองหลาง B',' ',1)
                                                ,split_part('HLDKB - ลาดกระบัง',' ',1)
                                                ,split_part('HLKSI - หลักสี่',' ',1)
                                                ,split_part('HMBRI - มีนบุรี',' ',1)
                                                ,split_part('HNJOK-A - หนองจอก-A',' ',1)
                                                ,split_part('HNJOK-B - หนองจอก-B',' ',1)
                                                ,split_part('HNKAM - หนองแขม',' ',1)
                                                ,split_part('HPASI-A - ภาษีเจริญ-A',' ',1)
                                                ,split_part('HPASI-B - ภาษีเจริญ-B',' ',1)
                                                ,split_part('HPKNG - เขตพระโขนง',' ',1)
                                                ,split_part('HPRAO-A - ลาดพร้าว-A',' ',1)
                                                ,split_part('HPRAO-B - ลาดพร้าว-B',' ',1)
                                                ,split_part('HPRAP - ป้อมปราบศัตรูพ่าย',' ',1)
                                                ,split_part('HPWAN - เขตปทุมวัน',' ',1)
                                                ,split_part('HPWET-A - ประเวศ-A',' ',1)
                                                ,split_part('HPWET-B - ประเวศ-B',' ',1)
                                                ,split_part('HPYTH-A - พญาไท-A',' ',1)
                                                ,split_part('HPYTH-B - พญาไท-B',' ',1)
                                                ,split_part('HRBRN - ราษฎร์บูรณะ',' ',1)
                                                ,split_part('HRCTW - ราชเทวี',' ',1)
                                                ,split_part('HSAMP - สัมพันธวงศ์',' ',1)
                                                ,split_part('HSAPA - สะพานสูง',' ',1)
                                                ,split_part('HSATH - สาทร',' ',1)
                                                ,split_part('HBRAK - บางรัก',' ',1)
                                                ,split_part('HSLNG-A - สวนหลวง-A',' ',1)
                                                ,split_part('HSLNG-B - เขตสวนหลวง-B',' ',1)
                                                ,split_part('HSMAI-A - สายไหม-A',' ',1)
                                                ,split_part('HSMAI-B - สายไหม-B',' ',1)
                                                ,split_part('HTAWI - ทวีวัฒนา',' ',1)
                                                ,split_part('HTHON - ธนบุรี',' ',1)
                                                ,split_part('HBKKY - บางกอกใหญ่',' ',1)
                                                ,split_part('HTLCH - ตลิ่งชัน',' ',1)
                                                ,split_part('HTOEI - คลองเตย',' ',1)
                                                ,split_part('HWTNA - วัฒนา',' ',1)
                                                ,split_part('HYNWA - ยานนาวา',' ',1)
                                                ,split_part('HBBTG-A - บางบัวทอง',' ',1)
                                                ,split_part('HBBTG-B - บางบัวทอง-B',' ',1)
                                                ,split_part('HBYAI - บางใหญ่',' ',1)
                                                ,split_part('HGUAI - อำเภอบางกรวย',' ',1)
                                                ,split_part('HKRET-A - ปากเกร็ด-A',' ',1)
                                                ,split_part('HKRET-B - ปากเกร็ด-B',' ',1)
                                                ,split_part('HKRET-C - ปากเกร็ด-C',' ',1)
                                                ,split_part('HNONT-A - นนทบุรี-A',' ',1)
                                                ,split_part('HNONT-B - นนทบุรี-B',' ',1)
                                                ,split_part('HNONT-C - นนทบุรี-C',' ',1)
                                                ,split_part('HKLNG-A - คลองหลวง-A',' ',1)
                                                ,split_part('HKLNG-B - คลองหลวง-B',' ',1)
                                                ,split_part('HKLNG-C - คลองหลวง-C',' ',1)
                                                ,split_part('HLDLK - ลาดหลุมแก้ว',' ',1)
                                                ,split_part('HLUKA-A - ลำลูกกา-A',' ',1)
                                                ,split_part('HLUKA-B - ลำลูกกา-B',' ',1)
                                                ,split_part('HPTUM-A - ปทุมธานี-A',' ',1)
                                                ,split_part('HPTUM-B - ปทุมธานี-B',' ',1)
                                                ,split_part('HTYBR-A - ธัญบุรี-A',' ',1)
                                                ,split_part('HTYBR-B - ธัญบุรี-B',' ',1)
                                                ,split_part('HBGBO - บางบ่อ',' ',1)
                                                ,split_part('HBPLI-A - บางพลี-A',' ',1)
                                                ,split_part('HBPLI-B - บางพลี-B',' ',1)
                                                ,split_part('HBPLI-C - บางพลี-C',' ',1)
                                                ,split_part('HBSAO - บางเสาธง',' ',1)
                                                ,split_part('HPAPD - พระประแดง',' ',1)
                                                ,split_part('HSMJD - อำเภอพระสมุทรเจดีย์',' ',1)
                                                ,split_part('HSMPK-A - สมุทรปราการ-A',' ',1)
                                                ,split_part('HSMPK-B - สมุทรปราการ-B',' ',1)
                                                ,split_part('HSMPK-C - สมุทรปราการ-C',' ',1)
                                                ,split_part('HSAKN-A - สมุุทรสาคร-A',' ',1)
                                                ,split_part('HSAKN-B - สมุุทรสาคร-B',' ',1)
                                                ,split_part('HBPAW - อำเภอบ้านแพ้ว',' ',1)
                                                ,split_part('HKTBN - อำเภอกระทุ่มแบน',' ',1)
                                        ) then 'GBKK' 
                                          when latest_awb_station_name = any (values 
                                                'Kerry'
                                                ,'4PL-Kerry (non-int)'
                                                ,'4PL-Kerry (R3)'
                                                ,'Flash'
                                                ,'Ninja van' 
                                        ) then '4PL'
                                        ELSE 'UPC' END AS Correct_hub_zone
                                        
from thopsbi_spx.dwd_pub_shipment_info_df_th as fleet_order 
left join 
    (
        select 
            shipment_id
            ,status_time as exceptional_time 
            ,r2_destination as r2_r3_destination
            ,wrong_destination
            ,status
        from 
        (
            select 
                shipment_id
                ,from_unixtime(ctime-3600) as status_time 
                ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) as r2_destination 
                ,try(cast(json_extract(json_parse(content),'$.pickup_station_name') as varchar)) as wrong_destination  
                ,row_number() over(partition by shipment_id order by ctime desc) as row_number 
                ,status

            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            where status in (82,83)
        )       
        where row_number = 1 and date(status_time) = current_date - interval '1' day 
        -- where row_number = 1 and date(status_time) between current_date - interval '7' day  and current_date - interval '1' day 
        and r2_destination like 'SOC_'
    ) as r2_r3_track
on fleet_order.shipment_id = r2_r3_track.shipment_id
left join 
    (
        with latest_soc_recieved as(
            select 
                shipment_id
                ,from_unixtime(ctime-3600) as status_time 
                ,status
                ,try(cast(json_extract(json_parse(content),'$.dest_station_name') as varchar)) as dest_station_name 
                ,try(cast(json_extract(json_parse(content),'$.pickup_station_name') as varchar)) as pickup_station_name
                ,row_number() over (partition by shipment_id order by ctime-360 desc) as row_number
            from spx_mart.shopee_ssc_spx_track_th_db__order_tracking_tab__reg_continuous_s0_live
            where 
                status = 8
        )
        select *
        from latest_soc_recieved
        where row_number = 1
    ) as latest_sc_time
on fleet_order.shipment_id = latest_sc_time.shipment_id
where 
    date(exceptional_time) = current_date - interval '1' day
group by   
    date(exceptional_time) 
    ,fleet_order.shipment_id
    ,latest_sc_time.status_time
    ,wrong_destination 
    ,coalesce(latest_awb_station_name,'Flash')
    ,case when split_part(coalesce(latest_awb_station_name,'Flash'),' ',1) = any (values  split_part('HBBON - บางบอน',' ',1)
                                                ,split_part('HBGNA-A - บางนา-A',' ',1)
                                                ,split_part('HBGNA-B - บางนา-B',' ',1)
                                                ,split_part('HBKAE-A - บางแค-A',' ',1)
                                                ,split_part('HBKAE-B - บางแค-B',' ',1)
                                                ,split_part('HBKEN-A - บางเขน-A',' ',1)
                                                ,split_part('HBKEN-B - บางเขน-B',' ',1)
                                                ,split_part('HBKPI-A - บางกะปิ-A',' ',1)
                                                ,split_part('HBKPI-B - บางกะปิ-B',' ',1)
                                                ,split_part('HBKTH-A - บางขุนเทียน-A',' ',1)
                                                ,split_part('HBKTH-B - เขตบางขุนเทียน-B',' ',1)
                                                ,split_part('HBKUM - บึงกุ่ม',' ',1)
                                                ,split_part('HBPAT - บางพลัด',' ',1)
                                                ,split_part('HBSUE - บางซื่อ',' ',1)
                                                ,split_part('HDDNG - ดินแดง',' ',1)
                                                ,split_part('HDONM - ดอนเมือง',' ',1)
                                                ,split_part('HHKWG-A - ห้วยขวาง-A',' ',1)
                                                ,split_part('HHKWG-B - ห้วยขวาง-B',' ',1)
                                                ,split_part('HJJAK-A - จตุจักร-A',' ',1)
                                                ,split_part('HJJAK-B - จตุจักร-B',' ',1)
                                                ,split_part('HJJAK-C - เขตจตุุจักร-C',' ',1)
                                                ,split_part('HJOMT-A - จอมทอง-A',' ',1)
                                                ,split_part('HJOMT-B - จอมทอง-B',' ',1)
                                                ,split_part('HKSAN - เขตคลองสาน',' ',1)
                                                ,split_part('HKSWA-A - คลองสามวา-A',' ',1)
                                                ,split_part('HKSWA-B - คลองสามวา-B',' ',1)
                                                ,split_part('HKYAO - คันนายาว',' ',1)
                                                ,split_part('HLAEM - บางคอแหลม',' ',1)
                                                ,split_part('HLANG-A - วังทองหลาง-A',' ',1)
                                                ,split_part('HLANG-B - เขตวังทองหลาง B',' ',1)
                                                ,split_part('HLDKB - ลาดกระบัง',' ',1)
                                                ,split_part('HLKSI - หลักสี่',' ',1)
                                                ,split_part('HMBRI - มีนบุรี',' ',1)
                                                ,split_part('HNJOK-A - หนองจอก-A',' ',1)
                                                ,split_part('HNJOK-B - หนองจอก-B',' ',1)
                                                ,split_part('HNKAM - หนองแขม',' ',1)
                                                ,split_part('HPASI-A - ภาษีเจริญ-A',' ',1)
                                                ,split_part('HPASI-B - ภาษีเจริญ-B',' ',1)
                                                ,split_part('HPKNG - เขตพระโขนง',' ',1)
                                                ,split_part('HPRAO-A - ลาดพร้าว-A',' ',1)
                                                ,split_part('HPRAO-B - ลาดพร้าว-B',' ',1)
                                                ,split_part('HPRAP - ป้อมปราบศัตรูพ่าย',' ',1)
                                                ,split_part('HPWAN - เขตปทุมวัน',' ',1)
                                                ,split_part('HPWET-A - ประเวศ-A',' ',1)
                                                ,split_part('HPWET-B - ประเวศ-B',' ',1)
                                                ,split_part('HPYTH-A - พญาไท-A',' ',1)
                                                ,split_part('HPYTH-B - พญาไท-B',' ',1)
                                                ,split_part('HRBRN - ราษฎร์บูรณะ',' ',1)
                                                ,split_part('HRCTW - ราชเทวี',' ',1)
                                                ,split_part('HSAMP - สัมพันธวงศ์',' ',1)
                                                ,split_part('HSAPA - สะพานสูง',' ',1)
                                                ,split_part('HSATH - สาทร',' ',1)
                                                ,split_part('HBRAK - บางรัก',' ',1)
                                                ,split_part('HSLNG-A - สวนหลวง-A',' ',1)
                                                ,split_part('HSLNG-B - เขตสวนหลวง-B',' ',1)
                                                ,split_part('HSMAI-A - สายไหม-A',' ',1)
                                                ,split_part('HSMAI-B - สายไหม-B',' ',1)
                                                ,split_part('HTAWI - ทวีวัฒนา',' ',1)
                                                ,split_part('HTHON - ธนบุรี',' ',1)
                                                ,split_part('HBKKY - บางกอกใหญ่',' ',1)
                                                ,split_part('HTLCH - ตลิ่งชัน',' ',1)
                                                ,split_part('HTOEI - คลองเตย',' ',1)
                                                ,split_part('HWTNA - วัฒนา',' ',1)
                                                ,split_part('HYNWA - ยานนาวา',' ',1)
                                                ,split_part('HBBTG-A - บางบัวทอง',' ',1)
                                                ,split_part('HBBTG-B - บางบัวทอง-B',' ',1)
                                                ,split_part('HBYAI - บางใหญ่',' ',1)
                                                ,split_part('HGUAI - อำเภอบางกรวย',' ',1)
                                                ,split_part('HKRET-A - ปากเกร็ด-A',' ',1)
                                                ,split_part('HKRET-B - ปากเกร็ด-B',' ',1)
                                                ,split_part('HKRET-C - ปากเกร็ด-C',' ',1)
                                                ,split_part('HNONT-A - นนทบุรี-A',' ',1)
                                                ,split_part('HNONT-B - นนทบุรี-B',' ',1)
                                                ,split_part('HNONT-C - นนทบุรี-C',' ',1)
                                                ,split_part('HKLNG-A - คลองหลวง-A',' ',1)
                                                ,split_part('HKLNG-B - คลองหลวง-B',' ',1)
                                                ,split_part('HKLNG-C - คลองหลวง-C',' ',1)
                                                ,split_part('HLDLK - ลาดหลุมแก้ว',' ',1)
                                                ,split_part('HLUKA-A - ลำลูกกา-A',' ',1)
                                                ,split_part('HLUKA-B - ลำลูกกา-B',' ',1)
                                                ,split_part('HPTUM-A - ปทุมธานี-A',' ',1)
                                                ,split_part('HPTUM-B - ปทุมธานี-B',' ',1)
                                                ,split_part('HTYBR-A - ธัญบุรี-A',' ',1)
                                                ,split_part('HTYBR-B - ธัญบุรี-B',' ',1)
                                                ,split_part('HBGBO - บางบ่อ',' ',1)
                                                ,split_part('HBPLI-A - บางพลี-A',' ',1)
                                                ,split_part('HBPLI-B - บางพลี-B',' ',1)
                                                ,split_part('HBPLI-C - บางพลี-C',' ',1)
                                                ,split_part('HBSAO - บางเสาธง',' ',1)
                                                ,split_part('HPAPD - พระประแดง',' ',1)
                                                ,split_part('HSMJD - อำเภอพระสมุทรเจดีย์',' ',1)
                                                ,split_part('HSMPK-A - สมุทรปราการ-A',' ',1)
                                                ,split_part('HSMPK-B - สมุทรปราการ-B',' ',1)
                                                ,split_part('HSMPK-C - สมุทรปราการ-C',' ',1)
                                                ,split_part('HSAKN-A - สมุุทรสาคร-A',' ',1)
                                                ,split_part('HSAKN-B - สมุุทรสาคร-B',' ',1)
                                                ,split_part('HBPAW - อำเภอบ้านแพ้ว',' ',1)
                                                ,split_part('HKTBN - อำเภอกระทุ่มแบน',' ',1)
                                        ) then 'GBKK' 
                                          when latest_awb_station_name = any (values 
                                                'Kerry'
                                                ,'4PL-Kerry (non-int)'
                                                ,'4PL-Kerry (R3)'
                                                ,'Flash'
                                                ,'Ninja van' 
                                        ) then '4PL'
                                        ELSE 'UPC' END
    ,case when exceptional_time is not null and fleet_order.is_4pl = true then 'r3'
    when  exceptional_time is not null then 'r2'
    end     
order by date(exceptional_time) asc

