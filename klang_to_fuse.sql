with thai_state AS 
(
    SELECT 
        CAST(id AS VARCHAR) AS thai_state
    FROM 
    (
        SELECT CAST(CAST(('จังหวัดกรุงเทพมหานคร','จังหวัดนนทบุรี','จังหวัดปทุมธานี','จังหวัดสมุทรปราการ','จังหวัดสมุทรสาคร','จังหวัดชัยนาท','จังหวัดพระนครศรีอยุธยา','จังหวัดลพบุรี','จังหวัดสระบุรี','จังหวัดสิงห์บุรี','จังหวัดอ่างทอง','จังหวัดอุทัยธานี','จังหวัดปราจีนบุรี'
        ,'จังหวัดนครนายก','จังหวัดจันทบุรี','จังหวัดฉะเชิงเทรา','จังหวัดชลบุรี','จังหวัดตราด','จังหวัดระยอง','จังหวัดสระแก้ว','จังหวัดเพชรบูรณ์','จังหวัดกำแพงเพชร','จังหวัดนครสวรรค์','จังหวัดพิจิตร','จังหวัดพิษณุโลก','จังหวัดสุโขทัย','จังหวัดเชียงใหม่','จังหวัดเชียงราย'
        ,'จังหวัดแพร่','จังหวัดแม่ฮ่องสอน','จังหวัดน่าน','จังหวัดพะเยา','จังหวัดลำปาง','จังหวัดลำพูน','จังหวัดอุตรดิตถ์','จังหวัดเลย','จังหวัดตาก','จังหวัดกาฬสินธุ์','จังหวัดขอนแก่น','จังหวัดชัยภูมิ','จังหวัดนครพนม','จังหวัดนครราชสีมา','จังหวัดบึงกาฬ','จังหวัดบุรีรัมย์'
        ,'จังหวัดมหาสารคาม','จังหวัดมุกดาหาร','จังหวัดยโสธร','จังหวัดร้อยเอ็ด','จังหวัดศรีสะเกษ','จังหวัดสกลนคร','จังหวัดสุรินทร์','จังหวัดหนองคาย','จังหวัดหนองบัวลำภู','จังหวัดอำนาจเจริญ','จังหวัดอุดรธานี','จังหวัดอุบลราชธานี','จังหวัดกระบี่','จังหวัดชุมพร','จังหวัดตรัง'
        ,'จังหวัดนครศรีธรรมราช','จังหวัดนราธิวาส','จังหวัดปัตตานี','จังหวัดพังงา','จังหวัดพัทลุง','จังหวัดภูเก็ต','จังหวัดยะลา','จังหวัดระนอง','จังหวัดสงขลา','จังหวัดสตูล','จังหวัดสุราษฎร์ธานี','จังหวัดนครปฐม','จังหวัดสมุทรสงคราม','จังหวัดสุพรรณบุรี','จังหวัดเพชรบุรี','จังหวัดกาญจนบุรี'
        ,'จังหวัดประจวบคีรีขันธ์','จังหวัดราชบุรี') AS JSON) AS array(JSON)) AS bar
    )
    CROSS JOIN UNNEST(bar) AS bar(id)
)
,raw_order_paid as
(
    SELECT 
        DATE(FROM_UNIXTIME(v4.shipping_confirm_time - 3600)) AS paid_date
        ,v4.ordersn
        ,v4.shopid
        ,CASE 
            WHEN shipping_method IN (70066,70025) OR fulfilment_channel_id IN (70066,70025) OR service_code in ('H05', 'H06', 'H07','H08') THEN 1 
            ELSE 0 
        END AS is_spx
        ,CASE 
            WHEN shop_whs.whs_id is not null THEN 'WHS'
            WHEN cb_option = 1 THEN 'CB'
            ELSE 'MKP' 
        END AS order_type
        ,logistics_tab.buyer_addr_district 
        ,logistics_tab.buyer_addr_state
        ,CASE 
            WHEN buyer_addr_state IN ('จังหวัดกรุงเทพมหานคร', 'จังหวัดสมุทรปราการ', 'จังหวัดนนทบุรี', 'จังหวัดปทุมธานี') THEN 1
            else 0 
        end as is_gbkk 
        ,thai_state.thai_state
    FROM marketplace.shopee_order_v4_db__order_v4_tab__th_daily_s0_live as v4
    LEFT JOIN 
    (
        SELECT 
            ordersn
            ,service_code
            ,log_id
        FROM sls_mart.shopee_sls_logistic_th_db__logistic_request_tab_lfs_union_tmp
    ) as sls 
    ON v4.ordersn = sls.ordersn
    LEFT JOIN 
    (
        select 
            shopee_order_sn, whs_id
        from wms_mart.shopee_wms_th_db__sales_outbound_order_shopee_order_sn_v2_tab__th_daily_s0_live
    ) AS shop_whs
    ON v4.ordersn = shop_whs.shopee_order_sn
    LEFT JOIN 
    (
        SELECT  
            order_id
            ,extinfo.fulfilment_channel_id
            ,extinfo.buyer_address.state as buyer_addr_state
            ,extinfo.buyer_address.city as buyer_addr_district
            ,extinfo.buyer_address.state 
            ,extinfo.buyer_address.city 
        FROM marketplace.shopee_order_logistics_v2_db__order_logistics_v2_tab__th_daily_s0_live 
    ) as logistics_tab
    ON v4.orderid = logistics_tab.order_id
    LEFT JOIN thai_state 
    on thai_state.thai_state = logistics_tab.buyer_addr_state
)
,aggregate_avg as
(
select 
    paid_date
   ,buyer_addr_state
   ,buyer_addr_district 
   ,count(*) as total_order_piad 
   ,COUNT(CASE WHEN order_type = 'CB' THEN ordersn END) AS cb_paid
   ,COUNT(CASE WHEN order_type = 'WHS' THEN ordersn END) AS whs_paid
   ,COUNT(CASE WHEN order_type = 'MKP' THEN ordersn END) AS mkp_paid
from raw_order_paid 
where paid_date between DATE('2021-09-01') and DATE('2021-09-30')
and thai_state is not null
group by 1 ,2,3
)
select 
    buyer_addr_state
    ,buyer_addr_district
    ,avg(total_order_piad) as avg_order_paid
    ,avg(cb_paid) as avg_cb_paid
    ,avg(whs_paid) as avg_whs_pai
    ,avg(mkp_paid) as avg_mkp_paid
from aggregate_avg
group by 
    buyer_addr_state
    ,buyer_addr_district