SELECT i_item_id, 
       Avg(cs_quantity) AS agg1, 
       Avg(cs_list_price) AS agg2, 
       Avg(cs_coupon_amt) AS agg3, 
       Avg(cs_sales_price) AS agg4 
FROM   cs, cd, d, i, p 
WHERE  cs_sold_date_sk = d_date_sk 
       AND cs_item_sk = i_item_sk 
       AND cs_bill_cdemo_sk = cd_demo_sk 
       AND cs_promo_sk = p_promo_sk 
       AND cd_gender = 'F' 
       AND cd_marital_status = 'W' 
       AND cd_education_status = 'Secondary' 
       AND (p_channel_email = 'N' OR p_channel_event = 'N') 
       AND d_year = 2000 
GROUP  BY i_item_id 
ORDER  BY i_item_id
LIMIT 100; 
