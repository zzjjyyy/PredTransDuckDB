SELECT i_item_id, 
       s_state, 
       Grouping(s_state) AS g_state, 
       Avg(ss_quantity) AS agg1, 
       Avg(ss_list_price) AS agg2, 
       Avg(ss_coupon_amt) AS agg3, 
       Avg(ss_sales_price) AS agg4 
FROM   ss, cd, d, s, i 
WHERE  ss_sold_date_sk = d_date_sk 
       AND ss_item_sk = i_item_sk 
       AND ss_store_sk = s_store_sk 
       AND ss_cdemo_sk = cd_demo_sk 
       AND cd_gender = 'M' 
       AND cd_marital_status = 'D' 
       AND cd_education_status = 'College' 
       AND d_year = 2000 
       AND s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN' ) 
GROUP  BY rollup ( i_item_id, s_state ) 
ORDER  BY i_item_id, s_state
LIMIT 100; 
