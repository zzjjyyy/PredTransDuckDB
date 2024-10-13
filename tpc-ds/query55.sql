SELECT i_brand_id AS brand_id, 
       i_brand AS brand, 
       Sum(ss_ext_sales_price) AS ext_price 
FROM   d, 
       ss, 
       i 
WHERE  d_date_sk = ss_sold_date_sk 
       AND ss_item_sk = i_item_sk 
       AND i_manager_id = 33 
       AND d_moy = 12 
       AND d_year = 1998 
GROUP  BY i_brand, 
          i_brand_id 
ORDER  BY ext_price DESC, 
          i_brand_id
LIMIT 100; 
