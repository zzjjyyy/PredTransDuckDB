SELECT dt.d_year, 
       i.i_brand_id AS brand_id, 
       i.i_brand AS brand, 
       Sum(ss_ext_sales_price) AS ext_price 
FROM   dt, 
       ss, 
       i 
WHERE  dt.d_date_sk = ss.ss_sold_date_sk 
       AND ss.ss_item_sk = i.i_item_sk 
       AND i.i_manager_id = 1 
       AND dt.d_moy = 11 
       AND dt.d_year = 1999 
GROUP  BY dt.d_year, 
          i.i_brand, 
          i.i_brand_id 
ORDER  BY dt.d_year, 
          ext_price DESC, 
          brand_id
LIMIT 100; 
