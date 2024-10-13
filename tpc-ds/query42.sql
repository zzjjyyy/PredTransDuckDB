SELECT dt.d_year, 
       i.i_category_id, 
       i.i_category, 
       Sum(ss_ext_sales_price) 
FROM   dt, 
       ss, 
       i 
WHERE  dt.d_date_sk = ss.ss_sold_date_sk 
       AND ss.ss_item_sk = i.i_item_sk 
       AND i.i_manager_id = 1 
       AND dt.d_moy = 12 
       AND dt.d_year = 2000 
GROUP  BY dt.d_year, 
          i.i_category_id, 
          i.i_category 
ORDER  BY Sum(ss_ext_sales_price) DESC, 
          dt.d_year, 
          i.i_category_id, 
          i.i_category
LIMIT 100; 
