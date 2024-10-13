SELECT 
         i_item_id, 
         i_item_desc, 
         i_category, 
         i_class, 
         i_current_price, 
         sum(cs_ext_sales_price) AS itemrevenue ,
         sum(cs_ext_sales_price)*100 / sum(sum(cs_ext_sales_price)) OVER (partition BY i_class) AS revenueratio 
FROM cs,
     i,
     d
WHERE cs_item_sk = i_item_sk 
AND i_category IN ('Children', 'Women', 'Electronics') 
AND cs_sold_date_sk = d_date_sk 
AND d_date BETWEEN cast('2001-02-03' AS DATE) AND (cast('2001-02-03' AS DATE) + INTERVAL '30' day) 
GROUP BY i_item_id, 
         i_item_desc, 
         i_category, 
         i_class, 
         i_current_price 
ORDER BY i_category, 
         i_class, 
         i_item_id, 
         i_item_desc, 
         revenueratio 
LIMIT 100; 

