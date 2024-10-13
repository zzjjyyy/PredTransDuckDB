SELECT Count(*) 
FROM   ss, 
       hd, 
       t, 
       s 
WHERE  ss_sold_time_sk = t.t_time_sk 
       AND ss_hdemo_sk = hd.hd_demo_sk 
       AND ss_store_sk = s_store_sk 
       AND t.t_hour = 15 
       AND t.t_minute >= 30 
       AND hd.hd_dep_count = 7 
       AND s.s_store_name = 'ese' 
ORDER  BY Count(*)
LIMIT 100; 
