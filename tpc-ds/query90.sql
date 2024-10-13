SELECT Cast(amc AS DECIMAL(15, 4)) / Cast(pmc AS DECIMAL(15, 4)) 
               am_pm_ratio 
FROM   (SELECT Count(*) amc 
        FROM   ws, 
               hd, 
               t, 
               wp 
        WHERE  ws_sold_time_sk = t.t_time_sk 
               AND ws_ship_hdemo_sk = hd.hd_demo_sk 
               AND ws_web_page_sk = wp.wp_web_page_sk 
               AND t.t_hour BETWEEN 12 AND 12 + 1 
               AND hd.hd_dep_count = 8 
               AND wp.wp_char_count BETWEEN 5000 AND 5200) at1, 
       (SELECT Count(*) pmc 
        FROM   ws, 
               hd, 
               t, 
               wp 
        WHERE  ws_sold_time_sk = t.t_time_sk 
               AND ws_ship_hdemo_sk = hd.hd_demo_sk 
               AND ws_web_page_sk = wp.wp_web_page_sk 
               AND t.t_hour BETWEEN 20 AND 20 + 1 
               AND hd.hd_dep_count = 8 
               AND wp.wp_char_count BETWEEN 5000 AND 5200) pt 
ORDER  BY am_pm_ratio
LIMIT 100; 
