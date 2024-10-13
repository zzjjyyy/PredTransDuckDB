SELECT c_last_name, 
       c_first_name, 
       substr(s_city, 1, 30), 
       ss_ticket_number, 
       amt, 
       profit 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               s.s_city, 
               Sum(ss_coupon_amt) AS amt, 
               Sum(ss_net_profit) AS profit 
        FROM   ss, 
               d, 
               s, 
               hd 
        WHERE  ss.ss_sold_date_sk = d.d_date_sk 
               AND ss.ss_store_sk = s.s_store_sk 
               AND ss.ss_hdemo_sk = hd.hd_demo_sk 
               AND (hd.hd_dep_count = 8 OR hd.hd_vehicle_count > 4 ) 
               AND d.d_dow = 1 
               AND d.d_year IN (2000, 2000 + 1, 2000 + 2) 
               AND s.s_number_employees BETWEEN 200 AND 295 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk, 
                  ss_addr_sk, 
                  s.s_city) ms, 
       c 
WHERE  ss_customer_sk = c_customer_sk 
ORDER  BY c_last_name, 
          c_first_name, 
          Substr(s_city, 1, 30), 
          profit
LIMIT 100; 
