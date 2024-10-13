SELECT c_last_name, 
       c_first_name, 
       ca_city, 
       bought_city, 
       ss_ticket_number, 
       amt, 
       profit 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               ca_city AS bought_city, 
               Sum(ss_coupon_amt) AS amt, 
               Sum(ss_net_profit) AS profit 
        FROM   ss, 
               d, 
               s, 
               hd, 
               ca
        WHERE  ss.ss_sold_date_sk = d.d_date_sk 
               AND ss.ss_store_sk = s.s_store_sk 
               AND ss.ss_hdemo_sk = hd.hd_demo_sk 
               AND ss.ss_addr_sk = ca.ca_address_sk 
               AND (hd.hd_dep_count = 6 
                    OR hd.hd_vehicle_count = 0) 
               AND d.d_dow IN (6, 0) 
               AND d.d_year IN (2000, 2000 + 1, 2000 + 2) 
               AND s.s_city IN ('Midway', 'Fairview', 'Fairview', 'Fairview', 'Fairview') 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk, 
                  ss_addr_sk, 
                  ca_city) dn, 
       c, 
       ca AS current_addr 
WHERE  ss_customer_sk = c_customer_sk 
       AND c.c_current_addr_sk = current_addr.ca_address_sk 
       AND current_addr.ca_city <> bought_city 
ORDER  BY c_last_name, 
          c_first_name, 
          ca_city, 
          bought_city, 
          ss_ticket_number
LIMIT 100; 
