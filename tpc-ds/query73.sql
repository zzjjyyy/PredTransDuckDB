SELECT c_last_name, 
       c_first_name, 
       c_salutation, 
       c_preferred_cust_flag, 
       ss_ticket_number, 
       cnt 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               Count(*) cnt 
        FROM   ss, 
               d, 
               s, 
               hd 
        WHERE  ss.ss_sold_date_sk = d.d_date_sk 
               AND ss.ss_store_sk = s.s_store_sk 
               AND ss.ss_hdemo_sk = hd.hd_demo_sk 
               AND d.d_dom BETWEEN 1 AND 2 
               AND ( hd.hd_buy_potential = '>10000' 
                      OR hd.hd_buy_potential = '0-500' ) 
               AND hd.hd_vehicle_count > 0 
               AND CASE 
                     WHEN hd.hd_vehicle_count > 0 THEN 
                     hd.hd_dep_count / 
                     hd.hd_vehicle_count 
                     ELSE NULL 
                   END > 1 
               AND d.d_year IN ( 2000, 2000 + 1, 2000 + 2 ) 
               AND s.s_county IN ( 'Williamson County', 'Williamson County', 
                                       'Williamson County', 
                                                             'Williamson County' 
                                     ) 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk) dj, 
       c 
WHERE  ss_customer_sk = c_customer_sk 
       AND cnt BETWEEN 1 AND 5 
ORDER  BY cnt DESC, 
          c_last_name ASC; 
