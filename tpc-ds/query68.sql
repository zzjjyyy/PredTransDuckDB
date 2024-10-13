SELECT c_last_name, 
               c_first_name, 
               ca_city, 
               bought_city, 
               ss_ticket_number, 
               extended_price, 
               extended_tax, 
               list_price 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               ca_city                 bought_city, 
               Sum(ss_ext_sales_price) extended_price, 
               Sum(ss_ext_list_price)  list_price, 
               Sum(ss_ext_tax)         extended_tax 
        FROM   ss, 
               d, 
               s, 
               hd, 
               ca 
        WHERE  ss.ss_sold_date_sk = d.d_date_sk 
               AND ss.ss_store_sk = s.s_store_sk 
               AND ss.ss_hdemo_sk = hd.hd_demo_sk 
               AND ss.ss_addr_sk = ca.ca_address_sk 
               AND d.d_dom BETWEEN 1 AND 2 
               AND ( hd.hd_dep_count = 8 
                      OR hd.hd_vehicle_count = 3 ) 
               AND d.d_year IN ( 1998, 1998 + 1, 1998 + 2 ) 
               AND s.s_city IN ( 'Fairview', 'Midway' ) 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk, 
                  ss_addr_sk, 
                  ca_city) dn, 
       c, 
       current_addr 
WHERE  ss_customer_sk = c_customer_sk 
       AND c.c_current_addr_sk = current_addr.ca_address_sk 
       AND current_addr.ca_city <> bought_city 
ORDER  BY c_last_name, 
          ss_ticket_number
LIMIT 100; 
