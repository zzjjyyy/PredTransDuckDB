SELECT Count(*) 
FROM   (SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   ss, 
               d, 
               c 
        WHERE  ss_sold_date_sk = d_date_sk 
               AND ss_customer_sk = c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11 
        INTERSECT 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   cs, 
               d, 
               c 
        WHERE  cs_sold_date_sk = d_date_sk 
               AND cs_bill_customer_sk = c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11 
        INTERSECT 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   ws, 
               d, 
               c 
        WHERE  ws_sold_date_sk = d_date_sk 
               AND ws_bill_customer_sk = c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11) hot_cust
LIMIT 100; 
