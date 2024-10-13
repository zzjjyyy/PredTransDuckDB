SELECT cd_gender, 
               cd_marital_status, 
               cd_education_status, 
               Count(*) cnt1, 
               cd_purchase_estimate, 
               Count(*) cnt2, 
               cd_credit_rating, 
               Count(*) cnt3 
FROM   c, 
       ca, 
       cd 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_state IN ( 'KS', 'AZ', 'NE' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   ss, 
                          d 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 2004 
                          AND d_moy BETWEEN 3 AND 3 + 2) 
       AND ( NOT EXISTS (SELECT * 
                         FROM   ws, 
                                d 
                         WHERE  c.c_customer_sk = ws_bill_customer_sk 
                                AND ws_sold_date_sk = d_date_sk 
                                AND d_year = 2004 
                                AND d_moy BETWEEN 3 AND 3 + 2) 
             AND NOT EXISTS (SELECT * 
                             FROM   cs, 
                                    d 
                             WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                    AND cs_sold_date_sk = d_date_sk 
                                    AND d_year = 2004 
                                    AND d_moy BETWEEN 3 AND 3 + 2) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating
LIMIT 100; 
