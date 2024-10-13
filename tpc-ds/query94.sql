SELECT 
         Count(DISTINCT ws_order_number) AS 'order count' , 
         Sum(ws_ext_ship_cost)           AS 'total shipping cost' , 
         Sum(ws_net_profit)              AS 'total net profit' 
FROM     ws1 , 
         d, 
         ca, 
         web 
WHERE    d_date BETWEEN '2000-3-01' AND (Cast('2000-3-01' AS DATE) + INTERVAL '60' day) 
AND      ws1.ws_ship_date_sk = d_date_sk 
AND      ws1.ws_ship_addr_sk = ca_address_sk 
AND      ca_state = 'MT' 
AND      ws1.ws_web_site_sk = web_site_sk 
AND      web_company_name = 'pri' 
AND      EXISTS 
         ( 
                SELECT * 
                FROM   ws2 
                WHERE  ws1.ws_order_number = ws2.ws_order_number 
                AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk) 
AND      NOT EXISTS 
         ( 
                SELECT * 
                FROM   wr1 
                WHERE  ws1.ws_order_number = wr1.wr_order_number) 
ORDER BY count(DISTINCT ws_order_number) 
LIMIT 100; 

