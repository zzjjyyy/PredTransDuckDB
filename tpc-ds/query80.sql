WITH ssr AS 
( 
                SELECT s_store_id AS store_id, 
                       Sum(ss_ext_sales_price) AS sales, 
                       Sum(COALESCE(sr_return_amt, 0)) AS returns1, 
                       Sum(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit 
                FROM ss 
                LEFT OUTER JOIN sr 
                ON (ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number), 
                                d, 
                                s, 
                                i, 
                                p 
                WHERE           ss_sold_date_sk = d_date_sk 
                AND             d_date BETWEEN Cast('2000-08-26' AS DATE) AND (Cast('2000-08-26' AS DATE) + INTERVAL '30' day) 
                AND             ss_store_sk = s_store_sk 
                AND             ss_item_sk = i_item_sk 
                AND             i_current_price > 50 
                AND             ss_promo_sk = p_promo_sk 
                AND             p_channel_tv = 'N' 
                GROUP BY        s_store_id) , csr AS 
( 
                SELECT cp_catalog_page_id AS catalog_page_id, 
                       sum(cs_ext_sales_price) AS sales, 
                       sum(COALESCE(cr_return_amount, 0)) AS returns1, 
                       sum(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit 
                FROM            cs 
                LEFT OUTER JOIN cr 
                ON (cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number), 
                                d, 
                                cp, 
                                i, 
                                p 
                WHERE           cs_sold_date_sk = d_date_sk 
                AND             d_date BETWEEN cast('2000-08-26' AS date) AND (cast('2000-08-26' AS date) + INTERVAL '30' day) 
                AND             cs_catalog_page_sk = cp_catalog_page_sk 
                AND             cs_item_sk = i_item_sk 
                AND             i_current_price > 50 
                AND             cs_promo_sk = p_promo_sk 
                AND             p_channel_tv = 'N' 
                GROUP BY        cp_catalog_page_id) , wsr AS 
( 
                SELECT web_site_id, 
                       sum(ws_ext_sales_price) AS sales, 
                       sum(COALESCE(wr_return_amt, 0)) AS returns1, 
                       sum(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit 
                FROM            ws 
                LEFT OUTER JOIN wr 
                ON (ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number), 
                                d, 
                                web, 
                                i, 
                                p 
                WHERE           ws_sold_date_sk = d_date_sk 
                AND             d_date BETWEEN cast('2000-08-26' AS date) AND (cast('2000-08-26' AS date) + INTERVAL '30' day) 
                AND             ws_web_site_sk = web_site_sk 
                AND             ws_item_sk = i_item_sk 
                AND             i_current_price > 50 
                AND             ws_promo_sk = p_promo_sk 
                AND             p_channel_tv = 'N' 
                GROUP BY        web_site_id) 
SELECT channel,
       id,
       sum(sales) AS sales,
       sum(returns1) AS returns1,
       sum(profit) AS profit
FROM ( 
                SELECT 'store channel' AS channel, 
                       'store' || store_id AS id, 
                       sales, 
                       returns1, 
                       profit 
                FROM ssr 
                UNION ALL 
                SELECT 'catalog channel' AS channel, 
                       'catalog_page' || catalog_page_id AS id, 
                       sales, 
                       returns1, 
                       profit 
                FROM csr 
                UNION ALL 
                SELECT 'web channel' AS channel, 
                       'web_site' || web_site_id AS id, 
                       sales, 
                       returns1, 
                       profit 
                FROM wsr) x 
GROUP BY rollup (channel, id) 
ORDER BY channel, 
         id 
LIMIT 100; 

