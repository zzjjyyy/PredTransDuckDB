WITH ssCTE AS 
( 
         SELECT   s_store_sk, 
                  Sum(ss_ext_sales_price) AS sales, 
                  Sum(ss_net_profit)      AS profit 
         FROM     ss, 
                  d, 
                  s 
         WHERE    ss_sold_date_sk = d_date_sk 
         AND      d_date BETWEEN Cast('2001-08-16' AS DATE) AND      ( 
                           Cast('2001-08-16' AS DATE) + INTERVAL '30' day) 
         AND      ss_store_sk = s_store_sk 
         GROUP BY s_store_sk) , srCTE AS 
( 
         SELECT   s_store_sk, 
                  sum(sr_return_amt) AS returns1, 
                  sum(sr_net_loss)   AS profit_loss 
         FROM     sr, 
                  d, 
                  s 
         WHERE    sr_returned_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         AND      sr_store_sk = s_store_sk 
         GROUP BY s_store_sk), csCTE AS 
( 
         SELECT   cs_call_center_sk, 
                  sum(cs_ext_sales_price) AS sales, 
                  sum(cs_net_profit)      AS profit 
         FROM     cs, 
                  d 
         WHERE    cs_sold_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         GROUP BY cs_call_center_sk ), crCTE AS 
( 
         SELECT   cr_call_center_sk, 
                  sum(cr_return_amount) AS returns1, 
                  sum(cr_net_loss)      AS profit_loss 
         FROM     cr, 
                  d 
         WHERE    cr_returned_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         GROUP BY cr_call_center_sk ), wsCTE AS 
( 
         SELECT   wp_web_page_sk, 
                  sum(ws_ext_sales_price) AS sales, 
                  sum(ws_net_profit)      AS profit 
         FROM     ws, 
                  d, 
                  wp 
         WHERE    ws_sold_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         AND      ws_web_page_sk = wp_web_page_sk 
         GROUP BY wp_web_page_sk), wrCTE AS 
( 
         SELECT   wp_web_page_sk, 
                  sum(wr_return_amt) AS returns1, 
                  sum(wr_net_loss)   AS profit_loss 
         FROM     wr, 
                  d, 
                  wp 
         WHERE    wr_returned_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND (cast('2001-08-16' AS date) + INTERVAL '30' day) 
         AND      wr_web_page_sk = wp_web_page_sk 
         GROUP BY wp_web_page_sk) 
SELECT
         channel , 
         id , 
         sum(sales)   AS sales , 
         sum(returns1) AS returns1 , 
         sum(profit)  AS profit 
FROM     ( 
                   SELECT    'store channel' AS channel , 
                             ssCTE.s_store_sk   AS id , 
                             sales , 
                             COALESCE(returns1, 0)               AS returns1 , 
                             (profit - COALESCE(profit_loss,0)) AS profit 
                   FROM      ssCTE 
                   LEFT JOIN srCTE 
                   ON        ssCTE.s_store_sk = srCTE.s_store_sk 
                   UNION ALL 
                   SELECT 'catalog channel' AS channel , 
                          cs_call_center_sk AS id , 
                          sales , 
                          returns1 , 
                          (profit - profit_loss) AS profit 
                   FROM   csCTE
                   LEFT JOIN crCTE
                   ON       csCTE.cs_call_center_sk = crCTE.cr_call_center_sk
                   UNION ALL 
                   SELECT    'web channel'     AS channel , 
                             wsCTE.wp_web_page_sk AS id , 
                             sales , 
                             COALESCE(returns1, 0)                  returns1 , 
                             (profit - COALESCE(profit_loss,0)) AS profit 
                   FROM      wsCTE 
                   LEFT JOIN wrCTE 
                   ON        wsCTE.wp_web_page_sk = wrCTE.wp_web_page_sk) x 
GROUP BY rollup (channel, id) 
ORDER BY channel, 
         id 
LIMIT 100; 
