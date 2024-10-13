WITH ss_items 
     AS (SELECT i_item_id AS item_id, 
                Sum(ss_ext_sales_price) AS ss_item_rev 
         FROM   ss, 
                i, 
                d 
         WHERE  ss_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   d 
                               WHERE  d_week_seq = (SELECT d_week_seq 
                                                    FROM   d 
                                                    WHERE  d_date = '2002-02-25' 
                                                   )) 
                AND ss_sold_date_sk = d_date_sk 
         GROUP  BY i_item_id), 
     cs_items 
     AS (SELECT i_item_id AS item_id, 
                Sum(cs_ext_sales_price) AS cs_item_rev 
         FROM   cs, 
                i, 
                d 
         WHERE  cs_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   d 
                               WHERE  d_week_seq = (SELECT d_week_seq 
                                                    FROM   d 
                                                    WHERE  d_date = '2002-02-25' 
                                                   )) 
                AND cs_sold_date_sk = d_date_sk 
         GROUP  BY i_item_id), 
     ws_items 
     AS (SELECT i_item_id               item_id, 
                Sum(ws_ext_sales_price) ws_item_rev 
         FROM   ws, 
                i, 
                d 
         WHERE  ws_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   d 
                               WHERE  d_week_seq = (SELECT d_week_seq 
                                                    FROM   d 
                                                    WHERE  d_date = '2002-02-25' 
                                                   )) 
                AND ws_sold_date_sk = d_date_sk 
         GROUP  BY i_item_id) 
SELECT ss_items.item_id, 
               ss_item_rev, 
               ss_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 
               100 ss_dev, 
               cs_item_rev, 
               cs_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 
               100 cs_dev, 
               ws_item_rev, 
               ws_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 
               100 ws_dev, 
               ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 
               average 
FROM   ss_items, 
       cs_items, 
       ws_items 
WHERE  ss_items.item_id = cs_items.item_id 
       AND ss_items.item_id = ws_items.item_id 
       AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev 
       AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev 
       AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev 
       AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev 
       AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev 
       AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev 
ORDER  BY ss_items.item_id, 
          ss_item_rev
LIMIT 100; 
