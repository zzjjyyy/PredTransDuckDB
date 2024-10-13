WITH ssCTE
     AS (SELECT i_item_id,
                Sum(ss_ext_sales_price) total_sales
         FROM   ss,
                d,
                ca,
                i
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM i
                              WHERE i_color IN ('firebrick', 'rosy', 'white'))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     csCTE
     AS (SELECT i_item_id,
                Sum(cs_ext_sales_price) total_sales
         FROM   cs,
                d,
                ca,
                i
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   i
                              WHERE  i_color IN ('firebrick', 'rosy', 'white')
                             )
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     wsCTE
     AS (SELECT i_item_id,
                Sum(ws_ext_sales_price) total_sales
         FROM   ws,
                d,
                ca,
                i
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   i
                              WHERE  i_color IN ('firebrick', 'rosy', 'white')
                             )
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id)
SELECT i_item_id,
       Sum(total_sales) AS total_sales
FROM   (SELECT *
        FROM ssCTE
        UNION ALL
        SELECT *
        FROM csCTE
        UNION ALL
        SELECT *
        FROM wsCTE) tmp1
GROUP  BY i_item_id
ORDER  BY total_sales
LIMIT 100;
