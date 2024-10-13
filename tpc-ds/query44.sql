SELECT asceding.rnk, i1.i_product_name AS best_performing, i2.i_product_name AS worst_performing 
FROM (SELECT * 
      FROM (SELECT item_sk, Rank() OVER (ORDER BY rank_col ASC) AS rnk 
            FROM (SELECT ss_item_sk AS item_sk, avg(ss_net_profit) AS rank_col 
                  FROM ss1 
                  WHERE ss_store_sk = 4 
                  GROUP BY ss_item_sk 
                  HAVING Avg(ss_net_profit) > 0.9 * (SELECT Avg(ss_net_profit) AS rank_col 
                                                     FROM ss 
                                                     WHERE ss_store_sk = 4 
                                                           AND ss_cdemo_sk IS NULL 
                                                     GROUP BY ss_store_sk))V1) V11 
      WHERE rnk < 11) asceding, 
     (SELECT * 
      FROM (SELECT item_sk, Rank() OVER (ORDER BY rank_col DESC) AS rnk 
            FROM (SELECT ss_item_sk AS item_sk, avg(ss_net_profit) AS rank_col 
                  FROM ss1 
                  WHERE ss_store_sk = 4 
                  GROUP BY ss_item_sk 
                  HAVING Avg(ss_net_profit) > 0.9 * (SELECT Avg(ss_net_profit) AS rank_col 
                                                     FROM ss 
                                                     WHERE ss_store_sk = 4 
                                                     AND ss_cdemo_sk IS NULL 
                                                     GROUP BY ss_store_sk))V2) V21 
      WHERE  rnk < 11) descending, 
      i1, i2 
WHERE asceding.rnk = descending.rnk 
      AND i1.i_item_sk = asceding.item_sk 
      AND i2.i_item_sk = descending.item_sk 
ORDER  BY asceding.rnk
LIMIT 100; 
