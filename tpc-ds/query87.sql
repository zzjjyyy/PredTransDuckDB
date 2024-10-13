SELECT count(*) 
from ((SELECT distinct c_last_name, c_first_name, d_date
       from ss, d, c
       where ss.ss_sold_date_sk = d.d_date_sk
         and ss.ss_customer_sk = c.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (SELECT distinct c_last_name, c_first_name, d_date
       from cs, d, c
       where cs.cs_sold_date_sk = d.d_date_sk
         and cs.cs_bill_customer_sk = c.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (SELECT distinct c_last_name, c_first_name, d_date
       from ws, d, c
       where ws.ws_sold_date_sk = d.d_date_sk
         and ws.ws_bill_customer_sk = c.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
) cool_cust;

