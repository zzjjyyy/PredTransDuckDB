SELECT  *
from
 (SELECT count(*) h8_30_to_9
 from ss, hd, t, s
 where ss_sold_time_sk = t.t_time_sk   
     and ss_hdemo_sk = hd.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and t.t_hour = 8
     and t.t_minute >= 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2)) 
     and s.s_store_name = 'ese') s1,
 (SELECT count(*) h9_to_9_30 
 from ss, hd, t, s
 where ss_sold_time_sk = t.t_time_sk
     and ss_hdemo_sk = hd.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and t.t_hour = 9 
     and t.t_minute < 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2))
     and s.s_store_name = 'ese') s2,
 (SELECT count(*) h9_30_to_10 
 from ss, hd, t, s
 where ss_sold_time_sk = t.t_time_sk
     and ss_hdemo_sk = hd.hd_demo_sk
     and ss_store_sk = s_store_sk
     and t.t_hour = 9
     and t.t_minute >= 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2))
     and s.s_store_name = 'ese') s3,
 (SELECT count(*) h10_to_10_30
 from ss, hd , t, s
 where ss_sold_time_sk = t.t_time_sk
     and ss_hdemo_sk = hd.hd_demo_sk
     and ss_store_sk = s_store_sk
     and t.t_hour = 10 
     and t.t_minute < 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2))
     and s.s_store_name = 'ese') s4,
 (SELECT count(*) h10_30_to_11
 from ss, hd , t, s
 where ss_sold_time_sk = t.t_time_sk
     and ss_hdemo_sk = hd.hd_demo_sk
     and ss_store_sk = s_store_sk
     and t.t_hour = 10 
     and t.t_minute >= 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2))
     and s.s_store_name = 'ese') s5,
 (SELECT count(*) h11_to_11_30
 from ss, hd , t, s
 where ss_sold_time_sk = t.t_time_sk
     and ss_hdemo_sk = hd.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and t.t_hour = 11
     and t.t_minute < 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2))
     and s.s_store_name = 'ese') s6,
 (SELECT count(*) h11_30_to_12
 from ss, hd , t, s
 where ss_sold_time_sk = t.t_time_sk
     and ss_hdemo_sk = hd.hd_demo_sk
     and ss_store_sk = s_store_sk
     and t.t_hour = 11
     and t.t_minute >= 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2))
     and s.s_store_name = 'ese') s7,
 (SELECT count(*) h12_to_12_30
 from ss, hd , t, s
 where ss_sold_time_sk = t.t_time_sk
     and ss_hdemo_sk = hd.hd_demo_sk
     and ss_store_sk = s_store_sk
     and t.t_hour = 12
     and t.t_minute < 30
     and ((hd.hd_dep_count = -1 and hd.hd_vehicle_count<=-1+2) or
          (hd.hd_dep_count = 2 and hd.hd_vehicle_count<=2+2) or
          (hd.hd_dep_count = 3 and hd.hd_vehicle_count<=3+2))
     and s.s_store_name = 'ese') s8;
