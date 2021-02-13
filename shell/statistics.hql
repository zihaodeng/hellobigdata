-- 指定数据库
use covid19count;
-- 清空统计表 
truncate table t_infectstatistics;
-- 插入最新统计数据
-- 开启动态分区，默认是false
set hive.exec.dynamic.partition=true;
-- 开启允许所有分区都是动态的，否则必须要有静态分区才能使用。
set hive.exec.dynamic.partition.mode=nonstrict; 
-- 插入数据 指定列进行插入 partition默认select语句最末尾的列
insert into t_infectstatistics partition(cty) (id,country,area,count,cty)
select row_number() over (order by infectcountry,infectarea asc),infectcountry,infectarea,count(1),infectcountry
from t_infectcount 
group by infectcountry,infectarea;

-- 不指定列进行插入
-- insert into t_infectstatistics partition(cty) 
-- select row_number() over (order by infectcountry,infectarea asc),count(1),infectarea,infectcountry
-- from t_infectcount 
-- group by infectcountry,infectarea;