#!/bin/bash
# 执行hive命令，添加表分区，以导入hdfs文件到hive分区
# 每小时30分执行
cur_datetime=$(date +%Y%m%d%H)
hive -e "use covid19count;alter table t_infectcount add if not exists partition (partdate = '${cur_datetime}');"