#!/bin/bash
# 计算：汇总统计各国家区域的数量
hive -f /opt/shell/covid19/statistics.hql >> /opt/shell/covid19/execresult/statistics.txt