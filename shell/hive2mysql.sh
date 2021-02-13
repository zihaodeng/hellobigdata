#!/bin/bash
# 执行sqoop命令 将数据从hive表里导入到mysql
sqoop --options-file /opt/shell/covid19/exportdata.opt >> /opt/shell/covid19/execresult/exportdata.txt