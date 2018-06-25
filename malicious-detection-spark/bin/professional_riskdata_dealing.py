# coding:utf-8
# 读取数据
# hive -e"
# set mapreduce.job.queuename=theme;
# select
# ip,
# ipPrecent,
# qps,
# udidcount,
# uidcount,
# difmethodcount,
# impcount,
# notexsitudidcount,
# imppercent,
# uidcount_udidcount,
# udidcount_uidcount,
# allcountpercent,
#minute
# from mid_riskcontrol.mid_monitor_ip_5m
# where whiteIpFlag = false
# and concat(year,month,day,hour,minute) = '201712130100'
# ">>riskdata121300.txt
#    
###########执行过程
# 数据读取
import risk_control as rc
import pandas as pd
import sys

path = sys.argv[1]
# data_all = pd.read_table('/Users/slade/Documents/Yoho/hdfs_data/riskdata121300.txt', header=None)
data_all = pd.read_table(path+'riskdataIp.txt', header=None)
data_all.columns = ["ip","ipprecent","qps","udidcount","uidcount","difmethodcount","impcount","notexsitudidcount","imppercent","uidcount_udidcount","udidcount_uidcount","allcountpercent", "minutes"]
data_all = data_all[["ip","ipprecent","qps","udidcount","uidcount","difmethodcount","impcount","notexsitudidcount","imppercent","uidcount_udidcount","udidcount_uidcount","allcountpercent", "minutes"]]
train_data_res, start_min, end_min ,train_data_res_backup = rc.break_user(data_all)
outlier_dict, heavy_point, train_data_res = rc.train(train_data_res=train_data_res)
ip = rc.ip_search(outlier_dict=outlier_dict, train_data_res=train_data_res)
ip_exception_res = rc.ip_exception(train_data_res_backup)
ip_res = rc.match_data(ip,ip_exception_res,train_data_res_backup)
rc.datasave(ip_res=ip_res, start_min=start_min, end_min=end_min,path=path)
