#!/bin/bash
# 5m统计ip的指标 ===> 计算异常ip的特征值
#
source /etc/profile
source ~/.bash_profile
source ~/.bashrc

if [ $# -ge 1 ]
then
    dateid=$1
else
    dateid="`date +\%Y\%m\%d\%H\%M`"
fi
echo $((dateid%5))
_dateid=$((dateid%5))
P_DATA=$((dateid-_dateid))
echo $P_DATA
P_YEAR=${P_DATA:0:4}
P_MONTH=${P_DATA:4:2}
P_DAY=${P_DATA:6:2}
P_HOUR=${P_DATA:8:2}
P_MINUTE=${P_DATA:10:2}
hour=$P_YEAR$P_MONTH$P_DAY$P_HOUR
minute=$P_MINUTE
echo $hour
echo $minute
file_home=/home/hadoop/malicious/mid_monitor_checkIp_5m/riskdata


OUTPUT_PATH=/mid_monitor_checkIp/$hour/$minute
hadoop fs -mkdir -p $OUTPUT_PATH

if hadoop fs -test -e ${OUTPUT_PATH}
then
    hadoop fs -rm -r ${OUTPUT_PATH} 
fi
    hadoop fs -mkdir ${OUTPUT_PATH}


hive -e "alter table mid_riskcontrol.mid_monitor_ip_5m ADD IF NOT EXISTS PARTITION (hour='$hour',minute='$minute')" 
echo "partition is generate successfully" 

rm -f $file_home/riskdataIp* 

hive -e " set hive.strict.checks.cartesian.product=false;select ip,count/allcount,qps,udidcount,uidcount,difmethodcount,impcount,notexsitudidcount,imppercent,uidcount_udidcount,udidcount_uidcount,allcountpercent,minute from
(select * from mid_riskcontrol.mid_monitor_ip_5m where whiteIpFlag = false and hour='$hour' and minute='$minute') a 
join 
(select sum(count) allcount from mid_riskcontrol.mid_monitor_ip_5m where hour='$hour' and minute='$minute') b" >> $file_home/riskdataIp.txt

if [ ! -f "$file_home/riskdataIp.txt" ]; then 
   echo "not create riskdataIp file"
   exit 0
fi

python /home/hadoop/malicious/mid_monitor_checkIp_5m/professional_riskdata_dealing.py $file_home/

if [ ! -f "$file_home/riskdataIp_res0.json" ]; then 
   echo "not create result file"
   exit 0
fi 

res1=`cat $file_home/riskdataIp_res*`
echo $res1

curlresult=`curl -H "Content-Type: application/json" -X POST  -d "${res1}"  http://malicious.yohops.com/outer/writeMipsInfoToOpsDb?cloud=qcloud`
echo $curlresult

hadoop fs -put $file_home/riskdataIp_res* $OUTPUT_PATH/

exit 0

