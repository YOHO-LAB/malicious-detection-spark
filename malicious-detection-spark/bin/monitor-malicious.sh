#!/bin//bash
source /etc/profile
i21=`yarn application -list|grep -w MaliciousDetectExecutor2|awk '{print $6}'`
j21=`yarn application -list|grep -w MaliciousDetectExecutor2|awk '{print $1}'`
echo $i21
echo $j21
if [ ! $i21 ]; then
   nohup spark-submit \
    --name  MaliciousDetectExecutor2 \
    --master yarn-cluster \
    --queue hadoop \
    --class com.yoho.bigdata.spark.executor.MaliciousDetectExecutor  \
    --executor-memory 2g \
    --driver-memory 1g \
    --num-executors 2 \
    --executor-cores 4  \
    --conf  spark.locality.wait=100 \
    --conf spark.memory.fraction=0.75 \
    --conf spark.memory.storageFraction=0.2 \
    --conf  spark.default.parallelism=2 \
    --conf  spark.streaming.concurrentJobs=1 \
   /home/hadoop/malicious/malicious-detection-spark-1.0-SNAPSHOT-jar-with-dependencies.jar "2" >>/dev/null 2>&1 &
   /usr/bin/curl --request GET --url 'http://172.31.50.139:8880/monitor/recvMonitAlarmInfo?info=MaliciousDetecter2-Spark%E4%BB%BB%E5%8A%A1%E5%BC%82%E5%B8%B8%2C%E9%87%8D%E5%90%AF%E4%B8%AD......tencent&mobiles=13770920736,13915992579'  --header 'cache-control: no-cache'
   echo "start MaliciousDetecter2 ok!!!"
fi

i301=`yarn application -list|grep -w MaliciousDetectExecutor30|awk '{print $6}'`
j301=`yarn application -list|grep -w MaliciousDetectExecutor30|awk '{print $1}'`
echo $i301
echo $j301
if [ ! $i301 ]; then
   nohup spark-submit \
    --name  MaliciousDetectExecutor30 \
    --master yarn-cluster \
    --queue hadoop \
    --class com.yoho.bigdata.spark.executor.MaliciousDetectExecutor  \
    --executor-memory 2g \
    --driver-memory 1g \
    --num-executors 3 \
    --executor-cores 4  \
    --conf  spark.locality.wait=100 \
    --conf spark.memory.fraction=0.75 \
    --conf spark.memory.storageFraction=0.2 \
    --conf  spark.default.parallelism=2 \
    --conf  spark.streaming.concurrentJobs=1 \
   /home/hadoop/malicious/malicious-detection-spark-1.0-SNAPSHOT-jar-with-dependencies.jar  "30" >>/dev/null 2>&1 &
   /usr/bin/curl --request GET --url 'http://172.31.50.139:8880/monitor/recvMonitAlarmInfo?info=MaliciousDetecter30-Spark%E4%BB%BB%E5%8A%A1%E5%BC%82%E5%B8%B8%2C%E9%87%8D%E5%90%AF%E4%B8%AD......tencent&mobiles=13770920736,13915992579'  --header 'cache-control: no-cache'
   echo "start MaliciousDetecter30 ok!!!"
fi