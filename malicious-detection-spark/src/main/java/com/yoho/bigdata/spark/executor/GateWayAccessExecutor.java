package com.yoho.bigdata.spark.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yoho.bigdata.spark.utils.UdidUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.constant.TopicConst;
import com.yoho.bigdata.spark.handler.handler.assist.WhiteIpHandler;
import com.yoho.bigdata.spark.model.EventMessage;
import com.yoho.bigdata.spark.model.IpGroupStatisticsBean;
import com.yoho.bigdata.spark.model.parser.MessageParser;
import com.yoho.bigdata.spark.utils.DateUtil;
import com.yoho.bigdata.spark.utils.ImpApiContext;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 算法需要数据清洗
 */
public class GateWayAccessExecutor {

    public static final String defaultOutputPath = "hdfs:///usr/hive/warehouse/mid_riskcontrol.db/mid_monitor_ip_5m";

    public static final int duration = 5;

    public static void main(String[] args) {
        JavaStreamingContext streamingContext = getStreamContext();
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            streamingContext.stop(true, true);
        }
    }


    private static JavaStreamingContext getStreamContext() {
        SparkConf sparkConf = new SparkConf().setAppName("etl.realtime.spark_brain_gatewayaccess_res").set("spark.streaming.stopGracefullyOnShutdown", "true");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Minutes.apply(duration));

        PropertiesContext pc = new PropertiesContext(streamingContext.sparkContext(), 30);
        ImpApiContext impApiContext=new ImpApiContext(streamingContext.sparkContext());
        WhiteIpHandler whiteIpHandler = new WhiteIpHandler(pc);

        Map<String, String> kafkaParamMap = new HashMap<String, String>();
        kafkaParamMap.put("bootstrap.servers", "ops.kafka.yohoops.org:9092");
        kafkaParamMap.put("fetch.message.max.bytes", "5242880");
        kafkaParamMap.put("group.id", "spark.brain.gateway_acess_res_test");
        HashSet<String> topics = Sets.newHashSet(TopicConst.GATE_TOPIC);


        JavaDStream<EventMessage> kafkaStream = KafkaUtils.createDirectStream(streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParamMap,
                topics)
                .mapPartitions((FlatMapFunction<Iterator<Tuple2<String, String>>, EventMessage>) iterator -> {
                    List<EventMessage> iterable = new ArrayList<>();
                    while (iterator.hasNext()) {
                        Tuple2<String, String> tuple2 = iterator.next();
                        if (StringUtils.isBlank(tuple2._2())) {
                            continue;
                        }
                        try {
                            EventMessage message = MessageParser.parse(tuple2._2());
                            if (message != null) {
                                iterable.add(message);
                            } else {
                                System.out.println("can't parse -->" + tuple2._2());
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    return iterable.iterator();
                });

        //根据IP分组
        JavaPairDStream<String, EventMessage> pairDStream=kafkaStream.mapPartitionsToPair(new PairFlatMapFunction<Iterator<EventMessage>, String, EventMessage>() {
            @Override
            public Iterator<Tuple2<String, EventMessage>> call(Iterator<EventMessage> eventMessageIterator) throws Exception {
                List<Tuple2<String, EventMessage>> tuple2List=Lists.newArrayList();
                while (eventMessageIterator.hasNext()){
                    EventMessage message=eventMessageIterator.next();
                    if(null==message){
                        continue;
                    }
                    tuple2List.add(new Tuple2<>(message.getUserIP(), message));
                }
                return tuple2List.iterator();
            }
        });

        JavaPairDStream<String, Iterable<EventMessage>> ipDStream = pairDStream.groupByKey();

        //统计IP,总访问接口次数
        JavaPairDStream<String, IpGroupStatisticsBean> countByIpDStream = ipDStream.mapValues(
                (Function<Iterable<EventMessage>, IpGroupStatisticsBean>) iterable -> {
                    IpGroupStatisticsBean stat = new IpGroupStatisticsBean();
                    List<EventMessage> messages = Lists.newArrayList(iterable);
                    stat.setCount(messages.size());
                    stat.setMessageList(messages);
                    return stat;
                });

        JavaDStream<IpGroupStatisticsBean> ipGroupStatisticsBeanDStream = countByIpDStream.map(v -> {
            IpGroupStatisticsBean groupBean = v._2();
            groupBean.setIp(v._1());
            return groupBean;
        });

        JavaDStream<IpGroupStatisticsBean> groupBeanJavaDStream = ipGroupStatisticsBeanDStream.mapPartitions((FlatMapFunction<Iterator<IpGroupStatisticsBean>, IpGroupStatisticsBean>) ipGroupStatisticsBeanIterator -> {

            //获取敏感api列表
            Set<String> redisImpApiList = impApiContext.getApiList();

            List<IpGroupStatisticsBean> returnList = Lists.newArrayList();

            long timestamp = System.currentTimeMillis();
            while (ipGroupStatisticsBeanIterator.hasNext()) {
                IpGroupStatisticsBean inputBean = ipGroupStatisticsBeanIterator.next();

                IpGroupStatisticsBean returnBean = new IpGroupStatisticsBean();
                List<EventMessage> messages = inputBean.getMessageList();

                HashSet<String> udidSet = new HashSet<>();
                HashSet<String> uidSet = new HashSet<>();
                //排重设备号
                Set<String> deviceSet = new HashSet<>();
                //排重api
                Set<String> methodSet = new HashSet<>();
                Set<String> notExistUdidSet = new HashSet<>();
                //敏感消息数
                int impCount = 0;
                int iphoneClient = 0;
                int androidClient = 0;
                int webClient = 0;
                int h5Client =0;
                for (EventMessage message : messages) {
                    if (message == null) {
                        continue;
                    }
                    // 判断是否为敏感接口
                    if (impApiContext.isImpAPI(redisImpApiList, message.getRequestMethod())) {
                        ++impCount;
                    }
                    methodSet.add(message.getRequestMethod());
                    deviceSet.add(message.getUserAgentInfo());

                    if(StringUtils.isNotBlank(message.getUid()) && !"0".equals(message.getUid())){
                        uidSet.add(message.getUid());
                    }
                    udidSet.add(message.getUdid());

                    if("iphone".equals(message.getClientType())){
                        iphoneClient++;
                    }else if("android".equals(message.getClientType())){
                        androidClient++;
                    }else if("web".equals(message.getClientType())){
                        webClient++;
                    }else if("h5".equals(message.getClientType())){
                        h5Client++;
                    }
                }
                for (String udid : udidSet) {
                    boolean existUdid = UdidUtil.isExistUdid(udid);
                    if (!existUdid) {
                        notExistUdidSet.add(udid);
                    }
                }
                returnBean.setIp(inputBean.getIp());
                returnBean.setTimestamp(timestamp);
                returnBean.setWhiteIpFlag(whiteIpHandler.isWhiteList(inputBean.getIp()));
                returnBean.setCount(messages.size());
                returnBean.setQps(((float) messages.size()) / (duration * 60));
                returnBean.setUidCount(uidSet.size());
                returnBean.setUdidCount(udidSet.size());
                returnBean.setDeviceTypeCount(deviceSet.size());
                returnBean.setDifMethodCount(methodSet.size());
                returnBean.setImpCount(impCount);
                returnBean.setNotExistUdidCount(notExistUdidSet.size());
                returnBean.setImpPercent(((float) impCount) / messages.size());
                returnBean.setUdidCount_UidCount(((float) udidSet.size()) / uidSet.size());
                returnBean.setUidCount_UdidCount(((float) uidSet.size()) / udidSet.size());
                returnBean.setAllCountPercent(((float) messages.size()) / methodSet.size());
                returnBean.setIphoneClient(iphoneClient);
                returnBean.setAndroidClient(androidClient);
                returnBean.setWebClient(webClient);
                returnBean.setH5Client(h5Client);
                returnList.add(returnBean);
            }
            return returnList.iterator();
        });

        groupBeanJavaDStream.foreachRDD(new VoidFunction2<JavaRDD<IpGroupStatisticsBean>, Time>() {
            @Override
            public void call(JavaRDD<IpGroupStatisticsBean> ipGroupStatisticsBeanJavaRDD, Time time) throws Exception {

                long now = System.currentTimeMillis() / 1000;
                now = now - now % 300;
                String hour = DateUtil.getDate(now,DateUtil.FORMATTER_HOUR);
                String minute = DateUtil.getDate(now,DateUtil.FORMATTER_M);
                String path = defaultOutputPath + "/hour=" + hour + "/minute=" + minute;
                ipGroupStatisticsBeanJavaRDD.repartition(1).saveAsTextFile(path);
            }
        });
        return streamingContext;
    }
}
