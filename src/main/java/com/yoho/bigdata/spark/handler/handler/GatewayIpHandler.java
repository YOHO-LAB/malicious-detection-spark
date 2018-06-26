package com.yoho.bigdata.spark.handler.handler;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yoho.bigdata.spark.utils.UdidUtil;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.constant.TopicConst;
import com.yoho.bigdata.spark.model.EventMessage;
import com.yoho.bigdata.spark.model.ReportBo;
import com.yoho.bigdata.spark.model.UserIPDetailStat;
import com.yoho.bigdata.spark.model.UserIPStat;
import com.yoho.bigdata.spark.utils.AppConsts;
import com.yoho.bigdata.spark.utils.ImpApiContext;
import com.yoho.bigdata.spark.utils.MathUtil;

import scala.Tuple2;


/**
 * 按用户IP维度进行统计
 * <p>
 * Created by zrow.li on 2017/3/30.
 */
public class GatewayIpHandler extends DimensionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GatewayIpHandler.class);

    public GatewayIpHandler(PropertiesContext pc,ImpApiContext impApiContext, int timeDimension) {
        super(pc,impApiContext);
        this.timeDimension = timeDimension;
    }

    @Override
    protected JavaDStream aggregate(JavaDStream dStream) {
        JavaDStream<EventMessage> inputDStream = dStream;

        //根据IP分组
        JavaPairDStream<String, EventMessage> pairDStream=inputDStream.mapPartitionsToPair(new PairFlatMapFunction<Iterator<EventMessage>, String, EventMessage>() {
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
        JavaPairDStream<String, UserIPStat> countByIpDStream = ipDStream.mapValues(
                (Function<Iterable<EventMessage>, UserIPStat>) iterable -> {
                    UserIPStat stat = new UserIPStat();
                    long timestamp = System.currentTimeMillis();
                    List<EventMessage> messages = Lists.newArrayList(iterable);
                    stat.setCount(messages.size());
                    stat.setMessages(messages);
                    stat.setTimestamp(timestamp);
                    return stat;
                }).filter((Function<Tuple2<String, UserIPStat>, Boolean>) tuple2 -> {
            // 过滤掉访问次数小的ip地址, 减少排序的数据量
            return tuple2._2().getCount() > AppConsts.getMinViews(getTimeDimension());
        });

        JavaDStream<UserIPStat> userIPStatJavaDStream = countByIpDStream.map(v -> {
            UserIPStat ipStat = v._2();
            ipStat.setUserIP(v._1());
            return ipStat;
        });
        return userIPStatJavaDStream;

    }

    @Override
    protected void statAndPush(JavaDStream dStream) {
        JavaDStream<UserIPDetailStat> detailStat = getUserIPDetail(dStream).cache();

        //1将所有数据存入hbase,不用上报（2s的任务不处理）
        if(AppConsts.getTimeDension(timeDimension) != AppConsts.TIME_DIMENSION_02S){
            detailStat.foreachRDD(stringJavaRDD ->
                    stringJavaRDD.foreachPartition(reportBoIterator -> reporterMaliciousIpHandler.insertHbaseNoReport(reportBoIterator)));
        }

        //2处理封禁规则,2s和30s都要处理
        //过滤不在白名单中和符合规则的
        JavaDStream<ReportBo> maliciousIps = detailStat.filter(stat ->
                !stat.isWhiteIpFlag() && alarmOrForbidRuleHandler.meetYohoForbidRule(stat)
        ).map(stat -> ReportBo.builder().ip(stat.getIp()).reason(JSON.toJSONString(stat)).messages(stat.getMessages())
                .timestamp(stat.getTimestamp()).duration(timeDimension).build());

        //上报
        maliciousIps.foreachRDD(stringJavaRDD -> stringJavaRDD.foreachPartition(
                reportBoIterator -> reporterMaliciousIpHandler.reportForbidIp(reportBoIterator)));

        //3处理告警规则，只处理30s的数据
        //过滤不在白名单中和符合规则的
        if(AppConsts.getTimeDension(timeDimension) != AppConsts.TIME_DIMENSION_02S){
            JavaDStream<ReportBo> maliciousOPSIps = detailStat.filter(stat ->
                    !stat.isWhiteIpFlag() && AppConsts.getTimeDension(timeDimension) != AppConsts.TIME_DIMENSION_02S
                            && alarmOrForbidRuleHandler.meetYohoAlarmRule(stat)
            ).map(stat -> ReportBo.builder().ip(stat.getIp()).reason(JSON.toJSONString(stat)).messages(stat.getMessages())
                    .timestamp(stat.getTimestamp()).duration(timeDimension).build());
            //只上报ops
            maliciousOPSIps.foreachRDD(stringJavaRDD -> stringJavaRDD.foreachPartition(
                    reportBoIterator -> reporterMaliciousIpHandler.reportAlarmIp(reportBoIterator)));
        }

    }

    @Override
    protected JavaDStream filter(JavaDStream<EventMessage> dStream) {
        return dStream.filter(o -> o.getTopic().equals(TopicConst.GATE_TOPIC));
    }

    private JavaDStream<UserIPDetailStat> getUserIPDetail(JavaDStream<UserIPStat> dStream) {
        return dStream.mapPartitions((FlatMapFunction<Iterator<UserIPStat>, UserIPDetailStat>) userIPStatIterator -> {
            List<UserIPDetailStat> userIPDetailStats = new ArrayList<UserIPDetailStat>();
            //获取api列表
            Set<String> redisApliList= impApiContext.getApiList();
            //获取登录接口列表
            Set<String> redisLoginApliList= impApiContext.getloginApiList();

            while (userIPStatIterator.hasNext()) {
                UserIPStat ipStat = userIPStatIterator.next();
                List<EventMessage> messages = ipStat.getMessages();
                UserIPDetailStat detailStat = new UserIPDetailStat();
                detailStat.setIp(ipStat.getUserIP());
                detailStat.setMessages(ipStat.getMessages());
                detailStat.setTimestamp(AppConsts.getCurrentTime());
                HashSet<String> udidSet = new HashSet<>();
                HashSet<String> uidSet = new HashSet<>();
                Map<String, Integer> methodMap = Maps.newHashMap();
                Set<String> deviceSet = new HashSet<>();
                Set<String> methodSet = new HashSet<>();
                long impCount = 0;
                int loginApiCount = 0;
                for (EventMessage message : messages) {
                    if (message == null) {
                        continue;
                    }
                    // 判断是否为敏感接口
                    if (impApiContext.isLoginAPI(redisLoginApliList,message.getRequestMethod())) {
                        ++loginApiCount;
                        methodMap.put(message.getRequestMethod(), methodMap.getOrDefault(message.getRequestMethod(), 0) + 1);
                    }
                    // 判断是否为敏感接口
                    if (impApiContext.isImpAPI(redisApliList,message.getRequestMethod())) {
                        ++impCount;
                        if (!impApiContext.isLoginAPI(redisLoginApliList,message.getRequestMethod())) {
                            methodMap.put(message.getRequestMethod(), methodMap.getOrDefault(message.getRequestMethod(), 0) + 1);
                        }
                    }
                    methodSet.add(message.getRequestMethod());
                    uidSet.add(message.getUid());
                    deviceSet.add(message.getUserAgentInfo());
                    // 判断是否为不存在的udid
                    String udid = message.getUdid();
                    udidSet.add(udid);
                    // ...
                }
                // 当udid个数是多个时, 才需要去查询是否在udid池中
                if (udidSet.size() > 2) {
                    for (String udid : udidSet) {
                        boolean existUdid = UdidUtil.isExistUdid(udid);
                        if (!existUdid) {
                            detailStat.incrNotExsitUdidCount();
                        }
                    }
                }
                detailStat.setWhiteIpFlag(whiteIpHandler.isWhiteList(ipStat.getUserIP()));
                detailStat.setQps(messages.size());
                detailStat.setDifMethodCount(methodSet.size());
                detailStat.setMethodDetail(methodMap);
                detailStat.setImpCount(impCount);
                detailStat.setLoginApiCount(loginApiCount);
                detailStat.setImpApiPrecent(MathUtil.precent(detailStat.getImpCount(), detailStat.getQps()));
                detailStat.setUidCount(uidSet.size());
                detailStat.setUdidCount(udidSet.size());
                detailStat.setNotExistUdidPercent(MathUtil.precent(detailStat.getNotExsitUdidCount(), detailStat.getUdidCount()));
                //detailStat.setIpPrecent(ipStat.getPercent());
                detailStat.setDeviceTypeCount(deviceSet.size());
                detailStat.setMessages(messages);
                userIPDetailStats.add(detailStat);
            }
            return userIPDetailStats.iterator();
        });
    }
}
