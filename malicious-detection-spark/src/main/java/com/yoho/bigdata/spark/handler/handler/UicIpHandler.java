package com.yoho.bigdata.spark.handler.handler;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.yoho.bigdata.spark.utils.ImpApiContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.alibaba.fastjson.JSON;
import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.constant.TopicConst;
import com.yoho.bigdata.spark.model.EventMessage;
import com.yoho.bigdata.spark.model.ReportBo;
import com.yoho.bigdata.spark.model.UicIpStat;
import com.yoho.bigdata.spark.utils.MathUtil;

import scala.Tuple2;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/22.
 */
public class UicIpHandler extends DimensionHandler {


    public UicIpHandler(PropertiesContext pc, ImpApiContext impApiContext, int dimension) {
        super(pc,impApiContext);
        this.timeDimension = dimension;
    }

    @Override
    protected JavaDStream aggregate(JavaDStream dStream) {
        JavaDStream<EventMessage> inputDStream = dStream;

        JavaPairDStream<String, EventMessage> ipPairDStream=inputDStream.mapPartitionsToPair(new PairFlatMapFunction<Iterator<EventMessage>, String, EventMessage>() {
            @Override
            public Iterator<Tuple2<String, EventMessage>> call(Iterator<EventMessage> eventMessageIterator) throws Exception {
                List<Tuple2<String, EventMessage>> tuple2List= Lists.newArrayList();
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

        JavaPairDStream<String, Iterable<EventMessage>> ipDStream = ipPairDStream.groupByKey();

        return ipDStream.map((Function<Tuple2<String, Iterable<EventMessage>>, UicIpStat>) ipTuple2 -> {
            Iterable<EventMessage> eventMessages = ipTuple2._2();
            Iterator<EventMessage> iterator = eventMessages.iterator();
            int total = 0;
            int success = 0;
            while (iterator.hasNext()) {
                total++;
                EventMessage next = iterator.next();
                if (next.getHttpStatus().equals("200")) {
                    success++;
                }
            }

            int fail = total - success;
            UicIpStat uicIpStat = new UicIpStat();
            uicIpStat.setIp(ipTuple2._1());
            uicIpStat.setQps(total);
            uicIpStat.setSucc(success);
            uicIpStat.setFail(fail);
            uicIpStat.setFailPercent(MathUtil.precent(fail, total));
            uicIpStat.setWhiteIpFlag(whiteIpHandler.isWhiteList(ipTuple2._1()));
            return uicIpStat;
        });
    }

    @Override
    protected void statAndPush(JavaDStream dStream) {
        JavaDStream<UicIpStat> inputStream = dStream;
        //过滤不在白名单中和符合规则的
        JavaDStream<ReportBo> maliciousIps = inputStream.filter(stat ->
                !stat.isWhiteIpFlag() && alarmOrForbidRuleHandler.meetUicForbidRule(stat)
        ).map(stat -> ReportBo.builder().ip(stat.getIp()).reason(JSON.toJSONString(stat)).duration(timeDimension).build());

        //上报
        maliciousIps.foreachRDD((VoidFunction<JavaRDD<ReportBo>>) stringJavaRDD -> {
            stringJavaRDD.foreachPartition((VoidFunction<Iterator<ReportBo>>) reportBoIterator -> reporterMaliciousIpHandler.reportForbidIp(reportBoIterator));
        });
    }

    @Override
    protected JavaDStream filter(JavaDStream<EventMessage> dStream) {
        return dStream.filter(o -> o.getTopic().equals(TopicConst.UIC_TOPIC));
    }
}
