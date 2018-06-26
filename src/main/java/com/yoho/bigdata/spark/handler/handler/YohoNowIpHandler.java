package com.yoho.bigdata.spark.handler.handler;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.constant.TopicConst;
import com.yoho.bigdata.spark.model.EventMessage;
import com.yoho.bigdata.spark.model.MediaIpStat;
import com.yoho.bigdata.spark.model.ReportBo;
import com.yoho.bigdata.spark.model.UserIPStat;
import com.yoho.bigdata.spark.utils.ImpApiContext;
import com.yoho.bigdata.spark.utils.MathUtil;

/**
 * 描述:mars  & now  恶意IP处理器
 * Created by pangjie@yoho.cn on 2017/8/22.
 */
public class YohoNowIpHandler extends GatewayIpHandler {

    public YohoNowIpHandler(PropertiesContext pc,ImpApiContext impApiContext, int timeDimension) {
        super(pc, impApiContext,timeDimension);
    }

    @Override
    protected void statAndPush(JavaDStream dStream) {
        JavaDStream<MediaIpStat> userIPDetail = getMediaIpStat(dStream);

        //过滤不在白名单中和符合规则的
        JavaDStream<ReportBo> maliciousIps = userIPDetail.filter(stat ->
                !stat.isWhiteIpFlag() && alarmOrForbidRuleHandler.meetMediaForbidRule(stat)
        ).map(stat -> ReportBo.builder().ip(stat.getIp()).duration(timeDimension).reason(JSON.toJSONString(stat)).build());

        //上报
        maliciousIps.foreachRDD((VoidFunction<JavaRDD<ReportBo>>) stringJavaRDD -> {
            stringJavaRDD.foreachPartition((VoidFunction<Iterator<ReportBo>>) stringIterator -> reporterMaliciousIpHandler.reportForbidIp(stringIterator));
        });
    }


    private JavaDStream<MediaIpStat> getMediaIpStat(JavaDStream<UserIPStat> dStream) {
    	 //获取api列表
        final Set<String> redisApliList= impApiContext.getApiList();
        return dStream.map(ipStat -> {
            List<EventMessage> messages = ipStat.getMessages();
            MediaIpStat mediaIpStat = new MediaIpStat();
            mediaIpStat.setIp(ipStat.getUserIP());
            HashSet<String> udidSet = new HashSet<>();
            HashSet<String> uidSet = new HashSet<>();
            Map<String, Integer> methodMap = Maps.newHashMap();
            Set<String> deviceSet = new HashSet<>();
            Set<String> methodSet = new HashSet<>();
            long impCount = 0;
            for (EventMessage message : messages) {
                if (message == null) {
                    continue;
                }
                // 判断是否为敏感接口
                if (impApiContext.isImpAPI(redisApliList,message.getRequestMethod())) {
                    ++impCount;
                    methodMap.put(message.getRequestMethod(), methodMap.getOrDefault(message.getRequestMethod(), 0) + 1);
                }
                methodSet.add(message.getRequestMethod());
                uidSet.add(message.getUid());
                deviceSet.add(message.getUserAgentInfo());
                String udid = message.getMediaUdid();
                udidSet.add(udid);
            }
            mediaIpStat.setWhiteIpFlag(whiteIpHandler.isWhiteList(ipStat.getUserIP()));
            mediaIpStat.setQps(messages.size());
            mediaIpStat.setDifMethodCount(methodSet.size());
            mediaIpStat.setMethodDetail(methodMap);
            mediaIpStat.setImpCount(impCount);
            mediaIpStat.setImpApiPrecent(MathUtil.precent(mediaIpStat.getImpCount(), mediaIpStat.getQps()));
            mediaIpStat.setUidCount(uidSet.size());
            mediaIpStat.setUdidCount(udidSet.size());
            mediaIpStat.setIpPrecent(ipStat.getPercent());
            mediaIpStat.setDeviceTypeCount(deviceSet.size());
            return mediaIpStat;
        });
    }


    @Override
    protected JavaDStream filter(JavaDStream<EventMessage> dStream) {
        return dStream.filter(o -> o.getTopic().equals(TopicConst.MEDIA_TOPIC));
    }
}
