package com.yoho.bigdata.spark.handler.handler;

import java.io.Serializable;

import com.yoho.bigdata.spark.utils.ImpApiContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.handler.handler.assist.AlarmOrForbidRuleHandler;
import com.yoho.bigdata.spark.handler.handler.assist.ReporterMaliciousIpHandler;
import com.yoho.bigdata.spark.handler.handler.assist.WhiteIpHandler;
import com.yoho.bigdata.spark.model.EventMessage;

/**
 * 聚合基类
 * Created by zrow.li on 2017/3/30.
 */
public abstract class DimensionHandler implements Serializable {

    protected int timeDimension;

    protected ReporterMaliciousIpHandler reporterMaliciousIpHandler;

    protected WhiteIpHandler whiteIpHandler;

    protected AlarmOrForbidRuleHandler alarmOrForbidRuleHandler;

    protected ImpApiContext impApiContext;

    public DimensionHandler(PropertiesContext pc,ImpApiContext context) {
        this.impApiContext=context;
        reporterMaliciousIpHandler = new ReporterMaliciousIpHandler(pc);
        whiteIpHandler = new WhiteIpHandler(pc);
        alarmOrForbidRuleHandler = new AlarmOrForbidRuleHandler(pc);
    }


    public void handle(JavaDStream dStream) {
        // 先按统计维度做聚合
        JavaDStream pair = aggregate(filter(dStream));
        statAndPush(pair);
    }

    protected int getTimeDimension() {
        return this.timeDimension;
    }

    protected abstract JavaDStream aggregate(JavaDStream dStream);

    /**
     * 统计&推送
     *
     * @param dStream
     */
    protected abstract void statAndPush(JavaDStream dStream);

    /**
     * 过滤
     *
     * @param dStream
     * @return
     */
    protected abstract JavaDStream filter(JavaDStream<EventMessage> dStream);

}
