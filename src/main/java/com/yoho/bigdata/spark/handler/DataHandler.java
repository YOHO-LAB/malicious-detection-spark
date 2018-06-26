package com.yoho.bigdata.spark.handler;

import java.io.Serializable;

import com.yoho.bigdata.spark.utils.ImpApiContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.handler.handler.DimensionHandler;
import com.yoho.bigdata.spark.handler.handler.GatewayIpHandler;
import com.yoho.bigdata.spark.handler.handler.UicIpHandler;
import com.yoho.bigdata.spark.handler.handler.YohoNowIpHandler;
import com.yoho.bigdata.spark.model.EventMessage;

/**
 * 数据分析类
 * Created by zrow.li on 2017/3/29.
 */
public final class DataHandler implements Serializable {

    private int timeDimension;

    private int duration;

    private JavaSparkContext jsc;

    public DataHandler(JavaSparkContext jsc,int duration, int timeDimension) {
        this.jsc = jsc;
        this.duration=duration;
        this.timeDimension = timeDimension;
    }

    public void handle(JavaDStream<EventMessage> dStream) {
        // 清洗数据
        dStream.cache();

        PropertiesContext pc = new PropertiesContext(jsc,duration);
        ImpApiContext impApiContext=new ImpApiContext(jsc);
        DimensionHandler handler = new GatewayIpHandler(pc, impApiContext,timeDimension);
        handler.handle(dStream);

        DimensionHandler uicIpHandler = new UicIpHandler(pc,impApiContext, timeDimension);
        uicIpHandler.handle(dStream);

        DimensionHandler mediaIpHandler = new YohoNowIpHandler(pc,impApiContext, timeDimension);
        mediaIpHandler.handle(dStream);
    }

}
