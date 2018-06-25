package com.yoho.bigdata.spark.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.yoho.bigdata.spark.listener.MaliciousDetectStreamingListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.yoho.bigdata.spark.constant.TopicConst;
import com.yoho.bigdata.spark.handler.DataHandler;
import com.yoho.bigdata.spark.model.EventMessage;
import com.yoho.bigdata.spark.model.parser.MessageParser;
import com.yoho.bigdata.spark.utils.AppConsts;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 恶意防刷分析
 * Created by zrow.li on 2017/3/29.
 */
public class MaliciousDetectExecutor {
    private static final Logger logger = LoggerFactory.getLogger(MaliciousDetectExecutor.class);

    public static void main(String[] args) throws IOException {
        int duration = AppConsts.BATCH_DURATION_02S;
        logger.info("args.length:{}", args.length);
        if (args.length == 1) {
            logger.info("arg:{}", args[0]);
            duration = Integer.valueOf(StringUtils.trim(args[0]));
        }

        JavaStreamingContext streamingContext = getStreamContext(duration);
        streamingContext.addStreamingListener(new MaliciousDetectStreamingListener(duration));
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            logger.warn("malicious catch exception: {}", e);
            streamingContext.stop(true, true);
        }
    }

    private static JavaStreamingContext getStreamContext(int duration) {
        SparkConf sparkConf = new SparkConf().setAppName("MaliciousDetecter" + duration).set("spark.streaming.stopGracefullyOnShutdown", "true");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf,
                Durations.seconds(duration));

        Map<String, String> kafkaParamMap = new HashMap<String, String>();
        kafkaParamMap.put("bootstrap.servers", "ops.kafka.yohoops.org:9092");
        kafkaParamMap.put("fetch.message.max.bytes", "5242880");
        kafkaParamMap.put("group.id", appendTimeDimension("malicious", duration));
        HashSet<String> topics = Sets.newHashSet(TopicConst.GATE_TOPIC, TopicConst.UIC_TOPIC);
        //重新分区为6，uic的消息很少将topic 分区减少为2
        JavaDStream<EventMessage> dStream = KafkaUtils.createDirectStream(streamingContext,
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
                                logger.info("the message parse is null, {}", tuple2._2());
                            }
                        } catch (Exception ex) {
                            logger.warn("{}", ex);
                        }
                    }
                    return iterable.iterator();
                });

        DataHandler handler = new DataHandler(streamingContext.sparkContext(),
                duration, AppConsts.getTimeDension(duration));
        handler.handle(dStream);

        return streamingContext;

    }

    private static String appendTimeDimension(String suffix, int duration) {
        StringBuilder builder = new StringBuilder();
        builder.append(suffix);
        builder.append(duration + "");
        return builder.toString();
    }
}
