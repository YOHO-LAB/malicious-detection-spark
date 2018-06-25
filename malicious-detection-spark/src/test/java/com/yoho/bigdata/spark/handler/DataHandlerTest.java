package com.yoho.bigdata.spark.handler;

import com.yoho.bigdata.spark.utils.AppConsts;
import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Serializable;

/**
 *
 * Created by zrow.li on 2017/3/29.
 */
public class DataHandlerTest extends TestCase implements Serializable {
    private transient JavaStreamingContext jssc;

    @BeforeClass
    public void setUp() {
        SparkConf sparkConf = new SparkConf().setAppName("DataHandlerTest");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.testing.memory", "536870912");
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(AppConsts.SLIDE_DURATION));
//        jssc.checkpoint("/fs/mutest");
    }

    @AfterClass
    public void tearDown() {
        try {
            Thread.sleep(1000000);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        jssc.stop();
        jssc = null;
    }

//    @Test
//    public void testDataHandler() {
//        List<EventMessage> list = prepareMessage();
//        JavaRDD<EventMessage> rdd = jssc.sparkContext().parallelize(list);
//        Queue<JavaRDD<EventMessage>> queue = new LinkedBlockingQueue<>();
//        queue.add(rdd);
//        JavaDStream<EventMessage> dstream = jssc.queueStream(queue);
//        jssc.sparkContext().emptyRDD();
//
//        dstream.print();
//        DataHandler handler = new DataHandler(jssc.sparkContext(),2,2);
//        handler.handle(dstream);
//
//        jssc.start();
//
//    }

    private int getTimeDension(int duration) {
        if (duration == 30) {
            return AppConsts.TIME_DIMENSION_30S;
        } else {
            return AppConsts.TIME_DIMENSION_02S;
        }
    }

//    private List<EventMessage> prepareMessage() {
//        List<EventMessage> list = Lists.newArrayList();
//        String localIP = "192.168.1.1";
//        String curTime = "20170329175758";
//        Map map1=new HashMap();
//        map1.put("a","1");
//        map1.put("b","2");
//
//        Map map2=new HashMap();
//        map1.put("a","1");
//        map1.put("b","2");
//
//        list.add(new EventMessage(localIP, "1.2.3.4", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c98 )", "user.instalment.getBankCards", map1, "200", 30, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.4", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c97 )", "/promt", map2, "200", 31, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.7", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c99 )", "/promt", map2,
//                "200", 31, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.4", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c91 )", "/order", map2,
//                "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.5", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c98 )", "user.instalment.getBankCards", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.4", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c98 )", "/order", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.4", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c98 )", "/order", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.6", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c98 )", "user.instalment.getBankCards", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.6", curTime, "get", "user-agent", "/order", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.6", curTime, "get", "user-agent", "/order", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.6", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c98 )", "app.passport.signinAES", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.6", curTime, "get", "user-agent", "app.passport.signinAES", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.6", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c95 )", "app.passport.signinAES",
//                map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.6", curTime, "get", "user-agent", "/order", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.8", curTime, "get", "user-agent", "user.instalment.getBankCards", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.8", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c97 )", "app.passport.signinAES",
//                map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.8", curTime, "get", "YOHO!Buy/5.3.1.326 ( Model/MI 3;OS/4.4.4;" +
//                "Channel/2937;Resolution/1920*1080;udid/8603110241280196128ba470569c98 )", "app.passport.signinAES", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.8", curTime, "get", "user-agent", "app.passport.signinAES", map2, "200", 32, TopicConst.GATE_TOPIC));
//        list.add(new EventMessage(localIP, "1.2.3.9", curTime, "get", "user-agent", "app.passport.signinAES",
//                map2, "200", 32, TopicConst.GATE_TOPIC));
//        return list;
//    }
}
