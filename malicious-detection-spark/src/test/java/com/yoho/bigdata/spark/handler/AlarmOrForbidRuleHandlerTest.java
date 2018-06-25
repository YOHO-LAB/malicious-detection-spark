package com.yoho.bigdata.spark.handler;

import com.yoho.bigdata.spark.handler.handler.assist.AlarmOrForbidRuleHandler;
import com.yoho.bigdata.spark.model.UserIPDetailStat;
import com.yoho.bigdata.spark.config.AlarmRuleProperties;
import com.yoho.bigdata.spark.config.AllProperties;
import com.yoho.bigdata.spark.config.PropertiesContext;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import junit.framework.TestCase;
import ognl.Ognl;
import ognl.OgnlException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mvel2.MVEL;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.MapVariableResolverFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/29.
 */
public class AlarmOrForbidRuleHandlerTest extends TestCase implements Serializable {

    private transient JavaSparkContext jsc;

    private AllProperties allProperties;

    private AlarmOrForbidRuleHandler alarmOrForbidRuleHandler;

    private UserIPDetailStat stat;

    private PropertiesContext pc;

    @BeforeClass
    public void setUp() {
        SparkConf sparkConf = new SparkConf().setAppName("RedisContextTest");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.testing.memory", "536870912");
        jsc = new JavaSparkContext(sparkConf);

        allProperties = new AllProperties();
        AlarmRuleProperties alarmRuleProperties = new AlarmRuleProperties();
        List<String> yohoAlarmRule = new ArrayList();
        yohoAlarmRule.add("qps>30&&impApiPrecent>90");
        yohoAlarmRule.add("udidCount>30");
        alarmRuleProperties.setYoho(yohoAlarmRule);
        allProperties.setAlarmRule(alarmRuleProperties);

        jsc.broadcast(allProperties);
        pc = new PropertiesContext(jsc, 2,allProperties);
        alarmOrForbidRuleHandler = new AlarmOrForbidRuleHandler(pc);

        stat = new UserIPDetailStat();
        stat.setUidCount(50);
        stat.setUdidCount(50);
        stat.setDeviceTypeCount(1);
        stat.setNotExistUdidPercent(20);
        stat.setNotExsitUdidCount(1);
        stat.setQps(50);
        stat.setDifMethodCount(5);
        stat.setImpApiPrecent(91);
        stat.setImpCount(5);
        stat.setWhiteIpFlag(false);
        stat.setIpPrecent(1);

    }


    @AfterClass
    public void tearDown() {
        jsc.stop();
        jsc = null;
    }


    @Test
    public void testRule() {


        long l = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            alarmOrForbidRuleHandler.meetYohoAlarmRule(stat);
        }
        System.out.println(System.currentTimeMillis() - l);

    }


    @Test
    public void testRuleGroovy() {
        List<String> yohoAlarmRules = pc.getAlarmRules().getYoho();
        long l = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {

            Binding binding = new Binding();
            binding.setProperty("qps", stat.getQps());
            binding.setProperty("difMethodCount", stat.getDifMethodCount());
            binding.setProperty("impCount", stat.getImpCount());
            binding.setProperty("impApiPrecent", stat.getImpApiPrecent());
            binding.setProperty("uidCount", stat.getUdidCount());
            binding.setProperty("ipPrecent", stat.getIpPrecent());
            binding.setProperty("udidCount", stat.getUdidCount());
            binding.setProperty("notExsitUdidCount", stat.getNotExsitUdidCount());
            binding.setProperty("notExistUdidPercent", stat.getNotExistUdidPercent());

            GroovyShell shell = new GroovyShell(binding);

            for (String rule : yohoAlarmRules) {
                boolean eval = (Boolean) shell.evaluate(rule);
                if (eval) {
                }
            }
//            alarmOrForbidRuleHandler.meetYohoAlarmRule(stat);
        }
        System.out.println(System.currentTimeMillis() - l);

    }


    @Test
    public void testRuleOgnl() {
        List<String> yohoAlarmRules = pc.getAlarmRules().getYoho();
        long l = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {

            Map<String, Object> context = new HashMap<String, Object>();
            context.put("qps", stat.getQps());
            context.put("difMethodCount", stat.getDifMethodCount());
            context.put("impCount", stat.getImpCount());
            context.put("impApiPrecent", stat.getImpApiPrecent());
            context.put("uidCount", stat.getUdidCount());
            context.put("ipPrecent", stat.getIpPrecent());
            context.put("udidCount", stat.getUdidCount());
            context.put("notExsitUdidCount", stat.getNotExsitUdidCount());
            context.put("notExistUdidPercent", stat.getNotExistUdidPercent());

            for (String rule : yohoAlarmRules) {
                boolean eval = false;
                try {
                    eval = (Boolean) Ognl.getValue(rule, context);
                    System.out.println(rule+"==>"+eval);
                    if (eval) {

                    }
                } catch (OgnlException e) {
                    e.printStackTrace();
                }
            }
//            alarmOrForbidRuleHandler.meetYohoAlarmRule(stat);
        }
        System.out.println(System.currentTimeMillis() - l);
    }


    @Test
    public void testRuleMvel() {
        List<String> yohoAlarmRules = pc.getAlarmRules().getYoho();
        long l = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {

            Map<String, Object> context = new HashMap<String, Object>();
            context.put("qps", stat.getQps());
            context.put("difMethodCount", stat.getDifMethodCount());
            context.put("impCount", stat.getImpCount());
            context.put("impApiPrecent", stat.getImpApiPrecent());
            context.put("ipPrecent", stat.getIpPrecent());
            context.put("udidCount", stat.getUdidCount());
            context.put("notExsitUdidCount", stat.getNotExsitUdidCount());
            context.put("notExistUdidPercent", stat.getNotExistUdidPercent());
            VariableResolverFactory functionFactory = new MapVariableResolverFactory(context);
            for (String rule : yohoAlarmRules) {
                boolean eval = false;
                eval = (Boolean) MVEL.eval(rule, functionFactory);
                System.out.println(rule+"==>"+eval);
                if (eval) {

                }

            }
//            alarmOrForbidRuleHandler.meetYohoAlarmRule(stat);
        }
        System.out.println(System.currentTimeMillis() - l);
    }


}
