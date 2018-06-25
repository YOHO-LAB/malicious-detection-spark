package com.yoho.bigdata.spark.handler.handler.assist;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.mvel2.MVEL;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.MapVariableResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yoho.bigdata.spark.config.PropertiesContext;
import com.yoho.bigdata.spark.model.MediaIpStat;
import com.yoho.bigdata.spark.model.UicIpStat;
import com.yoho.bigdata.spark.model.UserIPDetailStat;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/25.
 */
public class AlarmOrForbidRuleHandler implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(AlarmOrForbidRuleHandler.class);

    private PropertiesContext pc;

    private List<String> yohoForbidRules;

    private List<String> uicForbidRules;

    private List<String> mediaForbidRules;

    private List<String> yohoAlarmRules;


    public AlarmOrForbidRuleHandler(PropertiesContext pc) {
        this.pc = pc;
        yohoForbidRules = pc.getForbidRule().getYoho();
        uicForbidRules = pc.getForbidRule().getUic();
        mediaForbidRules = pc.getForbidRule().getMedia();
        yohoAlarmRules = pc.getAlarmRules().getYoho();
    }


    /**
     * 满足yoho的告警规则
     *
     * @param stat
     * @return
     */
    public boolean meetYohoForbidRule(UserIPDetailStat stat) {


        if (stat == null || CollectionUtils.isEmpty(yohoForbidRules)) {
            return false;
        }
        VariableResolverFactory functionFactory = getAlarmRuleVariableResolverFactory(stat);

		for (String rule : yohoForbidRules) {
			boolean eval = (Boolean) MVEL.eval(rule, functionFactory);
			if (eval) {
				stat.setMeetForbidRule(rule);
				return true;
			}
		}
        return false;
    }


    /**
     * 满足yoho ops的告警规则
     *
     * @param stat
     * @return
     */
    public boolean meetYohoAlarmRule(UserIPDetailStat stat) {


        if (stat == null || CollectionUtils.isEmpty(yohoAlarmRules)) {
            return false;
        }
        VariableResolverFactory functionFactory = getAlarmRuleVariableResolverFactory(stat);
        for (String rule : yohoAlarmRules) {
            boolean eval = (Boolean) MVEL.eval(rule, functionFactory);
			if (eval) {
				stat.setMeetForbidRule(rule);
				return true;
			}
        }
        return false;
    }


    /**
     * 满足UIC的告警规则
     *
     * @param stat
     * @return
     */
    public boolean meetUicForbidRule(UicIpStat stat) {


        if (stat == null || CollectionUtils.isEmpty(uicForbidRules)) {
            return false;
        }

        Map<String, Object> context = new HashMap<>();
        context.put("qps", stat.getQps());
        context.put("succ", stat.getSucc());
        context.put("fail", stat.getFail());
        context.put("failPercent", stat.getFailPercent());
        VariableResolverFactory functionFactory = new MapVariableResolverFactory(context);

        for (String rule : uicForbidRules) {
            boolean eval = (Boolean) MVEL.eval(rule, functionFactory);
            if (eval) {
            		stat.setMeetForbidRule(rule);
                return true;
            }
        }
        return false;
    }


    /**
     * 满足UIC的告警规则
     *
     * @param stat
     * @return
     */
    public boolean meetMediaForbidRule(MediaIpStat stat) {

        if (stat == null || CollectionUtils.isEmpty(mediaForbidRules)) {
            return false;
        }

        Map<String, Object> context = new HashMap<>();
        context.put("qps", stat.getQps());
        context.put("difMethodCount", stat.getDifMethodCount());
        context.put("impCount", stat.getImpCount());
        context.put("impApiPrecent", stat.getImpApiPrecent());
        context.put("uidCount", stat.getUdidCount());
        context.put("ipPrecent", stat.getIpPrecent());
        context.put("udidCount", stat.getUdidCount());
        VariableResolverFactory functionFactory = new MapVariableResolverFactory(context);

        for (String rule : mediaForbidRules) {
            boolean eval = (Boolean) MVEL.eval(rule, functionFactory);
            if (eval) {
                return true;
            }
        }
        return false;
    }


    private VariableResolverFactory getAlarmRuleVariableResolverFactory(UserIPDetailStat stat){
        Map<String, Object> context = new HashMap<>();
        context.put("qps", stat.getQps());
        context.put("difMethodCount", stat.getDifMethodCount());
        context.put("impCount", stat.getImpCount());
        context.put("loginApiCount",stat.getLoginApiCount());
        context.put("impApiPrecent", stat.getImpApiPrecent());
        context.put("uidCount", stat.getUdidCount());
        context.put("udidCount", stat.getUdidCount());
        context.put("notExsitUdidCount", stat.getNotExsitUdidCount());
        context.put("notExistUdidPercent", stat.getNotExistUdidPercent());
        VariableResolverFactory functionFactory = new MapVariableResolverFactory(context);
        return functionFactory;
    }
}
