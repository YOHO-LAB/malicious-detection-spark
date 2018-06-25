package com.yoho.bigdata.spark.handler.handler.assist;

import java.io.Serializable;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.yoho.bigdata.spark.config.PropertiesContext;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/25.
 */
public class WhiteIpHandler implements Serializable {

    private PropertiesContext pc;

    public WhiteIpHandler(PropertiesContext pc) {
        this.pc = pc;
    }

    public boolean isWhiteList(String ip) {
    		//这个ip不在白名单中，用于做外部探测
        if(CollectionUtils.isNotEmpty(pc.getWhiteIp().getDetectIps()) && pc.getWhiteIp().getDetectIps().contains(ip)) {
            return false;
        }
        Set<String> whiteList = pc.getWhiteIp().getIps();
        Set<String> intranetList = pc.getWhiteIp().getIntranetIps();
        boolean isFilter = whiteList.contains(ip);
        if (!isFilter) {
            Iterable<String> iterable = Splitter.on(".").trimResults().split(ip);
            String[] ipTokens = Lists.newArrayList(iterable).toArray(new String[0]);
            String ipB = ipTokens[0] + "." + ipTokens[1] + ".0.0";
            return intranetList.contains(ipB);
        }
        return isFilter;
    }

}
