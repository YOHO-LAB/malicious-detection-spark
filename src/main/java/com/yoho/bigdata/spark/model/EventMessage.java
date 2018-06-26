package com.yoho.bigdata.spark.model;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Splitter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 对应kafka消息
 * Created by zrow.li on 2017/3/29.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventMessage implements Serializable {

    // 本地IP
    private String localIP;
    // 用户IP
    private String userIP;
    // 当前时间
    private String currentTime;
    // http请求类型
    private String httpType;
    // user-agent
    private String userAgent;
    // 请求标示
    private String requestMethod;
    // 请求参数
    private Map<String, String> requestParams;
    // http响应码
    private String httpStatus;
    // 处理时长
    private double duration;
    //topic
    protected String topic;
    //原始消息
    private String originalMessage;
    //客户端类型
    private String clientType;

    // 从user-agent里获取udid
    public String getUdid() {
        if (StringUtils.equals(this.userAgent, "yoho/nodejs")) {
            if (MapUtils.isEmpty(this.requestParams)) {
                return "";
            }
            return this.requestParams.getOrDefault("udid", "");
        } else {
            String params = StringUtils.substringBetween(this.userAgent, "(", ")");
            if (StringUtils.isNotEmpty(params)) {
                Iterable<String> msgs = Splitter.on(";").omitEmptyStrings().trimResults().split(params);
                String[] startwith = {"udid/", "Udid/"};
                for (String msg : msgs) {
                    if (StringUtils.startsWithAny(msg, startwith)) {
                        return StringUtils.substringAfter(msg, "/");
                    }
                }
            }
        }
        return "";
    }

    // 对于ios的user-agent需要去掉sid和ts
    public String getUserAgentInfo() {
        if (StringUtils.startsWith(this.userAgent, "YH_Mall_iPhone")) {
            StringBuilder ua = new StringBuilder();
            String[] startWith = {"sid", "ts"};
            String params = StringUtils.substringBetween(this.userAgent, "(", ")");
            if (StringUtils.isNotEmpty(params)) {
                Iterable<String> msgs = Splitter.on(";").omitEmptyStrings().trimResults().split(params);
                for (String msg : msgs) {
                    if (!StringUtils.startsWithAny(msg, startWith)) {
                        ua.append(msg);
                        ua.append(";");
                    }
                }
                return ua.toString();
            }
        }
        return this.userAgent;
    }


    public String getUid() {
        if (requestParams == null) {
            return "";
        }

        Object uid = requestParams.get("uid");
        if (uid == null) {
            return "";
        } else {
            return uid + "";
        }
    }

    public String getMediaUdid() {
        if (requestParams == null) {
            return "";
        }

        Object udid = requestParams.get("udid");
        if (udid == null) {
            return "";
        } else {
            return udid + "";
        }
    }

}
