package com.yoho.bigdata.spark.model.parser;

import com.alibaba.fastjson.JSON;
import com.yoho.bigdata.spark.model.EventMessage;
import com.yoho.bigdata.spark.constant.TopicConst;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/22.
 */
public class MessageParser {

    public static EventMessage parse(String msg) {

        if (StringUtils.isBlank(msg)) {
            return null;
        }

        HashMap<String, String> hashMap = JSON.parseObject(msg, HashMap.class);
        String type = hashMap.get("type");
        String message = hashMap.get("message");
        if (TopicConst.GATE_TOPIC.equals(type)) {
            return GateWayMessageParser.parse(type, message);
        } else if (TopicConst.UIC_TOPIC.equals(type)) {
           return GateWayMessageParser.parse(type, message);
        } else if (TopicConst.MEDIA_TOPIC.equals(type)) {
           return MediaMessageParser.parse(type, message);
        }
        return null;
    }
}
