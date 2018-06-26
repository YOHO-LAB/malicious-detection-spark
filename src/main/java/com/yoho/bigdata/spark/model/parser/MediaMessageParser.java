package com.yoho.bigdata.spark.model.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.yoho.bigdata.spark.model.EventMessage;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/22.
 */
public class MediaMessageParser {

    private static final Logger logger = LoggerFactory.getLogger(MediaMessageParser.class);

    public static EventMessage parse(String topic, String msg) {
        if (msg == null) {
            return null;
        }

        Iterable<String> msgsIterable = Splitter.on("|").trimResults().split(msg);
        List<String> msgs = Lists.newArrayList(msgsIterable);


        if (msgs.size() == 9) {
            return new EventMessage(msgs.get(0), msgs.get(1), msgs.get(2), msgs.get(3), msgs.get(4),
                    msgs.get(5), MediaMessageParser.parseParam(msgs.get(6)), msgs.get(7), Double.valueOf(msgs.get(8)), topic,msg,"");
        }

        return null;
    }


    public static Map parseParam(String params) {
        if (StringUtils.isBlank(params)) {
            return new HashedMap();
        }

        try {
            String s = params.replaceAll("\\x22", "%22");
            List<NameValuePair> pairs = URLEncodedUtils.parse(s, Charsets.UTF_8);

            Map p = new HashedMap();
            for (NameValuePair pair : pairs) {
                boolean k = pair.getName().equals("parameters");
                if (k) {
                    HashMap hashMap = JSON.parseObject(pair.getValue(), HashMap.class);
                    return hashMap;
                }
                p.put(pair.getName(), pair.getValue());
            }


            return p;

        } catch (Exception e) {
            return new HashedMap();
        }


    }

}
