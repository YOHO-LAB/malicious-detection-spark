package com.yoho.bigdata.spark.utils;

import com.yoho.bigdata.spark.store.redis.RedisTemplate;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UdidUtil {
    public static boolean isExistUdid(String udid) {
        String day = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
        String redisKey = day + ":totaluv";
        boolean exist = RedisTemplate.sismember(redisKey, udid);
        return exist;
    }
}
