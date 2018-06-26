package com.yoho.bigdata.spark.store.redis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.math.NumberUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


/**
 * Created by Jieron on 2017/2/16.
 */
public class RedisTemplate {

    private static JedisPool pool = null;

    static {
        Properties properties = new Properties();
        try {
            properties.load(RedisTemplate.class.getClassLoader().getResourceAsStream("redis.properties"));
            String host = properties.getProperty("redis.udid.host");
            int port = NumberUtils.toInt(properties.getProperty("redis.udid.port"), 6379);
            int maxTotal = NumberUtils.toInt(properties.getProperty("redis.maxTotal"), 20);
            int maxIdle = NumberUtils.toInt(properties.getProperty("redis.maxIdle"), 5);
            int maxWaitMillis = NumberUtils.toInt(properties.getProperty("redis.maxWaitMillis"), 5000);

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            config.setMaxWaitMillis(maxWaitMillis);
            pool = new JedisPool(config, host, port);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static Jedis getJedis() {
        Jedis jedis = pool.getResource();
        return jedis;
    }

    public static void closeResource(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
            }
        }
    }

    public static Map<String, String> hgetall(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hgetAll(key);
        } catch (Exception e) {
        } finally {
            closeResource(jedis);
        }
        return null;
    }

    public static boolean sismember(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
           return jedis.sismember(key, value);
        } catch (Exception e) {
        } finally {
            closeResource(jedis);
        }

        return false;
    }
    
    public static Set<String> smembers(String key) {
        Set<String> value = null;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            value = jedis.smembers(key);
        } catch (Exception e) {
        } finally {
            closeResource(jedis);
        }
        return value;
    }


    public static String get(String key) {
        String value = null;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            value = jedis.get(key);
        } catch (Exception e) {
            //
        } finally {
            closeResource(jedis);
        }
        return value;
    }
}
