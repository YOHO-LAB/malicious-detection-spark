package com.yoho.bigdata.spark.utils;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.yoho.bigdata.spark.config.AllProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import com.yoho.bigdata.spark.store.redis.RedisTemplate;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * 敏感接口列表
 * Created by zrow.li on 2017/4/1.
 */
public class ImpApiContext implements Serializable {

    private volatile Broadcast<Set<String>> broadCastApiSet;

    private volatile Broadcast<Set<String>> broadCastLoginApiSet;

	private static final String UIC_API = "/xxxx/xxxxx";

    private transient ScheduledExecutorService executors = Executors.newScheduledThreadPool(2);

    private volatile Set<String> broadcast_loginApiSet = new HashSet<>();
    private volatile Set<String> broadcast_apiSet = new HashSet<>();

    public ImpApiContext(JavaSparkContext sc) {
        Set<String> apiList=RedisTemplate.smembers("apiList");
        HashSet<String> newApiList= Sets.newHashSet(apiList);
        Set<String> loginApiList=RedisTemplate.smembers("loginApiList");
        HashSet<String> newLoginApiList= Sets.newHashSet(loginApiList);
        //本地的一个缓存
        broadcast_loginApiSet=newLoginApiList;
        broadcast_apiSet=newApiList;

        broadCastApiSet=sc.broadcast(newApiList);
        broadCastLoginApiSet=sc.broadcast(newLoginApiList);
        executors.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                refreshData(sc);
            }
        }, 60, 120, TimeUnit.SECONDS);
    }


    private void refreshData(JavaSparkContext sc) {

        Set<String> apiList=RedisTemplate.smembers("apiList");
        Set<String> loginApiList=RedisTemplate.smembers("loginApiList");
        HashSet<String> newApiList= Sets.newHashSet(apiList);
        HashSet<String> newLoginApiList= Sets.newHashSet(loginApiList);

        if(null!=broadCastApiSet && CollectionUtils.isNotEmpty(newApiList)) {
            broadCastApiSet.unpersist();
        }
        if(null!=broadCastLoginApiSet && CollectionUtils.isNotEmpty(newLoginApiList)) {
            broadCastLoginApiSet.unpersist();
        }
        if(CollectionUtils.isNotEmpty(newApiList)){
            broadCastApiSet=sc.broadcast(newApiList);
        }
        if(CollectionUtils.isNotEmpty(newLoginApiList)){
            broadCastLoginApiSet=sc.broadcast(newLoginApiList);
        }
        //更新缓存
        broadcast_loginApiSet=newLoginApiList;
        broadcast_apiSet=newApiList;
    }

    public  Set<String> getApiList() {
        Set<String> redisApliList=Sets.newHashSet();
        try {
             redisApliList = broadCastApiSet.getValue();
        }catch (Exception e){
            redisApliList=broadcast_apiSet;
        }

        Set<String> redisLoginApliList=Sets.newHashSet();
        try {
            redisLoginApliList = broadCastLoginApiSet.getValue();
        }catch (Exception e){
            redisLoginApliList=broadcast_loginApiSet;
        }

        if (CollectionUtils.isNotEmpty(redisApliList) && CollectionUtils.isNotEmpty(redisLoginApliList)) {
            redisApliList.addAll(redisLoginApliList);
        }
        return redisApliList;
    } 
    
    public Set<String> getloginApiList(){
	 	Set<String> redisLoginApliList= broadCastLoginApiSet.getValue();
	 	return redisLoginApliList;
    } 
    
    public boolean isImpAPI(Set<String> apiList,String api) {
        boolean result = apiList.contains(api);
        if (!result) {
            result = StringUtils.contains(api, UIC_API);
        }
        return result;
    }

    public boolean isLoginAPI(Set<String> loginApiSet,String api) {
        boolean result = loginApiSet.contains(api);
        if (!result) {
            result = StringUtils.contains(api, UIC_API);
        }
        return result;
    }
}
