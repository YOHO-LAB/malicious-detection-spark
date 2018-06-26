package com.yoho.bigdata.spark.config;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.yaml.snakeyaml.Yaml;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/15.
 */
public class PropertiesContext implements Serializable {
	
    private volatile Broadcast<AllProperties> broadCastRules;

    private int duration;

	private transient ScheduledExecutorService executors = Executors.newScheduledThreadPool(2);
	
    public PropertiesContext(JavaSparkContext sc,int duration) {
        try {
            this.duration=duration;
            AllProperties allProperties=loadYml(sc, duration);
            broadCastRules = sc.broadcast(allProperties);
            executors.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                   refreshData(sc,duration);
                }
            }, 60, 60, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public PropertiesContext(JavaSparkContext sc,int  duration , AllProperties allProperties) {
        this.duration=duration;
        broadCastRules = sc.broadcast(allProperties);
    }
    
    private void refreshData(JavaSparkContext sc, int duration) {
    		AllProperties allProperties=loadYml(sc, duration);
    		if(null==allProperties) {
    			System.out.println("no allProperties to braodcast");
    			return;
    		}
    		if(null!=broadCastRules) {
    			broadCastRules.unpersist();
    		}
    		broadCastRules=sc.broadcast(allProperties);
    		System.out.println("refresh allProperties to success allProperties:"+allProperties);
	}
    
	private AllProperties loadYml(JavaSparkContext sc, int duration) {
		AllProperties allProperties=null;
		try {
			Configuration conf = sc.hadoopConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream inputStream = fs.open(new Path("hdfs:///malicious/" + this.duration + "/config.yml"));
			Yaml yaml = new Yaml();
			allProperties=yaml.loadAs(inputStream, AllProperties.class);
		} catch (Exception e) {
			System.out.println("loadYml exception is:"+e);
		}
		return allProperties;
	}
    
    public AllProperties getAllProperties() {
        return broadCastRules.getValue();
    }

    public WhiteIPListProperties getWhiteIp() {
        return broadCastRules.getValue().getWhiteIp();
    }

    public AlarmRuleProperties getAlarmRules() {
        return broadCastRules.getValue().getAlarmRule();
    }

    public CallBackIpProperties getCallBackProperties() {
        return broadCastRules.getValue().getIps();
    }

    public AlarmRuleProperties getForbidRule() {
        return broadCastRules.getValue().getForbidRule();
    }

    public HBaseProperties getHbase() {
        return broadCastRules.getValue().getHbase();
    }
    
    public IDCProperties getIDC() {
        return broadCastRules.getValue().getIdc();
    }

    public ForbidProperties getForbid(){
        return broadCastRules.getValue().getForbid();
    }

}