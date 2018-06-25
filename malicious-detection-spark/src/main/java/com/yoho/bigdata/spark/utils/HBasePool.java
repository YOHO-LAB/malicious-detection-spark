package com.yoho.bigdata.spark.utils;

import com.yoho.bigdata.spark.config.PropertiesContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by xjipeng on 2017/10/16.
 */
public class HBasePool {

    private static final Logger logger = LoggerFactory.getLogger(HBasePool.class) ;

    /**
     * hbase connection
     */
    private static Connection connection = null ;

    public static synchronized Connection getConnection(PropertiesContext pc){
        if( connection == null ){
            try {
                Configuration hbaseConf = HBaseConfiguration.create();
                hbaseConf.set("hbase.zookeeper.quorum", pc.getHbase().getZkquorum());
                hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
                hbaseConf.set("hbase.defaults.for.version.skip", "true");

                connection = ConnectionFactory.createConnection(hbaseConf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection ;
    }


    public static void closeTable(Table table) {
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
