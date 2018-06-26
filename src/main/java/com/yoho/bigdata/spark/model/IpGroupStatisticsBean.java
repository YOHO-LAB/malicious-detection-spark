package com.yoho.bigdata.spark.model;

import java.io.Serializable;
import java.util.List;

import lombok.Data;

@Data
public class IpGroupStatisticsBean implements Serializable{
    private String ip;

    private long timestamp;

    //是否白名单
    private boolean whiteIpFlag;

    //该ip消息数
    private int count;

    //总消息数
//    private int allCount;

    //该ip消息数/总消息数
//    private float ipPercent;

    //该ip每秒消息数
    private float qps;

    //该ip排重udid数
    private int udidCount;

    //该ip排重uid数
    private int uidCount;

    //该ip排重登录设备数
    private int deviceTypeCount;

    //该ip排重接口数
    private int difMethodCount;

    //该ip敏感消息数
    private int impCount;

    //该ip敏感消息数/总敏感消息数
//    private float impApiPercent;

    //该ip今天没访问过的排重udid数
    private int notExistUdidCount;

    //notExistUdidCount/今天没放问过的消息总数   ???   应该是notexsitudidcount/所有ip今天没放问过的排重udid数
//    private float notExistUdidPercent;

    //该ip敏感消息数/该ip消息总数
    private float impPercent;

    //该ip排重udid数/该ip排重uid数
    private float udidCount_UidCount;

    //该ip排重uid数/该ip排重udid数
    private float uidCount_UdidCount;

    //该ip消息数/该ip排重接口数
    private float allCountPercent;

    //该ip的日志详情信息
    private List<EventMessage> messageList;

    private int iphoneClient = 0;
    private int androidClient = 0;
    private int webClient = 0;
    private int h5Client =0;

    @Override
    public String toString() {
        return "" + timestamp + "\t" + ip + "\t" + whiteIpFlag + "\t" + count + "\t" + qps + "\t" + udidCount + "\t" + uidCount + "\t" +
                deviceTypeCount + "\t" + difMethodCount + "\t" + impCount + "\t" + notExistUdidCount + "\t" + impPercent + "\t"
                + udidCount_UidCount + "\t" + uidCount_UdidCount + "\t" + allCountPercent + "\t" + iphoneClient+ "\t" + androidClient
                + "\t" + webClient+ "\t" + h5Client;

    }
}
