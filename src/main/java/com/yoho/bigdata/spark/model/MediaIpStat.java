package com.yoho.bigdata.spark.model;

import lombok.Data;
import lombok.ToString;

import java.util.Map;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/22.
 */
@Data
@ToString
public class MediaIpStat {
    // 用户IP
    private String ip;

    /**
     * 是否是白名单IP
     */
    private boolean whiteIpFlag;

    /**
     * 该IP的QPS
     */
    private int qps;

    /**
     * 不同接口的个数
     */
    private int difMethodCount;

    /**
     * 接口明细
     */
    private Map<String, Integer> MethodDetail;

    // 敏感接口访问次数
    private long impCount;


    // 敏感接口占比
    private double impApiPrecent;

    /**
     * uid个数
     */
    private int uidCount;


    // 单ip访问次数占比
    private double ipPrecent;


    // udid个数
    private long udidCount;

    /**
     * 设备类型总数
     */
    private int deviceTypeCount;

}
