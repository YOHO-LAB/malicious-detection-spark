package com.yoho.bigdata.spark.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/22.
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UicIpStat implements Serializable {

    /**
     * IP
     */
    private String ip;

    /**
     * 是否在白名单中
     */
    private boolean whiteIpFlag;

    /**
     * 单位时间的QPS
     */
    private int qps;

    /**
     * 成功的次数
     */
    private int succ;

    /**
     * 失败次数
     */
    private int fail;

    /**
     * 失败比例
     */
    private double failPercent;
    
    //被那条封禁规则命中
    private String meetForbidRule;


}
