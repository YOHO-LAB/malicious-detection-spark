package com.yoho.bigdata.spark.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.annotation.JSONField;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 单IP数据分维度统计
 * Created by zrow.li on 2017/4/5.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(exclude = {"messages", "ipPrecent", "impApiPrecent", "timestamp"})
@ToString(exclude = {"messages"})
public class UserIPDetailStat implements Serializable {
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

    // 不存在的udid个数
    private long notExsitUdidCount;

    /**
     * 不存在udid占比
     */
    private double notExistUdidPercent;

    /**
     * 设备类型总数
     */
    private int deviceTypeCount;

    // 时间戳
    private long timestamp;

    //登录接口调用次数
    private int loginApiCount;

    // 访问记录
    @JSONField(serialize = false)
    private List<EventMessage> messages;
    
    //被那条封禁规则命中
    private String meetForbidRule;

    public void incrNotExsitUdidCount() {
        ++this.notExsitUdidCount;
    }
}
