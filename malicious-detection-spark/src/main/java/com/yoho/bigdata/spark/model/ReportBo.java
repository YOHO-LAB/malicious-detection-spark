package com.yoho.bigdata.spark.model;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/9/14.
 */
@Data
@Builder
@ToString
public class ReportBo implements Serializable {

    /**
     * 恶意IP
     */
    private String ip;

    /**
     * 接口
     */
    private String api;

    /**
     * 统计时间长度
     */
    private int duration;

    /**
     * 原因  用json 字符串上报
     */
    private String reason;

    private long timestamp;

    @JSONField(serialize=false)
    private List<EventMessage> messages;
}
