package com.yoho.bigdata.spark.config;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/16.
 */
@Data
@ToString
public class AlarmRuleProperties implements Serializable {

    /**
     * yoho规则
     */
    private List<String> yoho;

    /**
     * uic规则
     */
    private List<String> uic;


    /**
     * 媒体相关规则
     */
    private List<String> media;
}
