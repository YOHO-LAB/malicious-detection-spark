package com.yoho.bigdata.spark.config;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/22.
 */
@Data
@ToString
public class CallBackIpProperties implements Serializable {

    private List<String> nginx;

    private List<String> uic;

    private List<String> media;

    private String ops;

    private String maliciousIp;

    /**
     * udid 调用地址
     */
    private String  udid;
}
