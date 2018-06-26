package com.yoho.bigdata.spark.config;

import java.io.Serializable;
import java.util.Set;

import lombok.Data;
import lombok.ToString;

/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/16.
 */
@Data
@ToString
public class WhiteIPListProperties implements Serializable {

    private Set<String> ips;

    private Set<String> intranetIps;
    
    //探测ip
    private Set<String> detectIps;

}
