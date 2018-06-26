package com.yoho.bigdata.spark.config;

import java.io.Serializable;

import lombok.Data;
import lombok.ToString;


/**
 * 描述:
 * Created by pangjie@yoho.cn on 2017/8/16.
 */
@Data
@ToString
public class AllProperties implements Serializable {

	private static final long serialVersionUID = 6246687766232495526L;

	private WhiteIPListProperties whiteIp;

    private AlarmRuleProperties forbidRule;

    private CallBackIpProperties ips;

    private AlarmRuleProperties alarmRule;

    private HBaseProperties hbase;
    
    private IDCProperties idc;

    private ForbidProperties forbid;
}
