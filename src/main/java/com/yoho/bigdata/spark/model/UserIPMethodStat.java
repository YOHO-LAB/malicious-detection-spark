package com.yoho.bigdata.spark.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 按method分别统计访问次数
 * Created by zrow.li on 2017/3/30.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class UserIPMethodStat implements Serializable {
    // 用户IP
    private String userIP;
    // 请求方法
    private String method;
    // 次数
    private long count;
}
