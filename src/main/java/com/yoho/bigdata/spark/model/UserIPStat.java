package com.yoho.bigdata.spark.model;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 *
 * Created by zrow.li on 2017/3/29.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserIPStat implements Serializable {
    // 用户IP
    @JSONField(name="ip")
    private String userIP;
    // 访问次数
    private long count;
    // 次数占比
    private double percent;
    // 系统时间
    @JSONField(name="timestamp")
    private long timestamp;
    // 访问记录
    @JSONField(serialize=false)
    private List<EventMessage> messages;
}
