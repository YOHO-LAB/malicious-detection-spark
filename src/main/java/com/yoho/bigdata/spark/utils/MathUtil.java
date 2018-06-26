package com.yoho.bigdata.spark.utils;

import java.math.BigDecimal;

/**
 * 数学计算公式
 * Created by zrow.li on 2017/4/5.
 */
public final class MathUtil {
    public static double precent(long numerator, long denominator) {
        if(numerator==0 || denominator==0){
            return 0;
        }
        double percent = Long.valueOf(numerator).doubleValue() / Long.valueOf(denominator).doubleValue();
        return (new BigDecimal(percent * 100)).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
