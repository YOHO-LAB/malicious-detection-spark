package com.yoho.bigdata.spark.utils;

import java.io.Serializable;

/**
 * 常量定义
 * Created by zrow.li on 2017/3/29.
 */
public final class AppConsts implements Serializable {
    public static int BATCH_DURATION_02S = 2;
    public static int BATCH_DURATION_30S = 30;

    // 时间维度 - 30s
    public static final int TIME_DIMENSION_30S = 30;
    // 时间维度 - 2s
    public static final int TIME_DIMENSION_02S = 2;
    public static final int SLIDE_DURATION = 5;

    public static final int MIN_VIEW_TIME_OF_30S = 5;
    public static final int MIN_VIEW_TIME_OF_02S = 5;


    public static int getMinViews(int timeDimension) {
        int views = AppConsts.MIN_VIEW_TIME_OF_02S;
        switch (timeDimension) {
            case AppConsts.TIME_DIMENSION_02S:
                views = AppConsts.MIN_VIEW_TIME_OF_02S;
                break;
            case AppConsts.TIME_DIMENSION_30S:
                views = AppConsts.MIN_VIEW_TIME_OF_30S;
                break;
            default:
                break;
        }
        return views;
    }

    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    public static int getTimeDension(int duration) {
        if (duration == AppConsts.BATCH_DURATION_30S) {
            return AppConsts.TIME_DIMENSION_30S;
        } else {
            return AppConsts.TIME_DIMENSION_02S;
        }
    }
}
