package com.yoho.bigdata.spark.config;


import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class HBaseProperties implements Serializable {

    private String zkquorum ;

    private boolean isWriteHbase = true;

}
