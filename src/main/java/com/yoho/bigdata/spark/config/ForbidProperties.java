package com.yoho.bigdata.spark.config;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class ForbidProperties implements Serializable {
	
	private static final long serialVersionUID = 958073937554740172L;
	
	private boolean flag = true;
}
