package com.example.elasticsearch.vo;

import lombok.Data;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2020年2月24日 上午9:51:49
* 类说明
*/
@Data
public class RangeVO {

	private String metricName;
	private long start;
	private long end;
	
}
