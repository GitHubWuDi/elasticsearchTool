package com.example.elasticsearch.vo;

import java.util.Map;

import lombok.Data;

/**
* @author wudi
* @version 创建时间：2018年8月4日 下午3:30:32
* @ClassName EsDocVO
* @Description doc数据VO
*/
@Data
public class EsDocVO {

	private String idValue; //primaryKey
	private Map<String,Object> map; //doc content
}
