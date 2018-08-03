package com.example.elasticsearch.util.page;

import java.util.List;

import lombok.Data;
/**
* @author wudi
* @version 创建时间：2018年7月28日 下午3:49:48
* @ClassName PageRes
* @Description 返回分页数据
*/
@Data
public class PageRes<T> {
	private String code;
	private List<T> list;
	private Long total;
	private String message;
	
}
