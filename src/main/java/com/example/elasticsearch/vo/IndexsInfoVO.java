package com.example.elasticsearch.vo;

import lombok.Data;

/** * 
* @author wudi 
* E‐mail:wudi@vrvmail.com.cn 
* @version 创建时间：2019年4月1日 上午11:15:34 
* 类说明  多索引下的分页查询
*/
@Data
public class IndexsInfoVO {
   
	private String[] index; //索引集合
	private String[] type; //索引类型集合
}
