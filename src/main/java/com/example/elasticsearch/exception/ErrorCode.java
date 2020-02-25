package com.example.elasticsearch.exception;
/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2020年2月25日 下午3:09:48
* 类说明   错误码+错误描述
*/
public interface ErrorCode {
     
	
	String getCode();   //获得错误码
	
	String getDescription();  //获得错误的描述
}
