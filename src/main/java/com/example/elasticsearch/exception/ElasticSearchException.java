package com.example.elasticsearch.exception;

import lombok.Data;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2020年2月25日 下午2:41:20
* 类说明     es运行过程当中报出的异常
*
*/

public class ElasticSearchException extends RuntimeException {
	
	protected  ErrorCode errorCode;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public ElasticSearchException() {
		super(ElasticSearchErrorEnum.UNSPECIFIED.getDescription());
	}
    
	
	public ElasticSearchException(ErrorCode errorCode){
		super(errorCode.getDescription());
	}
	
	public ElasticSearchException(ErrorCode errorCode,Throwable t){
		super(errorCode.getDescription(), t);
	}
	
    public ElasticSearchException(String description) {
    	super(description);
    	this.errorCode = ElasticSearchErrorEnum.UNSPECIFIED;
    }
    
    public ElasticSearchException(String description,Throwable t){
    	super(description, t);
    	this.errorCode  = ElasticSearchErrorEnum.UNSPECIFIED;
    }


	public ErrorCode getErrorCode() {
		return errorCode;
	}


	
}
