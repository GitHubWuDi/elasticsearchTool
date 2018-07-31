package com.example.elasticsearch.util;

/**
* @author wudi
* @version 创建时间：2018年7月22日 下午4:45:32
* @ClassName ElasticSearchException
* @Description Elasticsearch-client异常
*/
public class ElasticSearchException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Integer resultCode;
	
	public ElasticSearchException(){
		super();
	}
	
	public ElasticSearchException(Integer code,String message){
		super(message);
		this.resultCode = code;
	}

	public Integer getResultCode() {
		return resultCode;
	}

	public void setResultCode(Integer resultCode) {
		this.resultCode = resultCode;
	}
	
	

}
