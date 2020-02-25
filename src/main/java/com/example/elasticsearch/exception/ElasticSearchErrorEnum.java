package com.example.elasticsearch.exception;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2020年2月25日 下午2:46:09
* 类说明   es报错错误码说明
*/

public enum ElasticSearchErrorEnum implements ErrorCode {
	
	

	  UNSPECIFIED("500", "程序内部错误，请检查"),
	  NO_SERVICE("404", "网络异常, 服务器熔断"),
	  
	  REINDEX("4001","索引迁移失败"),
	  INDEX_IS_EXIST("4002","请检查索引是否存在或状态"),
	  CONSTRUCT_ERROR("4003","数据结构构造错误");
	
	private String code; //错误编码
	private String description; //错误描述
	 
	 private ElasticSearchErrorEnum(String code,String description){
		 this.code = code;
		 this.description = description;
	 }
	 
	 /**
	  * 通过code获得对应的枚举
	  * @param code
	  * @return
	  */
	 public static ElasticSearchErrorEnum getByCode(String code) {
		 for (ElasticSearchErrorEnum value : ElasticSearchErrorEnum.values()) {
			   if(code.equals(value.getCode())) {
				   return value;
			   }
		}
		return UNSPECIFIED;
	 }

	 
	 /**
	  * 检查枚举值当中是否包含该code
	  * @param code
	  * @return
	  */
	 public static Boolean contains(String code) {
		 for (ElasticSearchErrorEnum value : ElasticSearchErrorEnum.values()){
			 if(code.equals(value.getCode())){
				 return true;
			 }
		 }
		return false;
	 }
	 
	 
	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	 
	 
	 
	 
}
