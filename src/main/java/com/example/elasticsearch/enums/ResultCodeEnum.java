package com.example.elasticsearch.enums;

/**
 * 统一的返回成功和未知错误定义
 * 其他模块可以自定义自己的枚举范围，只需要实现IResultCode即可。各模块的code可以重复
 * @author wudi
 *
 */
public enum ResultCodeEnum implements IResultCode {
	FORM_VALIDATE_ERROR(-2, "表单验证错误"),
	UNKNOW_FAILED(-1, "未知的错误"),
	SUCCESS(0, "成功"),
	Unauthorized(403, "未授权的请求"),
	ERROR(500,"程序编译出错"),
	IPIsEmpty(-1,"IP为空");

	private Integer code;
	private String msg;
	
	ResultCodeEnum(Integer code, String msg) {
		this.code = code;
		this.msg = msg;
	}
	@Override
	public Integer getCode() {
		return code;
	}

	@Override
	public String getMsg() {
		return msg;
	}
	
}
