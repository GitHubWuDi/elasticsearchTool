package com.example.elasticsearch.vo;

import java.util.LinkedList;
import java.util.List;

import lombok.Data;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.springframework.util.StringUtils;

import com.example.elasticsearch.enums.FieldType;
import com.example.elasticsearch.util.DateUtil;

/**
 * @author wudi
 * @version 创建时间：2018年7月22日 下午5:21:21
 * @ClassName SearchField
 * @Description 分组查询
 */
@Data
public class SearchField {
	private String fieldName;
	private FieldType fieldType;
	private String timeFormat;
	private long timeSpan = -1;
	private DateHistogramInterval timeInterval;
	private SearchField childField;

	/**
	 * 增加一个分组字段
	 * 
	 * @param child
	 * @return
	 */
	public List<SearchField> getChildrenField() {
		List<SearchField> childrenField = new LinkedList<>();
		if (this.childField != null) {
			childrenField.add(this.childField);
		}
		return childrenField;
	}

	/**
	 * 构造函数一
	 * @param name
	 * @param type
	 * @param child
	 */
	public SearchField(String name, FieldType type, SearchField child) {
		this.setFieldName(name);
		this.setFieldType(type);
		if (type == FieldType.Date) {
			if (timeSpan <= 0) {
				timeSpan = 24 * 3600 * 1000;// 一天
			}
			if (StringUtils.isEmpty(timeFormat)) {
				timeFormat = DateUtil.DEFAULT_DATE_PATTERN;
			}
		}
		this.setChildField(child);
	}
    
	/**
	 * 构造函数-2
	 * @param name
	 * @param type
	 * @param format
	 * @param span
	 * @param child
	 */
	public SearchField(String name, FieldType type, String format, long span, SearchField child) {
		this.setFieldName(name);
		this.setFieldType(type);
		this.setTimeFormat(format);
		this.setTimeSpan(span);
		this.setChildField(child);
	}

	/**
	 * 构造函数-3
	 * @param name
	 * @param type
	 * @param format
	 * @param timeInterval
	 * @param child
	 */
	public SearchField(String name, FieldType type, String format, DateHistogramInterval timeInterval,
			SearchField child) {
		this.setFieldName(name);
		this.setFieldType(type);
		this.setTimeFormat(format);
		this.setTimeInterval(timeInterval);
		this.setChildField(child);
	}
}