package com.example.elasticsearch.util;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author wudi
 * @version 创建时间：2018年7月28日 下午4:48:34
 * @ClassName ElasticSearchUtil
 * @Description Es工具类
 */
public class ElasticSearchUtil {

	private static Logger logger = Logger.getLogger(ElasticSearchUtil.class);

	/**
	 * 获得properties对应的属性
	 * 
	 * @param declaredFields
	 * @return
	 */
	public static XContentBuilder getXContentBuilder(Field[] declaredFields) {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
			for (Field field : declaredFields) {
				Class<?> type = field.getType();
				String typename = type.getName();
				String name = field.getName();
				switch (typename) {
				case "java.lang.String":
					builder = builder.startObject(name).field("type", "keyword").endObject();
					break;
				case "java.lang.Integer":
				case "int":
					builder = builder.startObject(name).field("type", "integer").endObject();
					break;
				case "java.util.Date":
					builder = builder.startObject(name).field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss")
							.endObject();
					break;
				case "boolean":
				case "long":
				case "short":
				case "byte":
				case "double":
				case "float":
					builder = builder.startObject(name).field("type", typename).endObject();
					break;

				default:
					break;
				}
			}
			builder = builder.endObject().endObject();
			return builder;
		} catch (IOException e) {
			logger.error("解析匹配错误", e);
			return null;
		}
	}

}
