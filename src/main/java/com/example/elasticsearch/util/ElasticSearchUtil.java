package com.example.elasticsearch.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.example.elasticsearch.util.page.QueryCondition;

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

	/**
	 * 完成对应的查询（must查询语句）
	 * 
	 * @param conditions
	 * @return
	 */
	public static QueryBuilder toQueryBuilder(List<QueryCondition> conditions) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();//
		for (QueryCondition con : conditions) {
			query = query.must(toQueryBuild(con));
		}
		return query;
	}

	/**
	 * 查询语句
	 * 
	 * @param con
	 * @return
	 */
	public static QueryBuilder toQueryBuild(QueryCondition con) {
		QueryBuilder query = null;
		switch (con.getCompareExpression()) {
		/// <summary>
		/// 等于
		/// </summary>
		case Eq:
			query = QueryBuilders.termQuery(con.getField(), con.getValue1());
			break;
		/// <summary>
		/// where a!=1
		/// </summary>
		case NotEq:
			query = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(con.getField(), con.getValue1()));
			break;

		/// <summary>
		/// in (1,2,3)
		/// </summary>
		case In:
			// FilterBuilder inFilter = FilterBuilders.inFilter("typeId", ids)
			query = QueryBuilders.termsQuery(con.getField(), con.getValue1());
			break;

		/// <summary>
		/// 之间
		/// </summary>
		case Between:
			query = QueryBuilders.rangeQuery(con.getField()).gte(con.getValue1()).lt(con.getValue2());
			break;
		/// <summary>
		/// is not null
		/// </summary>
		case NotNull:
			// 参考https://blog.csdn.net/wangsht/article/details/52776139
			// query=QueryBuilders.notQuery(QueryBuilders.missingQuery(con.getField()));
			query = QueryBuilders.existsQuery(con.getField());
			break;

		/// <summary>
		/// is null
		/// </summary>
		case IsNull:
			// query=QueryBuilders.missingQuery(con.getField());
			query = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(con.getField()));
			break;

		/// <summary>
		/// 类似
		/// 不需要加配符号
		/// </summary>
		case Like:
			query = QueryBuilders.wildcardQuery(con.getField(), "*" + con.getValue1() + "*");
			break;
		/// <summary>
		/// like '%xx'
		/// 不需要加配符号
		/// </summary>
		case LikeEnd:
			query = QueryBuilders.wildcardQuery(con.getField(), "*" + con.getValue1());
			break;
		/// <summary>
		/// like 'xx%'
		/// 不需要加配符号
		/// </summary>
		case LikeBegin:
			query = QueryBuilders.wildcardQuery(con.getField(), con.getValue1() + "*");
			break;
		/// <summary>
		/// 大于
		/// </summary>
		case Gt:
			query = QueryBuilders.rangeQuery(con.getField()).gt(con.getValue1());
			break;
		/// <summary>
		/// 大于等于
		/// </summary>
		case Ge:
			query = QueryBuilders.rangeQuery(con.getField()).gte(con.getValue1());
			break;
		/// <summary>
		/// 小于等于
		/// </summary>
		case Le:
			query = QueryBuilders.rangeQuery(con.getField()).lte(con.getValue1());
			break;
		/// <summary>
		/// 小于
		/// </summary>
		case Lt:
			query = QueryBuilders.rangeQuery(con.getField()).lt(con.getValue1());
			break;

		/// <summary>
		/// 逻辑且
		/// </summary>
		case And:
			QueryBuilder must1 = toQueryBuild((QueryCondition) con.getValue1());
			QueryBuilder must2 = toQueryBuild((QueryCondition) con.getValue2());
			query = QueryBuilders.boolQuery().must(must1).must(must2);
			break;
		/// <summary>
		/// 逻辑或者
		/// </summary>
		case Or:
			QueryBuilder queryBuilder1 = toQueryBuild((QueryCondition) con.getValue1());
			QueryBuilder queryBuilder2 = toQueryBuild((QueryCondition) con.getValue2());

			query = QueryBuilders.boolQuery().should(queryBuilder1).should(queryBuilder2);
			break;
		/// <summary>
		/// 逻辑非
		/// </summary>
		case Not:
			query = QueryBuilders.boolQuery().mustNot(toQueryBuild((QueryCondition) con.getValue1()));
			break;

		}
		return query;
	}

	/**
	 * 将对应的实体转换成map数据结构
	 * @param obj
	 * @return
	 */
	public static Map<String, Object> transBean2Map(Object obj) {
		if (obj == null) {
			return null;
		}
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				// 过滤class属性
				if (!key.equals("class")) {
					// 得到property对应的getter方法
					Method getter = property.getReadMethod();
					String typeName = property.getPropertyType().getName();
					Object value = getter.invoke(obj);
					if (value != null) {// 过滤为null的数据

						if (typeName.equals("java.util.Date")) {

							map.put(key, DateUtil.format((Date) value));
						} else {
							map.put(key, value);
						}

					}
				}
			}
		} catch (Exception e) {

		}
		return map;

	}

}
