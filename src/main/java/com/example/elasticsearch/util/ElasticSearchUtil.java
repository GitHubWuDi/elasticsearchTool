package com.example.elasticsearch.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import com.example.elasticsearch.enums.ResultCodeEnum;
import com.example.elasticsearch.util.page.QueryCondition;
import com.example.elasticsearch.vo.SearchField;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

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
	public static XContentBuilder getXContentBuilder(Map<String, Class<?>> declaredFields) {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
			builder = getXContentBuilder(declaredFields, "", builder);
			builder = builder.endObject().endObject();
			return builder;
		} catch (IOException e) {
			logger.error("构造错误", e);
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "构造错误");
		}
	}

	private static XContentBuilder getXContentBuilder(Map<String, Class<?>> declaredFields, String rootName,
			XContentBuilder rootBuilder) {
		try {

			for (Map.Entry<String, Class<?>> field : declaredFields.entrySet()) {
				Class<?> type = field.getValue(); // field.getType();
				String typename = type.getName();
				String name = field.getKey(); // field.getName();
				if (!StringUtils.isEmpty(rootName)) {
					name = rootName + "." + name;
				}
				switch (typename) {
				case "java.lang.String":
					rootBuilder = rootBuilder.startObject(name).field("type", "keyword").endObject();
					break;
				case "java.lang.Integer":
				case "int":
					rootBuilder = rootBuilder.startObject(name).field("type", "integer").endObject();
					break;
				case "java.util.Date":
					rootBuilder = rootBuilder.startObject(name).field("type", "date")
							.field("format", "yyyy-MM-dd HH:mm:ss").endObject();
					break;
				case "boolean":
				case "java.lang.Boolean":
					rootBuilder = rootBuilder.startObject(name).field("type", "boolean").endObject();
					break;
				case "long":
				case "java.lang.Long":
					rootBuilder = rootBuilder.startObject(name).field("type", "long").endObject();
					break;
				case "byte":
				case "java.lang.Byte":
					rootBuilder = rootBuilder.startObject(name).field("type", "long").endObject();
					break;
				case "short":
				case "java.lang.Short":
					rootBuilder = rootBuilder.startObject(name).field("type", "short").endObject();
					break;
				case "double":
				case "java.lang.Double":
					rootBuilder = rootBuilder.startObject(name).field("type", "double").endObject();
					break;
				case "float":
				case "java.lang.Float":
					rootBuilder = rootBuilder.startObject(name).field("type", "float").endObject();
					break;
				default:
					Field[] fields = type.newInstance().getClass().getDeclaredFields();
					Map<String, Class<?>> map = fieldsConvertMap(fields);
					rootBuilder = getXContentBuilder(map, name, rootBuilder);
					break;
				}
			}
			return rootBuilder;
		} catch (Exception e) {
			logger.error("解析匹配错误", e);
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "构造错误");
		}
	}

	public static Map<String, Class<?>> fieldsConvertMap(Field[] fields) {
		Map<String, Class<?>> map = new HashMap<>();
		for (Field field : fields) {
			map.put(field.getName(), field.getType());
		}
		return map;
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
	 * 获得Map<String,Class<?>>
	 * 
	 * @param map
	 * @return
	 */
	public static Map<String, Class<?>> getFieldMap(Map<String, Object> map, String rootName) {
		Map<String, Class<?>> field = new HashMap<>();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			if (!StringUtils.isEmpty(rootName)) {
				key = rootName + "." + key;
			}
			Object value = entry.getValue();
			String name = entry.getValue().getClass().getName();
			switch (name) {
			case "java.util.Map":
			case "java.util.HashMap":
				Map<String, Object> maps = (Map<String, Object>) value;
				field.putAll(getFieldMap(maps, key));
				break;
			default:
				Class<? extends Object> fieldClass = entry.getValue().getClass();
				field.put(key, fieldClass);
				break;
			}
		}
		return field;
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
    * 获得泛型的类型
    * @param field
    * @return
    */
	public static String getParamterTypeByList(Field field) {
		String typeName = null;
		try {
			Type genericType = field.getGenericType();
			ParameterizedType pt = (ParameterizedType) genericType;
			Type ts = pt.getActualTypeArguments()[0];
			typeName = ts.getTypeName();
		} catch (Exception e) {
			logger.info("泛型实例化异常", e);
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), e.getMessage());
		}
		return typeName;
	}

	public static void main(String[] args) {
		List<String> list = new ArrayList<>();
		list.add("1");
		list.add("2");
		Type t = list.getClass().getGenericSuperclass();
		ParameterizedType pt = (ParameterizedType) t;
		Type type = pt.getActualTypeArguments()[0];
		Class<?> clazz = (Class<?>)type;
		String typeName = clazz.getTypeName();
		System.out.println(typeName);
	}
	
	/**
	 * 获得List对应的Map<String,Class<?>>
	 * 
	 * @param parameterTypeName
	 * @param key
	 * @param field
	 * @param entity
	 * @return
	 */
	private static Map<String, Object> getMapParamerInfoByList(String key, Object value,Object obj) {
		Map<String, Object> map = new HashMap<>();
		try {
			Field field = obj.getClass().getDeclaredField(key);
			String paramterType = getParamterTypeByList(field); // 获得List当中的包装类
			switch (paramterType) {
			case "java.lang.String":
			case "java.lang.Integer":
			case "java.lang.Boolean":
			case "java.lang.FLoat":
			case "java.lang.Long":
			case "java.lang.Byte":
			case "java.lang.Short":
			case "java.lang.Double":
				map.put(key, value);
				break;
			case "java.util.Map":
				List<Map<String, Object>> listMap = (List<Map<String, Object>>) value;
				for (Map<String, Object> childrenMap : listMap) {
					map.putAll(childrenMap);
				}
				break;
			default: //业务实体
				List<Object> businessList = (List<Object>) value;
				for (Object busiObject : businessList) {
					Map<String, Object> childrenMap = transBean2Map(busiObject);
					map.putAll(childrenMap);
				}
				break;
			}
		} catch (Exception e) {
			logger.error("List数据解析出现错误", e);
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), e.getMessage());
		}
		return map;
	}

	/**
	 * 将对应的实体转换成map数据结构
	 * 
	 * @param obj
	 * @return
	 */
	public static Map<String, Object> transBean2Map(Object obj) {
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				Method getter = property.getReadMethod();
				Object value = getter.invoke(obj);
				String typeName = property.getPropertyType().getName();
				switch (typeName) {
				case "java.lang.String":
				case "java.lang.Integer":
				case "int":
				case "java.lang.Boolean":
				case "boolean":
				case "java.lang.FLoat":
				case "float":
				case "java.lang.Long":
				case "long":
				case "java.lang.Byte":
				case "byte":
				case "java.lang.Short":
				case "short":
				case "java.lang.Double":
				case "double":
				case "java.util.Map":
					map.put(key, value);
					break;
				case "java.util.List":
					Map<String, Object> mapParamerInfoByList = getMapParamerInfoByList(key, value,obj);
					map.put(key, mapParamerInfoByList);
					break;
				case "java.util.Date":
					map.put(key, DateUtil.format((Date) value));
					break;
				case "java.lang.Class":
					break;
				default:
					map.put(key, transBean2Map(value));
					break;
				}
			}
			return map;
		} catch (Exception e) {
			throw new ElasticSearchException(ResultCodeEnum.ERROR.getCode(), "object转map出现错误");
		}

	}

	/**
	 * 解析aggs对应的数据格式
	 * 
	 * @param field
	 * @param aggregation
	 * @return
	 */
	public static List<Map<String, Object>> getMultiBucketsMap(SearchField field, Aggregation aggregation) {
		List<Map<String, Object>> result = new ArrayList<>();
		switch (field.getFieldType()) {
		case NumberSum:
		case NumberAvg:
		case NumberMax:
		case NumberMin:
		case ObjectDistinctCount:
			String valueAsString = ((NumericMetricsAggregation.SingleValue) aggregation).getValueAsString();
			Map<String, Object> mapss = new HashMap<>();
			mapss.put(field.getFieldName(), field.getFieldType().toString());// 根节点元素提取
			mapss.put("doc_count", valueAsString);// 根节点元素提取
			result.add(mapss);
			break;
		case Numberstat:
			Stats stats = (Stats) aggregation;
			Map<String, Object> mapstats = new HashMap<>();
			double avg = stats.getAvg();
			long count = stats.getCount();
			double max = stats.getMax();
			double min = stats.getMin();
			double sum = stats.getSum();
			mapstats.put(field.getFieldName(), field.getFieldType().toString());
			mapstats.put("avg", avg);
			mapstats.put("count", count);
			mapstats.put("max", max);
			mapstats.put("min", min);
			mapstats.put("sum", sum);
			result.add(mapstats);
			break;
		default:
			MultiBucketAggregation(field, aggregation, result);
			break;
		}
		return result;
	}

	/**
	 * 分组递归
	 * 
	 * @param field
	 * @param aggregation
	 * @param result
	 */
	private static void MultiBucketAggregation(SearchField field, Aggregation aggregation,
			List<Map<String, Object>> result) {
		List<? extends MultiBucketsAggregation.Bucket> buckets = ((MultiBucketsAggregation) aggregation).getBuckets();
		for (MultiBucketsAggregation.Bucket bucket : buckets) {
			Map<String, Object> maps = new HashMap<>();
			maps.put(field.getFieldName(), bucket.getKeyAsString());// 根节点元素提取
			if (field.getChildrenField() != null && field.getChildrenField().size() > 0) {
				for (SearchField child : field.getChildrenField()) {
					Aggregation childterms = bucket.getAggregations().get("aggs" + child.getFieldName());
					List<Map<String, Object>> childResult = getMultiBucketsMap(child, childterms);// 根节点的结果
					maps.put(child.getFieldName(), childResult);
					result.add(maps);
				}
			} else {
				maps.put("doc_count", bucket.getDocCount());// 根节点元素提取
				result.add(maps);
			}
		}
	}

}
