package com.example.elasticsearch.service;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.example.elasticsearch.model.PrimaryKey;
import com.google.gson.Gson;


/**
* @author wudi
* @version 创建时间：2018年7月31日 下午10:38:46
* @ClassName ElasticSearchService
* @Description ES相关业务实现类(标准：不在该类中使用elasticsearch原生API)
*/
public abstract class ElasticSearchService<T> {

	private static Logger logger = Logger.getLogger(ElasticSearchService.class);
	private Gson gson = new Gson();
	@Autowired
	private ElasticSearchManage elasticSearchManage;
	
	protected Class<T> clazz;
	protected String idField;
	
	/**
	 * 构造函数
	 */
	public ElasticSearchService(){
		// 反射得到T的真实类型
		ParameterizedType parameterizedType = (ParameterizedType)this.getClass().getGenericSuperclass(); //获取当前new的对象的泛型的父类的类型
		this.clazz = (Class<T>)parameterizedType.getActualTypeArguments()[0]; //获取第一个类型参数的真实类型model = clazz.newInstance();实例化需要的时候添加
		this.idField = clazz.getAnnotation(PrimaryKey.class).value();
	}
	
	/**
	 * 获得索引名称
	 * @return
	 */
	public abstract String getIndexName();
	
	/**
	 * 获得索引type
	 * @return
	 */
	public abstract String getType();
	
	/**
	 * 查看索引是否存在
	 * @return
	 */
	public Boolean isEsIndexExist(){
		String indexName = getIndexName();
		Boolean result = elasticSearchManage.checkEsIndexState(indexName);
		return result;
	}
	
	/**
	 * 检查索引状态（OPEN OR CLOSE）
	 * @return
	 */
	public String checkESIndexState(){
		String indexName = getIndexName();
		String indexStatus = elasticSearchManage.checkIndexStatus(indexName);
		return indexStatus;
	}
	
	
	/**
	 * 刷新索引
	 */
	public void refreshIndex(){
		String indexName = getIndexName();
		elasticSearchManage.refreshIndexByIndexName(indexName);
	}
	
	/**
	 * 创建索引
	 * @param shardCount
	 * @param repliceCount
	 * @return
	 */
	public Boolean createIndex(int shardCount,int repliceCount){
		Field[] fields = clazz.getDeclaredFields();
		String indexName = getIndexName();
		String type = getType();
		Boolean result = elasticSearchManage.createEsIndex(indexName, type, shardCount, repliceCount, fields);
		return result;
	}
	
	/**
	 * 根据id获得对应的doc
	 * @param id
	 * @return
	 */
	public T getDoc(Serializable id){
		if(id==null){
			return null;
		}else{
			String indexName = getIndexName();
			String type = getType();
			Map<String, Object> map = elasticSearchManage.getDoc(indexName, type, id.toString());
			String json = gson.toJson(map);
			T t = gson.fromJson(json, clazz);
			return t;
		}
	}
	
}
