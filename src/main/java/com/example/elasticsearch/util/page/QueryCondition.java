package com.example.elasticsearch.util.page;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.example.elasticsearch.enums.CompareExpression;
import com.example.elasticsearch.util.ArrayUtil;
import com.example.elasticsearch.util.DateUtil;

public class QueryCondition {

	private CompareExpression compareExpression = CompareExpression.Eq;

	// / <summary>
	// / 字段
	// / </summary>
	private String field;
	
	private Class<?> fieldClazz;

	// / <summary>
	// / 比较值1
	// / </summary>
	private Object value1;

	// / <summary>
	// / 比较值2
	// / </summary>
	private Object value2;
	
	/**
	 * 是否是数字类型
	 * 影响toString方法
	 */
	private boolean numFlag = false;
	
	private String fieldStr;

	private QueryCondition(CompareExpression compare, String field, Object value1) {
		this(compare, field, value1, "");
	}
	
	private QueryCondition(CompareExpression compare, String field, Object value1, String fieldStr) {
		this(compare, field, value1, "", fieldStr);
	}

	private QueryCondition(CompareExpression compare, String field, Object value1, Object value2) {
		this.compareExpression = compare;
		this.field = field;
		this.value1 = value1;
		this.value2 = value2;
	}
	
	public QueryCondition(CompareExpression compare, String field, Object value1, Object value2, String fieldStr) {
		this.compareExpression = compare;
		this.field = field;
		this.value1 = value1;
		this.value2 = value2;
		this.fieldStr = fieldStr;
	}
	
	/**
	 * 专门用作toString方法用的
	 * @param fieldStr
	 * @return
	 */
	public QueryCondition aliasField(String fieldStr){
		this.fieldStr = fieldStr;
		
		return this;
	}
	
	public QueryCondition copy(){
		QueryCondition nCondition = new QueryCondition(this.compareExpression, this.field, this.value1, this.value2);
		nCondition.numFlag = this.numFlag;
		
		return nCondition;
	}

	public static QueryCondition eq(String field, Object value) {
		return new QueryCondition(CompareExpression.Eq, field, value);
	}
	
	public static QueryCondition eq(String field, Object value, boolean isNum) {
		QueryCondition queryCondition = new QueryCondition(CompareExpression.Eq, field, value);
		queryCondition.setNumFlag(isNum);
		return queryCondition;
	}

	public static QueryCondition notEq(String field, Object value) {
		return new QueryCondition(CompareExpression.NotEq, field, value);
	}
	
	public static QueryCondition notEq(String field, Object value, boolean isNum) {
		QueryCondition queryCondition = new QueryCondition(CompareExpression.NotEq, field, value);
		queryCondition.setNumFlag(isNum);
		return queryCondition;
	}
	
	public static QueryCondition in(String field, Collection<?> values) {
		Object[] objs = values.toArray();
		QueryCondition queryCondition = new QueryCondition(CompareExpression.In, field, objs);
		return queryCondition;
	}

	public static QueryCondition in(String field, Collection<?> values, boolean isNum) {
		Object[] objs = values.toArray();
		QueryCondition queryCondition = new QueryCondition(CompareExpression.In, field, objs);
		queryCondition.setNumFlag(isNum);
		return queryCondition;
	}

	public static QueryCondition in(String field, String[] values) {
		return new QueryCondition(CompareExpression.In, field, values);
	}
	
	public static QueryCondition in(String field, String[] values, boolean isNum) {
		QueryCondition queryCondition = new QueryCondition(CompareExpression.In, field, values);
		queryCondition.setNumFlag(isNum);
		return queryCondition;
	}

	public static <Y extends Comparable<? super Y>> QueryCondition between(String field, Y value1, Y value2) {
		Class<? extends Object> class1 = value1.getClass();
		QueryCondition queryCondition = new QueryCondition(CompareExpression.Between, field, value1, value2);
		queryCondition.setFieldClazz(class1);
		
		return queryCondition;
	}
	
	public static <Y extends Comparable<? super Y>> QueryCondition between(String field, Y value1, Y value2, boolean isNum) {
		Class<? extends Object> class1 = value1.getClass();
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Between, field, value1, value2);
		queryCondition.setNumFlag(isNum);
		queryCondition.setFieldClazz(class1);
		return queryCondition;
	}

	public static QueryCondition notNull(String field) {
		return new QueryCondition(CompareExpression.NotNull, field, "");
	}

	public static QueryCondition isNull(String field) {
		return new QueryCondition(CompareExpression.IsNull, field, "");
	}
	
	public static QueryCondition like(String field, String str) {
		return new QueryCondition(CompareExpression.Like, field, str);
	}

	public static QueryCondition likeEnd(String field, String str) {
		return new QueryCondition(CompareExpression.LikeEnd, field, str);
	}

	public static QueryCondition likeBegin(String field, String str) {
		return new QueryCondition(CompareExpression.LikeBegin, field, str);
	}

	public static QueryCondition gt(String field, long value) {
		QueryCondition queryCondition = new QueryCondition(CompareExpression.Gt, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition ge(String field, long value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Ge, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition le(String field, long value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Le, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition lt(String field, long value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Lt, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition gt(String field, int value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Gt, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}
	
	public static QueryCondition gt(String field, Date value)
	{
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Gt, field, value);
		queryCondition.setNumFlag(false);
		
		return queryCondition;
	}
	public static QueryCondition gt(String field, String value)
	{
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Gt, field, value);
		queryCondition.setNumFlag(false);
		
		return queryCondition;
	}
	public static QueryCondition ge(String field, Date value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Ge, field, value);
		queryCondition.setNumFlag(false);
		
		return queryCondition;
	}
	
	public static QueryCondition ge(String field, String value) {
		QueryCondition queryCondition = new QueryCondition(CompareExpression.Ge, field, value);
		queryCondition.setNumFlag(false);
		
		return queryCondition;
	}

	public static QueryCondition ge(String field, int value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Ge, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition le(String field, String value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Le, field, value);
		queryCondition.setNumFlag(false);
		
		return queryCondition;
	}
	
	public static QueryCondition le(String field, int value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Le, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition lt(String field, int value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Lt, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}
	
	public static QueryCondition le(String field, Date value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Le, field, value);
		queryCondition.setNumFlag(false);
		
		return queryCondition;
	}

	public static QueryCondition lt(String field, Date value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Lt, field, value);
		queryCondition.setNumFlag(false);
		
		return queryCondition;
	}

	public static QueryCondition lt(String field, BigDecimal value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Lt, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition gt(String field, BigDecimal value) {
		QueryCondition queryCondition =  new QueryCondition(CompareExpression.Gt, field, value);
		queryCondition.setNumFlag(true);
		
		return queryCondition;
	}

	public static QueryCondition and(QueryCondition condition1, QueryCondition condition2) {
		return new QueryCondition(CompareExpression.And, "", condition1, condition2);
	}

	public static QueryCondition and(QueryCondition condition1, QueryCondition condition2, QueryCondition... conditions) {
		QueryCondition condition = new QueryCondition(CompareExpression.And, "", condition1, condition2);
		for (int i = 0; i < conditions.length; i++) {
			condition = new QueryCondition(CompareExpression.And, "", condition, conditions[i]);
		}

		return condition;
	}

	public static QueryCondition or(QueryCondition condition1, QueryCondition condition2, QueryCondition... conditions) {

		QueryCondition condition = new QueryCondition(CompareExpression.Or, "", condition1, condition2);
		for (int i = 0; i < conditions.length; i++) {
			condition = new QueryCondition(CompareExpression.Or, "", condition, conditions[i]);
		}

		return condition;
	}
	
	public static QueryCondition or(List<QueryCondition> cons) {
		int size = cons.size();
		if (size < 2) {
			return cons.get(0);
		}
		QueryCondition condition = new QueryCondition(CompareExpression.Or, "", cons.get(0), cons.get(1));
		for (int i = 2; i < size; i++) {
			condition = new QueryCondition(CompareExpression.Or, "", condition, cons.get(i));
		}

		return condition;
	}

	public static QueryCondition not(QueryCondition condition) {
		return new QueryCondition(CompareExpression.Not, "", condition);
	}

	public CompareExpression getCompareExpression() {
		return compareExpression;
	}

	public void setCompareExpression(CompareExpression compareExpression) {
		this.compareExpression = compareExpression;
	}

	public String getField() {
		return field;
	}
	
	public String getFieldStr(){
		if(StringUtils.isEmpty(fieldStr)){
			return field;
		}
		
		return fieldStr;
	}

	public void setField(String field) {
		this.field = field;
	}

	public Object getValue1() {
		return value1;
	}

	public void setValue1(Object value1) {
		this.value1 = value1;
	}

	public Object getValue2() {
		return value2;
	}

	public void setValue2(Object value2) {
		this.value2 = value2;
	}

	@Override
	public String toString() {
		switch (getCompareExpression()) {
		case Eq:
			return eqStr();
		case NotEq:
			return notEqStr();
		case Between:
			return betweenStr();
		case In:
			return inStr();
			
		case Like:
			return getFieldStr() + " LIKE '%" + getValue1().toString() + "%'";
		case LikeBegin:
			return getFieldStr() + " LIKE '" + getValue1().toString() + "%'";
		case LikeEnd:
			return getFieldStr() + " LIKE '%" + getValue1().toString() + "'";
		case Le:
			return leStr();
		case Lt:
			return ltStr();
		case Ge:
			return geStr();
		case Gt:
			return gtStr();
		case IsNull:
			return getFieldStr() + " IS NULL";
		case NotNull:
			return getFieldStr() + " IS NOT NULL";
		case And:
			return andStr();
		case Or:
			return orStr();
		case Not:
			return notStr();
		default:
			return "";
		}
	}

	private String notStr() {
		QueryCondition condition5 = (QueryCondition) getValue1();
		return "(NOT (" + condition5.toString() + "))";
	}

	private String orStr() {
		QueryCondition condition3 = (QueryCondition) getValue1();
		QueryCondition condition4 = (QueryCondition) getValue2();
		return "((" + condition3.toString() + ") OR (" + condition4.toString() + "))";
	}

	private String andStr() {
		QueryCondition condition1 = (QueryCondition) getValue1();
		QueryCondition condition2 = (QueryCondition) getValue2();
		return "((" + condition1.toString() + ") AND (" + condition2.toString() + "))";
	}

	private String gtStr() {
		if(numFlag){
			return getFieldStr() + ">" + getValue1().toString() + "";
		} else{
			return getFieldStr() + ">'" + getValue1().toString() + "'";
		}
	}

	private String geStr() {
		if(numFlag){
			return getFieldStr() + ">=" + getValue1().toString() + "";
		} else{
			return getFieldStr() + ">='" + getValue1().toString() + "'";
		}
	}

	private String ltStr() {
		if(numFlag){
			return getFieldStr() + "<" + getValue1().toString() + "";
		} else{
			return getFieldStr() + "<'" + getValue1().toString() + "'";
		}
	}

	private String leStr() {
		if(numFlag){
			return getFieldStr() + "<=" + getValue1().toString() + "";
		} else{
			return getFieldStr() + "<='" + getValue1().toString() + "'";
		}
	}

	private String inStr() {
		Object[] objArray = (Object[]) getValue1();
		if (numFlag) {
			
			String join = ArrayUtil.join(objArray, ",");
			return getFieldStr() + " IN (" + join + ")";
		} else {
			String join = ArrayUtil.join(objArray, "','");
			return getFieldStr() + " IN ('" + join + "')";
		}
	}

	private String betweenStr() {
		if (getValue1() instanceof Date) {
			String dataStr1 = DateUtil.format((Date)getValue1());
			String dataStr2 = DateUtil.format((Date) getValue2());
			return getFieldStr() + " BETWEEN '" + dataStr1 + "' AND '" + dataStr2 + "'";
		} else if(this.numFlag){
			return getFieldStr() + " BETWEEN " + getValue1().toString() + " AND " + getValue2().toString() + "";
		}else{
			return getFieldStr() + " BETWEEN '" + getValue1().toString() + "' AND '" + getValue2().toString() + "'";
		}
	}

	private String notEqStr() {
		if(this.numFlag){
			return getFieldStr() + "<>" + getValue1().toString() + "";
		}else{
			return getFieldStr() + "<>'" + getValue1().toString() + "'";
		}
	}

	private String eqStr() {
		if(this.numFlag){
			return getFieldStr() + "=" + getValue1().toString() + "";
		} else{
			return getFieldStr() + "='" + getValue1().toString() + "'";
		}
	}

	public boolean isNumFlag() {
		return numFlag;
	}

	public void setNumFlag(boolean numFlag) {
		this.numFlag = numFlag;
	}

	public Class<?> getFieldClazz() {
		return fieldClazz;
	}

	public void setFieldClazz(Class<?> fieldClazz) {
		this.fieldClazz = fieldClazz;
	}
}
