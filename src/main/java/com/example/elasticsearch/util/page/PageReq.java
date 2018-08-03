package com.example.elasticsearch.util.page;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

import lombok.Data;
/**
* @author wudi
* @version 创建时间：2018年7月28日 下午3:49:48
* @ClassName PageReq
* @Description pageReq申请元素
*/
@Data
public class PageReq {
	private String order_;    // 排序字段
	private String by_;   // 排序顺序
	private Integer start_;
	private Integer count_;
	
	public Sort getPageSort() {
		if(getOrder_() == null) {
			return null;
		}
		Direction pageDirection = getPageDirection();
		List<String> sorts = new ArrayList<>();
		sorts.add(getOrder_());
		return new Sort(pageDirection, sorts);
	}
	
	public Direction getPageDirection() {
		Direction direction = Direction.DESC;
		if("asc".equals(by_)) {
			direction = Direction.ASC;
		}
		
		return direction;
	}
	
	public Pageable getPageable() {
		Sort pageSort = getPageSort();
		if(pageSort == null) {
			return new PageRequest(start_, count_);
		} else {
			return new PageRequest(start_, count_, pageSort);
		}
	}

}
