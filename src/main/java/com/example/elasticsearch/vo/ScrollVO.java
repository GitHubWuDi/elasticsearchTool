package com.example.elasticsearch.vo;

import java.util.List;

import lombok.Data;

/**
 * 游标返回数据页
 * @author wd-pc
 *
 * @param <T>
 */
@Data
public class ScrollVO<T> {

	private List<T> list;
	private String scrollId;
	private long total;
}
