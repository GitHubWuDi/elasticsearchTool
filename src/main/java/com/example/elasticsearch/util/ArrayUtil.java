package com.example.elasticsearch.util;

import java.util.HashSet;
import java.util.Set;

public class ArrayUtil {
	
	private ArrayUtil() {
		
	}
	
	public static String join(Object[] objArray, String separate) {
		if (objArray.length == 0) {
			return "";
		}

		StringBuilder strBuilder = new StringBuilder();
		int len = objArray.length;
		for (int i = 0; i < len - 1; i++) {
			strBuilder.append(String.valueOf(objArray[i]));
			strBuilder.append(separate);
		}
		strBuilder.append(String.valueOf(objArray[len - 1]));

		return strBuilder.toString();
	}
	
	
	public static Object[] distinct(Object[] objArray)
	{
		Set<Object> result = new HashSet<>();
		for (int i = 0; i < objArray.length; i++) {
			if(!result.contains(objArray[i]))
			{
				result.add(objArray[i]);
			}
		}
		
		return result.toArray();
	}
}
