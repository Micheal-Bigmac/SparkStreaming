package com.hsae.rdbms.db2;

import java.util.Map;

/**
 * @ClassName: SmipleProcedure
 * @Description: 只有in型参数并且没有返回值的简单存储过程
 * @author: 韩欣宇
 * @company: 上海航盛实业有限公司
 * @date 2015年8月17日 下午4:44:42
 */
public class SmipleProcedure implements PreparedSqlAndValues {
	private String name;
	private Column[] ins;
	private Map<Integer, Object> parameterIndex2ValueMap;

	public SmipleProcedure(String name, Column[] ins, Map<Integer, Object> parameterIndex2ValueMap) {
		super();
		this.name = name;
		this.ins = ins;
		this.parameterIndex2ValueMap = parameterIndex2ValueMap;
	}

	public String getName() {
		return name;
	}

	public Column[] getIns() {
		return ins;
	}

	public Map<Integer, Object> getParameterIndex2ValueMap() {
		return parameterIndex2ValueMap;
	}

}
