package com.hsae.rdbms.db2;

import java.util.List;
import java.util.Map;

public class Insert implements PreparedSqlAndValues {
	private String preparedSql;
	private Column[] columns;
	private List<Map<Integer, Object>> parameterIndex2ValueMapList;

	public Insert(String preparedSql, Column[] columns, List<Map<Integer, Object>> parameterIndex2ValueMapList) {
		super();
		this.preparedSql = preparedSql;
		this.columns = columns;
		this.parameterIndex2ValueMapList = parameterIndex2ValueMapList;
	}

	public String getPreparedSql() {
		return preparedSql;
	}

	public Column[] getColumns() {
		return columns;
	}

	public List<Map<Integer, Object>> getParameterIndex2ValueMapList() {
		return parameterIndex2ValueMapList;
	}

}
