package com.hsae.rdbms.db2;

import java.util.Map;

public class Update implements PreparedSqlAndValues {
	private String tableName;
	private Column whereColumn;
	private Column[] updateColumns;
	private Map<Object, Object[]> where2UpdateValueMap;

	public Update(String tableName, Column whereColumn, Column[] updateColumns, Map<Object, Object[]> where2UpdateValueMap) {
		super();
		this.tableName = tableName;
		this.whereColumn = whereColumn;
		this.updateColumns = updateColumns;
		this.where2UpdateValueMap = where2UpdateValueMap;
	}

	public String getTableName() {
		return tableName;
	}

	public Column getWhereColumn() {
		return whereColumn;
	}

	public Column[] getUpdateColumns() {
		return updateColumns;
	}

	public Map<Object, Object[]> getWhere2UpdateValueMap() {
		return where2UpdateValueMap;
	}

}