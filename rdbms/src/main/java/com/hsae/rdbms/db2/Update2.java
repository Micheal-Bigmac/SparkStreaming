package com.hsae.rdbms.db2;

import java.util.Map;

public class Update2 implements PreparedSqlAndValues {
	private String tableName;
	private Column[] whereColumns;
	private Column[] updateColumns;
	private Map<Object[], Object[]> where2UpdateValueMap;

	public Update2(String tableName, Column[] whereColumns, Column[] updateColumns, Map<Object[], Object[]> where2UpdateValueMap) {
		super();
		this.tableName = tableName;
		this.whereColumns = whereColumns;
		this.updateColumns = updateColumns;
		this.where2UpdateValueMap = where2UpdateValueMap;
	}

	public String getTableName() {
		return tableName;
	}

	public Column[] getWhereColumns() {
		return whereColumns;
	}

	public Column[] getUpdateColumns() {
		return updateColumns;
	}

	public Map<Object[], Object[]> getWhere2UpdateValueMap() {
		return where2UpdateValueMap;
	}

}