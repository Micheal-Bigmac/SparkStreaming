package com.hsae.rdbms.db2;

public class Delete implements PreparedSqlAndValues {
	private String tableName;
	private Column[] whereColumns;

	public Delete(String tableName, Column[] whereColumns) {
		super();
		this.tableName = tableName;
		this.whereColumns = whereColumns;
	}

	public String getTableName() {
		return tableName;
	}

	public Column[] getWhereColumns() {
		return whereColumns;
	}

}