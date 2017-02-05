package com.hsae.rdbms.db2;

import java.util.Collection;

import com.hsae.rdbms.common.CommonTools;

public class SQLUtils {

	/**
	 * @Title: buildPreparedUpdateSqlWithUpdateColumnValues
	 * @Description: 拼接一条语句更新多行的update语句，其中由于SQLCODE=-418, SQLSTATE=42610原因，不得不将部分值直接拼入sql语句<br />
	 *               近支持根据单个where条件更新多行
	 * @param p
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:46:10
	 */
	public static String buildPreparedUpdateSqlWithUpdateColumnValues(Update p) {
		String tableName = p.getTableName();
		Column[] updateColumns = p.getUpdateColumns();
		Column whereColumn = p.getWhereColumn();
		String whereColumnName = whereColumn.getName();
		int size = p.getWhere2UpdateValueMap().size();
		Collection<Object[]> updateValues = p.getWhere2UpdateValueMap().values();
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(tableName).append(" SET ").append(buildPreparedSetSqlWithUpdateColumnValues(whereColumnName, updateColumns, updateValues))
				.append(" WHERE " + whereColumnName + " IN (").append(buildQuestionMarks(size)).append(")");
		return sb.toString();
	}

	/**
	 * @Title: buildPreparedSetSqlWithUpdateColumnValues
	 * @Description: F_VEHICLE_ID = <br />
	 *               (CASE F_ID <br />
	 *               WHEN ? THEN 20 <br />
	 *               WHEN ? THEN 16 <br />
	 *               WHEN ? THEN 18 <br />
	 *               ELSE NULL <br />
	 *               END), <br />
	 *               F_ID = <br />
	 *               (CASE F_ID <br />
	 *               WHEN ? THEN 21 <br />
	 *               WHEN ? THEN 17 <br />
	 *               WHEN ? THEN 19 <br />
	 *               ELSE NULL <br />
	 *               END) <br />
	 * @param whereColumnName
	 * @param updateColumns
	 * @param values
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:35:21
	 */
	private static String buildPreparedSetSqlWithUpdateColumnValues(String whereColumnName, Column[] updateColumns, Collection<Object[]> values) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0, length = updateColumns.length; i < length; i++) {
			sb.append(updateColumns[i].getName() + "=(CASE ").append(whereColumnName).append(" ");
			for (Object[] objects : values) {
				sb.append("WHEN ? THEN " + objectToSqlString(objects[i]) + " ");
			}
			sb.append("ELSE NULL END)");
			if (i != length - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}

	/**
	 * @Title: buildPreparedUpdateSqlWithUpdateColumnValues
	 * @Description: 拼接一条语句更新多行的update语句，其中由于SQLCODE=-418, SQLSTATE=42610原因，不得不将部分值直接拼入sql语句<br />
	 *               支持根据多个where条件更新多行
	 * @param p
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:43:38
	 */
	public static String buildPreparedUpdateSqlWithUpdateColumnValues(Update2 p) {
		String tableName = p.getTableName();
		Column[] updateColumns = p.getUpdateColumns();
		Column[] whereColumns = p.getWhereColumns();
		int size = p.getWhere2UpdateValueMap().size();
		Collection<Object[]> updateValues = p.getWhere2UpdateValueMap().values();
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(tableName).append(" SET ").append(buildPreparedSetSqlWithUpdateColumnValues(whereColumns, updateColumns, updateValues)).append(" WHERE ");
		for (int i = 0; i < size; i++) {
			sb.append("(" + buildPreparedAndSql(whereColumns) + ")");
			if (i != size - 1) {
				sb.append(" OR ");
			}
		}
		return sb.toString();
	}

	/**
	 * @Title: buildPreparedSetSqlWithUpdateColumnValues
	 * @Description: F_VEHICLE_ID = <br />
	 *               (CASE <br />
	 *               WHEN F_ID = ? AND F_PLATE_CODE = ? THEN 12 <br />
	 *               WHEN F_ID = ? AND F_PLATE_CODE = ? THEN 16 <br />
	 *               WHEN F_ID = ? AND F_PLATE_CODE = ? THEN 18 <br />
	 *               ELSE NULL <br />
	 *               END), <br />
	 *               F_TOTAL = <br />
	 *               (CASE <br />
	 *               WHEN F_ID = ? AND F_PLATE_CODE = ? THEN 21 <br />
	 *               WHEN F_ID = ? AND F_PLATE_CODE = ? THEN 17 <br />
	 *               WHEN F_ID = ? AND F_PLATE_CODE = ? THEN 19 <br />
	 *               ELSE NULL <br />
	 *               END) <br />
	 * @param whereColumns
	 * @param updateColumns
	 * @param values
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:39:32
	 */
	private static String buildPreparedSetSqlWithUpdateColumnValues(Column[] whereColumns, Column[] updateColumns, Collection<Object[]> values) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0, length = updateColumns.length; i < length; i++) {
			sb.append(updateColumns[i].getName() + "=(CASE ");
			for (Object[] objects : values) {
				sb.append("WHEN ");
				sb.append(buildPreparedAndSql(whereColumns));
				sb.append("THEN " + objectToSqlString(objects[i]) + " ");
			}
			sb.append("ELSE NULL END)");
			if (i != length - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}

	/**
	 * @Title: buildPreparedAndSql
	 * @Description: 拼接后如： F_ID = ? AND F_PLATE_CODE = ?
	 * @param whereColumns
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:41:08
	 */
	private static String buildPreparedAndSql(Column[] whereColumns) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < whereColumns.length; i++) {
			sb.append(whereColumns[i].getName() + "=? ");
			if (i != whereColumns.length - 1) {
				sb.append("AND ");
			}
		}
		return sb.toString();
	}

	/**
	 * @Title: objectToSqlString
	 * @Description: 把object类型转换成sql语句中的string，主要处理string、date类型
	 * @param o
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:42:11
	 */
	private static String objectToSqlString(Object o) {
		String s = "null";
		if (o instanceof String) {
			s = "'" + o.toString().replace("'", "''") + "'";
		} else if (o instanceof java.util.Date) {
			s = "'" + CommonTools.DATEFORMAT_WITH_3_MILLISECOND.format((java.util.Date) o) + "'";
		} else if (o instanceof java.sql.Date) {
			s = "'" + CommonTools.DATEFORMAT_WITH_3_MILLISECOND.format((java.sql.Date) o) + "'";
		} else {
			s = o.toString();
		}
		return String.valueOf(s);
	}

	/**
	 * @Title: buildInsertPreparedSql
	 * @Description: 拼接insert的预编译语句，values只有一组
	 * @param tableName
	 * @param columns
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年6月25日 上午10:38:22
	 */
	public static String buildPreparedInsertSql(String tableName, Column[] columns) {
		return buildPreparedInsertSql(tableName, columns, 1);
	}

	/**
	 * @Title: buildInsertPreparedSql
	 * @Description: 拼接insert的预编译语句
	 * @param tableName
	 * @param columns
	 * @param count
	 *            values的组的个数
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年6月25日 上午10:39:01
	 */
	public static String buildPreparedInsertSql(String tableName, Column[] columns, int count) {
		if (tableName == null || tableName.length() == 0 || columns == null || columns.length == 0 || count <= 0) {
			throw new IllegalArgumentException("argument is wrong");
		}
		StringBuilder sb = new StringBuilder("INSERT INTO ");
		sb.append(tableName).append("(").append(buildFieldNamesSql(columns)).append(") VALUES ");
		for (int i = 0; i < count; i++) {
			sb.append("(").append(buildQuestionMarks(columns.length)).append(")");
			if (i != count - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}

	/**
	 * @Title: buildFieldNamesSql
	 * @Description: 用逗号连接的字段名字符串
	 * @param columns
	 * @return Object
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:43:15
	 */
	private static Object buildFieldNamesSql(Column[] columns) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0, length = columns.length; i < length; i++) {
			if (i != length - 1) {
				sb.append(columns[i].getName()).append(',');
			} else {
				sb.append(columns[i].getName());
			}
		}
		return sb.toString();
	}

	/**
	 * @Title: buildCallableStatement
	 * @Description: 拼接简单的存储过程的执行语句
	 * @param s
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年8月17日 下午4:44:27
	 */
	public static String buildCallableStatement(SmipleProcedure s) {
		return new StringBuilder("CALL ").append(s.getName()).append(" (").append(buildQuestionMarks(s.getIns().length)).append(")").toString();
	}

	/**
	 * @Title: buildQuestionMarks
	 * @Description: 用逗号拼接n个问号
	 * @param count
	 * @return String
	 * @author: 韩欣宇
	 * @date 2015年7月24日 下午2:42:50
	 */
	private static String buildQuestionMarks(int count) {
		char[] res = new char[count * 2 - 1];
		for (int i = 0; i < res.length; i++) {
			if (i % 2 == 0) {
				res[i] = '?';
			} else {
				res[i] = ',';
			}
		}
		return new String(res);
	}

}
