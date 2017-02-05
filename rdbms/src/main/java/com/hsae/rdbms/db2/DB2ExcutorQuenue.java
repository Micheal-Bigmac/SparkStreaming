package com.hsae.rdbms.db2;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * @ClassName: DB2DataHelper
 * @Description: db2数据工具 这个类是用来在单线程模式下 执行sql 任务队列排队
 */
public class DB2ExcutorQuenue {
	private final static Logger LOG = LoggerFactory.getLogger(DB2ExcutorQuenue.class);

	public static Connection isExstConnection(Connection connection) {
		if (connection == null) { 
			try {
				connection= DB2ConnectionPool.getInstance().getConnection();
			} catch (SQLException e) {
				LOG.error("get Connection cautch one error and set Connection = null");
				connection=null;
			}
		}
		return connection;
	}

	/**
	 * @Title: log @Description: 输出错误日志，如果是SQLException，则把相信错误也输出出来 @param
	 *         m @param e void @author: 韩欣宇 @date 2015年7月29日 下午1:01:40 @throws
	 */
	private static void logError(String m, Exception e) {
		LOG.error(m, e);
		if (e instanceof SQLException) {
			SQLException sqle = (SQLException) e;
			if (sqle.getNextException() != null && !sqle.equals(sqle.getNextException())) {
				logError(m + " and next exception is", sqle.getNextException());
			}
		}
	}

	/**
	 * @Title: getConnection @Description: 从连接池过去链接 @return @throws SQLException
	 *         Connection @author: 韩欣宇 @date 2015年7月24日 下午2:54:32 @throws
	 */
	public static Connection getConnection() throws SQLException {
		return DB2ConnectionPool.getInstance().getConnection();
	}

	/**
	 * @Title: closeConnection @Description: 关闭连接，不throw异常，因为即使发生异常，影响也不大 @param
	 *         connection void @author: 韩欣宇 @date 2015年7月24日 下午2:54:47 @throws
	 */
	public static void closeConn(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e1) {
			logError("close connection error! ", e1);
		}
	}

	/**
	 * @Title: rollbackConn @Description: 回滚连接 @param connection void @author:
	 *         韩欣宇 @date 2015年7月24日 下午2:59:02 @throws
	 */
	public static void rollbackConn(Connection connection) {
		try {
			if (connection != null) {
				connection.rollback();
				LOG.warn("connection rollback!");
			}
		} catch (SQLException e) {
			logError("rollback has an error! ", e);
		}
	}

	/**
	 * @Title: rollbackConn @Description: 回滚连接并记录错误日志 @param connection @param
	 *         error void @author: 韩欣宇 @date 2015年7月24日 下午2:59:20 @throws
	 */
	public static void rollbackConn(Connection connection, Exception error) {
		logError("Connection has an error! ", error);
		try {
			if (connection != null) {
				connection.rollback();
				LOG.warn("connection rollback!");
			}
		} catch (SQLException e) {
			logError("rollback has an error! ", e);
		}
	}

	/**
	 * @throws SQLException
	 * 			@Title: executeNonQuery @Description: 执行sql @param
	 *             sql @return int 受影响的行数 @author: 韩欣宇 @date 2015年6月25日
	 *             上午11:07:18 @throws
	 */
	public static int executeNonQuery(String sql, Connection connection) throws SQLException {
		if (sql == null || sql.trim() == "") {
			return 0;
		}
		int returnValue = -1;
		try {
			Statement stmt = connection.createStatement();
			returnValue = stmt.executeUpdate(sql);
			return returnValue;
		} catch (SQLException e) {
			rollbackConn(connection, e);
			throw e;
		}
	}

	/**
	 * @throws SQLException
	 * 			@Title: excuteBatch @Description: 批量执行sql @param
	 *             sqlArrary @return int[] 受影响的行数列表 @author: 韩欣宇 @date
	 *             2015年6月25日 上午11:07:53 @throws
	 */
	public static int[] excuteBatch(List<String> sqlArrary, Connection connection) throws SQLException {
		try {
			if(connection==null||connection.isClosed()){
				connection= DB2ConnectionPool.getInstance().getConnection();
			}
			int[] result = null;
			connection.setAutoCommit(false);
			Statement stmt = connection.createStatement();
			for (String sql : sqlArrary) {
				if (sql != null && !"".equals(sql.trim())) {
					stmt.addBatch(sql);
				}
			}
			result = stmt.executeBatch();
			connection.commit();
			return result;
		} catch (SQLException e) {
			rollbackConn(connection, e);
			throw e;
		}
	}

	/**
	 * @Title: setObject @Description: 设置预编译语句的值 @param ps @param
	 *         parameterIndex @param column @param value @throws SQLException
	 *         void @author: 韩欣宇 @date 2015年7月24日 下午2:53:29 @throws
	 */
	private static void setObject(PreparedStatement ps, int parameterIndex, Column column, Object value) throws SQLException {
		if (column.getScaleOrLength() != 0) {
			ps.setObject(parameterIndex, value, column.getType(), column.getScaleOrLength());
		} else {
			ps.setObject(parameterIndex, value, column.getType());
		}
	}

	/**
	 * @Title: setObject @Description: 设置存储过程的值 @param ps @param
	 *         parameterIndex @param column @param value @throws SQLException
	 *         void @author: 韩欣宇 @date 2015年7月24日 下午2:53:29 @throws
	 */
	private static void setObject(CallableStatement ps, int parameterIndex, Column column, Object value) throws SQLException {
		if (column.getScaleOrLength() != 0) {
			ps.setObject(parameterIndex, value, column.getType(), column.getScaleOrLength());
		} else {
			ps.setObject(parameterIndex, value, column.getType());
		}
	}

	/**
	 * @Title: excuteBatchPreparedSqlAndValues @Description:
	 *         批量执行PreparedSqlAndValues，用于同一事物向单张表插入数据 @param
	 *         preparedSqlAndValues @throws IllegalArgumentException @throws
	 *         SQLException void @author: 韩欣宇 @date 2015年6月25日
	 *         上午11:09:50 @throws
	 */
	private static int excuteBatchPreparedInsertSql(Insert preparedSqlAndValues, Connection connection) throws IllegalArgumentException, SQLException {
		int count = 0;
		PreparedStatement ps = null;
		ps = connection.prepareStatement(preparedSqlAndValues.getPreparedSql());
		Column[] columns = preparedSqlAndValues.getColumns();
		List<Map<Integer, Object>> parameterIndex2ValueMapList = preparedSqlAndValues.getParameterIndex2ValueMapList();
		for (int j = 0; j < parameterIndex2ValueMapList.size(); j++) {
			Map<Integer, Object> parameterIndex2ValueMap = parameterIndex2ValueMapList.get(j);
			for (Entry<Integer, Object> e : parameterIndex2ValueMap.entrySet()) {
				int parameterIndex = e.getKey();
				if (parameterIndex < 1 || parameterIndex > columns.length) {
					throw new IllegalArgumentException("PreparedStatement's parameterIndex error, and parameterIndex=" + parameterIndex + " columns.length=" + columns.length);
				} else {
					Column column = columns[parameterIndex - 1];
					Object value = e.getValue();
					setObject(ps, parameterIndex, column, value);
				}
			}
			ps.addBatch();
		}
		int[] results = ps.executeBatch();
		for (int i = 0; i < results.length; i++) {
			count += results[i];
		}
		return count;
	}

	/**
	 * @return @Title: excuteBatchPreparedSqlAndValuesList @Description:
	 *         批量执行多个PreparedSqlAndValues，用于同一事物向多张表插入数据 @param
	 *         preparedSqlAndValuesList @throws IllegalArgumentException @throws
	 *         SQLException void @author: 韩欣宇 @date 2015年6月25日
	 *         上午11:10:07 @throws
	 */
	private static Integer[] excuteBatchPreparedInsertSql(List<Insert> preparedSqlAndValuesList, Connection connection) throws IllegalArgumentException, SQLException {
		List<Integer> result = new ArrayList<Integer>();
		for (Insert preparedSqlAndValues : preparedSqlAndValuesList) {
			PreparedStatement ps = connection.prepareStatement(preparedSqlAndValues.getPreparedSql());
			Column[] columns = preparedSqlAndValues.getColumns();
			List<Map<Integer, Object>> parameterIndex2ValueMapList = preparedSqlAndValues.getParameterIndex2ValueMapList();
			for (int j = 0; j < parameterIndex2ValueMapList.size(); j++) {
				Map<Integer, Object> parameterIndex2ValueMap = parameterIndex2ValueMapList.get(j);
				for (Entry<Integer, Object> e : parameterIndex2ValueMap.entrySet()) {
					int parameterIndex = e.getKey();
					if (parameterIndex < 1 || parameterIndex > columns.length) {
						throw new IllegalArgumentException("PreparedStatement's parameterIndex error, and parameterIndex=" + parameterIndex + " columns.length=" + columns.length);
					} else {
						Column column = columns[parameterIndex - 1];
						Object value = e.getValue();
						setObject(ps, parameterIndex, column, value);
					}
				}
				ps.addBatch();
			}
			int[] results = ps.executeBatch();
			int count = 0;
			for (int i = 0; i < results.length; i++) {
				count += results[i];
			}
			result.add(count);
		}
		return result.toArray(new Integer[0]);
	}

	/**
	 * @Title: excutePreparedUpdateSqlAndValues @Description:
	 *         一条预编译sql更新多行的update语句，其中由于SQLCODE=-418,
	 *         SQLSTATE=42610原因，不得不将部分值直接拼入sql语句<br />
	 *         近支持根据单个where条件更新多行 @param p @return @throws
	 *         IllegalArgumentException @throws SQLException int @author:
	 *         韩欣宇 @date 2015年7月24日 下午2:46:53 @throws
	 */
	private static int excutePreparedUpdateSql(Update p, Connection connection) throws IllegalArgumentException, SQLException {
		int count = 0;
		PreparedStatement ps = null;
		Column whereColumn = p.getWhereColumn();
		Column[] updateColumns = p.getUpdateColumns();
		int updateColumnsLength = updateColumns.length;
		Map<Object, Object[]> where2UpdateValueMap = p.getWhere2UpdateValueMap();
		int where2UpdateValueMapSize = where2UpdateValueMap.size();
		Collection<Object[]> values = where2UpdateValueMap.values();
		for (Object[] objects : values) {
			if (updateColumnsLength != objects.length) {
				throw new IllegalArgumentException("PreparedUpdateSqlAndValues's values length error, and updateColumns.length=" + updateColumnsLength + " objects.length=" + objects.length);
			}
		}
		String preparedUpdateSql = SQLUtils.buildPreparedUpdateSqlWithUpdateColumnValues(p);
		ps = connection.prepareStatement(preparedUpdateSql);
		int mapIndex = 1;
		for (Object where : where2UpdateValueMap.keySet()) {
			for (int j = 0; j < updateColumnsLength; j++) {
				int parameterIndex = mapIndex + j * where2UpdateValueMapSize;
				setObject(ps, parameterIndex, whereColumn, where);
			}
			int parameterIndex = mapIndex + updateColumnsLength * where2UpdateValueMapSize;
			setObject(ps, parameterIndex, whereColumn, where);
			mapIndex++;
		}
		count = ps.executeUpdate();
		return count;
	}

	/**
	 * @Title: excutePreparedUpdateSqlAndValues4MultiConditional @Description:
	 *         一条预编译sql更新多行的update语句，其中由于SQLCODE=-418,
	 *         SQLSTATE=42610原因，不得不将部分值直接拼入sql语句<br />
	 *         支持根据多个where条件更新多行 @param p @return @throws
	 *         IllegalArgumentException @throws SQLException int @author:
	 *         韩欣宇 @date 2015年7月24日 下午2:47:28 @throws
	 */
	private static int excutePreparedUpdateSql2(Update2 p, Connection connection) throws IllegalArgumentException, SQLException {
		int count = 0;
		PreparedStatement ps = null;
		Column[] whereColumns = p.getWhereColumns();
		int whereColumnLength = whereColumns.length;
		Column[] updateColumns = p.getUpdateColumns();
		int updateColumnsLength = updateColumns.length;
		Map<Object[], Object[]> where2UpdateValueMap = p.getWhere2UpdateValueMap();
		int where2UpdateValueMapSize = where2UpdateValueMap.size();
		Set<Object[]> keySet = where2UpdateValueMap.keySet();
		Collection<Object[]> values = where2UpdateValueMap.values();
		for (Object[] objects : keySet) {
			if (whereColumnLength != objects.length) {
				throw new IllegalArgumentException("PreparedUpdateSqlAndValues's whereColumns length error, and whereColumns.length=" + whereColumnLength + " objects.length=" + objects.length);
			}
		}
		for (Object[] objects : values) {
			if (updateColumnsLength != objects.length) {
				throw new IllegalArgumentException("PreparedUpdateSqlAndValues's values length error, and updateColumns.length=" + updateColumnsLength + " objects.length=" + objects.length);
			}
		}
		String preparedUpdateSql = SQLUtils.buildPreparedUpdateSqlWithUpdateColumnValues(p);
		ps = connection.prepareStatement(preparedUpdateSql);
		int mapIndex = 1;
		for (Object[] wheres : keySet) {
			for (int i = 0; i < wheres.length; i++) {
				for (int j = 0; j < updateColumnsLength; j++) {
					int parameterIndex = (mapIndex - 1) * whereColumnLength + whereColumnLength * j * where2UpdateValueMapSize + i + 1;
					setObject(ps, parameterIndex, whereColumns[i], wheres[i]);
				}
				int parameterIndex = (mapIndex - 1) * whereColumnLength + whereColumnLength * updateColumnsLength * where2UpdateValueMapSize + i + 1;
				setObject(ps, parameterIndex, whereColumns[i], wheres[i]);
			}
			mapIndex++;
		}
		count = ps.executeUpdate();
		return count;
	}

	/**
	 * @Title: excuteSmipleProcedure @Description: 执行简单的存储过程 @param
	 *         smipleProcedure @return @throws IllegalArgumentException @throws
	 *         SQLException int @author: 韩欣宇 @date 2015年8月17日 下午4:31:43 @throws
	 */
	public static int excuteSmipleProcedure(SmipleProcedure smipleProcedure, Connection connection) throws IllegalArgumentException, SQLException {
		int count = 0;
		CallableStatement cstmt = null;
		try {
			cstmt = connection.prepareCall(SQLUtils.buildCallableStatement(smipleProcedure));
			Column[] ins = smipleProcedure.getIns();
			Map<Integer, Object> parameterIndex2ValueMap = smipleProcedure.getParameterIndex2ValueMap();
			for (Entry<Integer, Object> e : parameterIndex2ValueMap.entrySet()) {
				int parameterIndex = e.getKey();
				if (parameterIndex < 1 || parameterIndex > ins.length) {
					throw new IllegalArgumentException("CallableStatement parameterIndex error, and parameterIndex=" + parameterIndex + " ins.length=" + ins.length);
				} else {
					Column column = ins[parameterIndex - 1];
					Object value = e.getValue();
					setObject(cstmt, parameterIndex, column, value);
				}
			}
			count = cstmt.executeUpdate();
			return count;
		} catch (SQLException e) {
			logError("Connection has an Procedure error! ", e);
			return 0;
		} finally {
			if (cstmt != null) {
				cstmt.close();
			}
		}
	}

	/**
	 * @Title: excuteBatchs @Description:
	 *         在一个链接一个事物里执行多个update、insert、delete @param ps @return @throws
	 *         IllegalArgumentException @throws SQLException int @author:
	 *         韩欣宇 @date 2015年7月27日 下午3:13:04 @throws
	 */
	public static int excuteBatchs(List<PreparedSqlAndValues> ps, Connection connection) throws IllegalArgumentException, SQLException {
		List<Insert> inserts = new ArrayList<Insert>();
		List<Delete> deletes = new ArrayList<Delete>();
		List<Update> updates = new ArrayList<Update>();
		List<Update2> updates2s = new ArrayList<Update2>();
		List<SmipleProcedure> smipleProcedures = new ArrayList<SmipleProcedure>();
		for (PreparedSqlAndValues p : ps) {
			if (p instanceof Insert) {
				inserts.add((Insert) p);
			} else if (p instanceof Delete) {
				deletes.add((Delete) p);
			} else if (p instanceof Update) {
				updates.add((Update) p);
			} else if (p instanceof Update2) {
				updates2s.add((Update2) p);
			} else if (p instanceof SmipleProcedure) {
				smipleProcedures.add((SmipleProcedure) p);
			}
		}
		int count = 0;
		try {
			connection.setAutoCommit(false);
			if (inserts.size() > 0) {
				Integer[] res = excuteBatchPreparedInsertSql(inserts, connection);
				for (int i = 0; i < res.length; i++) {
					count += res[i];
				}
			}
			if (deletes.size() > 0) {
				// TODO 待完善
			}
			if (updates.size() > 0) {
				for (Update update : updates) {
					count += excutePreparedUpdateSql(update, connection);
				}
			}
			if (updates2s.size() > 0) {
				for (Update2 update2 : updates2s) {
					count += excutePreparedUpdateSql2(update2, connection);
				}
			}
			if (smipleProcedures.size() > 0) {
				for (SmipleProcedure smipleProcedure : smipleProcedures) {
					count += excuteSmipleProcedure(smipleProcedure, connection);
				}
			}
			connection.commit();
			return count;
		} catch (SQLException e) {
			rollbackConn(connection, e);
			LOG.error("The data which causes the above error is " + JSON.toJSONString(ps));
			throw e;
		}
	}
}
