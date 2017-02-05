/**
 * @ClassName 
 * @Description 
 * @author 李小辉
 * @company 上海势航网络科技有限公司
 * @date 2016年9月27日
 */
package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.*;
import com.sxp.task.protobuf.generated.GpsInfo.CFBEngine;
import com.sxp.task.protobuf.generated.GpsInfo.Gps;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class CFBDiagnoseDB2Bolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(CFBDiagnoseDB2Bolt.class);
	private static Connection connection = null;
	static String TABLE_NAME = "T_FBDIAGNOSISENGINECUR";
	public static final Column WHERE_COLUMNS = new Column("F_VEHICLE_ID", Types.BIGINT);
	public static Column[] COLUMNS = new Column[] { new Column("F_VEHICLE_ID", Types.BIGINT), // 2
			new Column("F_KZZS", Types.DOUBLE), // 3
			new Column("F_KZNJ", Types.SMALLINT), // 4
			new Column("F_GZD2ZS", Types.DOUBLE), // 5
			new Column("F_GZD2NJ", Types.SMALLINT), // 6
			new Column("F_GZD3ZS", Types.DOUBLE), // 7
			new Column("F_GZD3NJ", Types.SMALLINT), // 8
			new Column("F_GZD4ZS", Types.DOUBLE), // 9
			new Column("F_GZD4NJ", Types.SMALLINT), // 10
			new Column("F_GZD5ZS", Types.DOUBLE), // 11
			new Column("F_GZD5NJ", Types.SMALLINT), // 12
			new Column("F_GZD6ZGZS", Types.DOUBLE), // 13
			new Column("F_TSQKP", Types.DOUBLE), // 14
			new Column("F_CKNJ", Types.SMALLINT), // 15
			new Column("F_GZD7CZZS", Types.DOUBLE), // 16
			new Column("F_ZDSDSEC", Types.DOUBLE), // 17
			new Column("F_ZDZS", Types.SMALLINT), // 18
			new Column("F_ZGZS", Types.SMALLINT), // 19
			new Column("F_ZDNJ", Types.SMALLINT), // 20
			new Column("F_ZGNJ", Types.SMALLINT), // 21
			new Column("F_RECORDTIME", Types.TIMESTAMP), // 22
			new Column("F_GPSTIME", Types.TIMESTAMP), // 23
			new Column("F_ENGINEMARK", Types.VARCHAR, 100), // 23
	};

	public void prepare(Map stormConf) {
		connection = DB2ExcutorQuenue.isExstConnection(connection);
	}

	public void execute(Tuple input) {
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		PreparedStatement ps = null;
		long start = 0;
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		try {
			for (int i = 0; i < dataList.size(); i++) {
				Gps gps = Gps.parseFrom(dataList.get(i));
				long vehicleId = gps.getVehicleID();
				CFBEngine cEngine = gps.getFBEngine();
				if (cEngine != null && !getCFBEngine(cEngine)) {
					List<Long> vehicleIdList = getVehicleId(vehicleId);
					if (vehicleIdList.size() == 0) {
						String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
						List<Map<Integer, Object>> parameterIndex2ValueMap = buildSqlObjectListFromGps(gps);
						preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, parameterIndex2ValueMap));
					} else {
						List<Column> updateColumns = new ArrayList<>();
						List<Object> objects = new ArrayList<>();
						Map<Object, Object[]> where2UpdateValueMap = new HashMap<Object, Object[]>();
						addColumn(new Column("F_KZZS", Types.DOUBLE), objects, updateColumns, cEngine.getKzZs(), "double");
						addColumn(new Column("F_KZNJ", Types.SMALLINT), objects, updateColumns, cEngine.getKzNj(), "int");
						addColumn(new Column("F_GZD2ZS", Types.DOUBLE), objects, updateColumns, cEngine.getGzd2Zs(), "double");
						addColumn(new Column("F_GZD2NJ", Types.SMALLINT), objects, updateColumns, cEngine.getGzd2Nj(), "int");
						addColumn(new Column("F_GZD3ZS", Types.DOUBLE), objects, updateColumns, cEngine.getGzd3Zs(), "double");
						addColumn(new Column("F_GZD3NJ", Types.SMALLINT), objects, updateColumns, cEngine.getGzd3Nj(), "int");
						addColumn(new Column("F_GZD4ZS", Types.DOUBLE), objects, updateColumns, cEngine.getGzd4Zs(), "double");
						addColumn(new Column("F_GZD4NJ", Types.SMALLINT), objects, updateColumns, cEngine.getGzd4Nj(), "int");
						addColumn(new Column("F_GZD5ZS", Types.DOUBLE), objects, updateColumns, cEngine.getGzd5Zs(), "double");
						addColumn(new Column("F_GZD5NJ", Types.SMALLINT), objects, updateColumns, cEngine.getGzd5Nj(), "int");
						addColumn(new Column("F_GZD6ZGZS", Types.DOUBLE), objects, updateColumns, cEngine.getGzd6Zgzs(), "double");
						addColumn(new Column("F_TSQKP", Types.DOUBLE), objects, updateColumns, cEngine.getTsqkp(), "double");
						addColumn(new Column("F_CKNJ", Types.SMALLINT), objects, updateColumns, cEngine.getCkNj(), "int");
						addColumn(new Column("F_GZD7CZZS", Types.DOUBLE), objects, updateColumns, cEngine.getGzd7Czzs(), "double");
						addColumn(new Column("F_ZDSDSEC", Types.DOUBLE), objects, updateColumns, cEngine.getZdSdsec(), "double");
						addColumn(new Column("F_ZDZS", Types.SMALLINT), objects, updateColumns, cEngine.getZdZs(), "int");
						addColumn(new Column("F_ZGZS", Types.SMALLINT), objects, updateColumns, cEngine.getZgZs(), "int");
						addColumn(new Column("F_ZDNJ", Types.SMALLINT), objects, updateColumns, cEngine.getZdNj(), "int");
						addColumn(new Column("F_ZGNJ", Types.SMALLINT), objects, updateColumns, cEngine.getZgNj(), "int");
						addColumn(new Column("F_ENGINEMARK", Types.VARCHAR, 100), objects, updateColumns, cEngine.getEngineMark(), "string");
						objects.add(new java.sql.Date(new Date().getTime()));
						updateColumns.add(new Column("F_RECORDTIME", Types.TIMESTAMP));
						objects.add(new java.sql.Date(gps.getGpsDate()));
						updateColumns.add(new Column("F_GPSTIME", Types.TIMESTAMP));

						int size = updateColumns.size();
						Column[] whereToColumns = new Column[size];
						for (int j = 0; j < updateColumns.size(); j++) {
							whereToColumns[j] = updateColumns.get(j);
						}
						where2UpdateValueMap.put(gps.getVehicleID(), objects.toArray());
						for (Object[] objectLength : where2UpdateValueMap.values()) {
							if (whereToColumns.length != objectLength.length) {
								throw new IllegalArgumentException("PreparedUpdateSqlAndValues's values length error, and updateColumns.length=" + whereToColumns.length + " objects.length=" + objectLength.length);
							}
						}
						preparedSqlAndValuesList.add(new Update(TABLE_NAME, WHERE_COLUMNS, whereToColumns, where2UpdateValueMap));
					}
				}
			}
		} catch (InvalidProtocolBufferException e) {
		} catch (SQLException e1) {

		}
		
//		try {
//			start = System.currentTimeMillis();
//			DB2ExcutorQuenue.excuteBatchs(preparedSqlAndValuesList, connection);
//			LOG.warn("execute sql for " + preparedSqlAndValuesList.size() + " records in " + ((System.currentTimeMillis() - start)) + "ms");
//		} catch (IllegalArgumentException e) {
//		} catch (SQLException e) {
			List<PreparedSqlAndValues> _Sqls = new ArrayList<PreparedSqlAndValues>();
			for (PreparedSqlAndValues tmp : preparedSqlAndValuesList) {
				_Sqls.add(tmp);
			}
			if (_Sqls != null && _Sqls.size() > 0) {
//				_collector.emit("405", new Values(_Sqls));
			}
			preparedSqlAndValuesList.clear();
//		}
	}

	private static void addColumn(Column column, List<Object> objects, List<Column> UPDATE_COLUMNS, String value, String flag) {
		if (!value.equals("")) {
			if (flag.equals("int")) {
				objects.add(Integer.parseInt(value));
				UPDATE_COLUMNS.add(column);
			}
			if (flag.equals("double")) {
				objects.add(Double.parseDouble(value));
				UPDATE_COLUMNS.add(column);
			}
			if (flag.equals("string")) {
				objects.add(value);
				UPDATE_COLUMNS.add(column);
			}
		}
	}

	public static List<Long> getVehicleId(long vehicleId) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		try {
			String sql = "SELECT F_VEHICLE_ID FROM T_FBDIAGNOSISENGINECUR WHERE F_VEHICLE_ID = " + vehicleId + "";
			List<Long> result = new ArrayList<Long>();
			// connection = DB2Excutor.getConnection();
			if (connection == null || connection.isClosed()) {
				connection = DB2ExcutorQuenue.getConnection();
			}
			pstmt = connection.prepareStatement(sql);
			resultSet = pstmt.executeQuery();
			while (resultSet.next()) {
				result.add(resultSet.getLong(1));
			}
			connection.commit();
			return result;
		} catch (SQLException e) {
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (pstmt != null)
				pstmt.close();
			if(connection!=null){
				connection.close();
			}
		}
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromGps(Gps gps) {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		CFBEngine cEngine = gps.getFBEngine();
		m.put(1, gps.getVehicleID());
		putValue(m, 2, cEngine.getKzZs(), "double");
		putValue(m, 3, cEngine.getKzNj(), "int");
		putValue(m, 4, cEngine.getGzd2Zs(), "double");
		putValue(m, 5, cEngine.getGzd2Nj(), "int");
		putValue(m, 6, cEngine.getGzd3Zs(), "double");
		putValue(m, 7, cEngine.getGzd3Nj(), "int");
		putValue(m, 8, cEngine.getGzd4Zs(), "double");
		putValue(m, 9, cEngine.getGzd4Nj(), "int");
		putValue(m, 10, cEngine.getGzd5Zs(), "double");
		putValue(m, 11, cEngine.getGzd5Nj(), "int");
		putValue(m, 12, cEngine.getGzd6Zgzs(), "double");
		putValue(m, 13, cEngine.getTsqkp(), "double");
		putValue(m, 14, cEngine.getCkNj(), "int");
		putValue(m, 15, cEngine.getGzd7Czzs(), "double");
		putValue(m, 16, cEngine.getZdSdsec(), "double");
		putValue(m, 17, cEngine.getZdZs(), "int");
		putValue(m, 18, cEngine.getZgZs(), "int");
		putValue(m, 19, cEngine.getZdNj(), "int");
		putValue(m, 20, cEngine.getZgNj(), "int");
		m.put(21, new java.sql.Date(new Date().getTime()));
		m.put(22, new java.sql.Date(gps.getGpsDate()));
		m.put(23, cEngine.getEngineMark());
		s.add(m);
		return s;
	}

	private static void putValue(Map<Integer, Object> m, int index, String value, String flag) {
		if (!value.equals("")) {
			if (flag.equals("int"))
				m.put(index, Integer.parseInt(value));
			if (flag.equals("double"))
				m.put(index, Double.parseDouble(value));
		} else
			m.put(index, 0);
	}

	private static boolean getCFBEngine(CFBEngine cEngine) {
		return (cEngine.getKzZs().equals("") && cEngine.getKzNj().equals("") && cEngine.getGzd2Zs().equals("") && cEngine.getGzd2Nj().equals("") && cEngine.getGzd3Zs().equals("") && cEngine.getGzd3Nj().equals("") && cEngine.getGzd4Zs().equals("") && cEngine.getGzd4Nj().equals("") && cEngine.getGzd5Zs().equals("") && cEngine.getGzd5Nj().equals("") && cEngine.getGzd6Zgzs().equals("") && cEngine.getTsqkp().equals("") && cEngine.getCkNj().equals("") && cEngine.getGzd7Czzs().equals("") && cEngine.getZdSdsec().equals("") && cEngine.getZdZs().equals("") && cEngine.getZgZs().equals("") && cEngine.getZdNj().equals("") && cEngine.getZgNj().equals("") && cEngine.getEngineMark().equals(""));
	}

}
