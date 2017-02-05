package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.ExceptionAlarmInfo.ExceptionAlarm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExceptionAlarmDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(ExceptionAlarmDB2Bolt.class);
	static String TABLE_NAME = "T_EXCEPTION_ALARM";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("T_ALARM_ID", Types.BIGINT),// 2
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 3
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> exceptionalarmList) throws InvalidProtocolBufferException {
		if(exceptionalarmList.size() == 0)
			return null;
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = exceptionalarmList.size(); i < size; i++) {
			try {
				ExceptionAlarm exceptionalarm = ExceptionAlarm.parseFrom(exceptionalarmList.get(i));
				s.add(buildSqlObjectListFromExceptionAlarm(exceptionalarm));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse ExceptionAlarm error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromExceptionAlarm(ExceptionAlarm exceptionalarm) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, exceptionalarm.getID());
		m.put(2, exceptionalarm.getAlarmID());
		m.put(3, exceptionalarm.getEnterpriseCode());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}