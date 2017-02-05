package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.common.CommonTools;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.ProgramVersionInfo.ProgramVersion;
import com.sxp.task.protobuf.generated.ProgramVersionInfo.ProgramVersionItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProgramVersionDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(ProgramVersionDB2Bolt.class);
	static String TABLE_NAME = "T_PROGRAMVERSIONREPORT";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLE_ID", Types.BIGINT),// 2
			new Column("F_TERMINAL_ID", Types.BIGINT),// 3
			new Column("F_INFOID", Types.SMALLINT),// 4
			new Column("F_CONTENT", Types.VARCHAR, 100),// 5
			new Column("F_RECORDTIME", Types.TIMESTAMP),// 6
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 7
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> programversionList) throws InvalidProtocolBufferException {
		if(programversionList.size() == 0)
			return null;
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = programversionList.size(); i < size; i++) {
			try {
				ProgramVersion programversion = ProgramVersion.parseFrom(programversionList.get(i));
				s.addAll(buildSqlObjectMapFromProgramVersion(programversion));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse ProgramVersion error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		return preparedSqlAndValuesList;
	}

	private static List<Map<Integer, Object>> buildSqlObjectMapFromProgramVersion(ProgramVersion programversion) {
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<ProgramVersionItem> items = programversion.getItemsList();
		for (ProgramVersionItem programVersionItem : items) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, CommonTools.getPK());
			m.put(2, programversion.getVehicleID());
			m.put(3, programversion.getTearminalID());
			m.put(4, programVersionItem.getType());
			m.put(5, programVersionItem.getContent());
			m.put(6, new java.sql.Date(programversion.getRecordTime()));
			m.put(7, programversion.getEnterpriseCode());
			s.add(m);
		}
		return s;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}