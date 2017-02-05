package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.common.CommonTools;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.Insert;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.sxp.task.protobuf.generated.DeviceParmsInfo.DeviceParm;
import com.sxp.task.protobuf.generated.DeviceParmsInfo.DeviceParms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeviceParmsDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(DeviceParmsDB2Bolt.class);
	static String TABLE_NAME = "T_VEHICLECONFIGSELECTLOG";
	public static Column[] COLUMNS = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_ORDER_ID", Types.BIGINT),// 2
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 3
	};
	public static final String INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(TABLE_NAME, COLUMNS);
	static String SUB_TABLE_NAME = "T_VEHICLECONFIGSELECTLOGDETAILS";
	public static Column[] COLUMNS_SUB = new Column[] { new Column("F_ID", Types.BIGINT),// 1
			new Column("F_VEHICLECONFIG_ID", Types.BIGINT),// 2
			new Column("F_PARAMID", Types.BIGINT),// 3
			new Column("F_PARAMVALUE", Types.VARCHAR, 500),// 4
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) // 5
	};
	public static final String SUB_INSERT_PREPARED_SQL = SQLUtils.buildPreparedInsertSql(SUB_TABLE_NAME, COLUMNS_SUB);

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> deviceparmsList) throws InvalidProtocolBufferException {
		if(deviceparmsList.size() == 0)
			return null;
		List<Map<Integer, Object>> s = new ArrayList<Map<Integer, Object>>();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (int i = 0, size = deviceparmsList.size(); i < size; i++) {
			try {
				DeviceParms deviceparms = DeviceParms.parseFrom(deviceparmsList.get(i));
				s.add(buildSqlObjectListFromDeviceParms(deviceparms));
				sD.addAll(buildSqlObjectListFromDeviceParm(deviceparms));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse DeviceParms error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Insert(INSERT_PREPARED_SQL, COLUMNS, s));
		preparedSqlAndValuesList.add(new Insert(SUB_INSERT_PREPARED_SQL, COLUMNS_SUB, sD));
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromDeviceParms(DeviceParms deviceparms) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		long pk = CommonTools.getPK();
		m.put(3, deviceparms.getQueryID());
		m.put(4, deviceparms.getEnterpiseCode());
		return m;
	}

	private static List<Map<Integer, Object>> buildSqlObjectListFromDeviceParm(DeviceParms deviceparms) {
		List<DeviceParm> itemsList = deviceparms.getItemsList();
		List<Map<Integer, Object>> sD = new ArrayList<Map<Integer, Object>>();
		for (DeviceParm deviceparm : itemsList) {
			Map<Integer, Object> m = new HashMap<Integer, Object>();
			m.put(1, deviceparm.getID());
			m.put(2, deviceparm.getParmID());
			m.put(3, deviceparm.getParmValue());
			sD.add(m);
		}
		return sD;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}