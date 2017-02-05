package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SQLUtils;
import com.hsae.rdbms.db2.Update;
import com.sxp.task.protobuf.generated.DeviceRegInfo.DeviceReg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeviceRegDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(DeviceRegDB2Bolt.class);
	public static final String TABLE_NAME_1 = "T_TERMINAL_REGISTER";
	public static final Column WHERE_COLUMNS_1 = new Column("F_VEHICLE_ID", Types.BIGINT);
	public static final Column[] UPDATE_COLUMNSS_1 = new Column[] { new Column("F_AUTH_CODE", Types.VARCHAR, 32),//
			new Column("F_REGISTER_DATE", Types.TIMESTAMP),//
			new Column("F_IS_LOGOUT", Types.SMALLINT),//
			new Column("F_IP_ADDRESS", Types.VARCHAR, 50),//
			new Column("F_PORT", Types.INTEGER),//
			new Column("F_UPDATE_DATE", Types.TIMESTAMP) //
	};

	public static final String TABLE_NAME_2 = "T_TERMINAL";
	public static final Column WHERE_COLUMNS_2 = new Column("F_ID", Types.BIGINT);
	public static final Column[] UPDATE_COLUMNSS_2 = new Column[] { new Column("F_PROVINCE", Types.VARCHAR, 32),//
			new Column("F_CITY_ID", Types.VARCHAR, 32),//
			new Column("F_EQUIPMENT_MODE", Types.VARCHAR, 64),//
			new Column("F_PLATE_COLOR", Types.VARCHAR, 20),//
			new Column("F_MOBILE_CODE", Types.VARCHAR, 32),//
			new Column("F_ENTERPRISE_CODE", Types.VARCHAR, 32) //
	};
	
	public static final String TABLE_NAME_3 = "T_VEHICLE";
	public static final Column WHERE_COLUMNS_3 = new Column("F_ID", Types.BIGINT);
	public static final Column[] UPDATE_COLUMNSS_3 = new Column[] { new Column("F_PLATE_COLOR", Types.VARCHAR, 20),//
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gpsList) throws InvalidProtocolBufferException {
		Map<Object, Object[]> where2UpdateValueMap1 = new HashMap<Object, Object[]>();
		Map<Object, Object[]> where2UpdateValueMap2 = new HashMap<Object, Object[]>();
		Map<Object, Object[]> where2UpdateValueMap3 = new HashMap<Object, Object[]>();
		List<byte[]> contentList = new ArrayList<byte[]>();
		for (int i = 0, size = gpsList.size(); i < size; i++) {
			try {
				DeviceReg d = DeviceReg.parseFrom(gpsList.get(i));
				where2UpdateValueMap1.put(d.getVehicleID(),
						new Object[] { d.getAuthCode(), new java.sql.Date(d.getRegisterDate()), 0, d.getIpAddress(),
								d.getPort(), new java.sql.Date(d.getUpdateTime()) });
				where2UpdateValueMap2.put(d.getDeviceID(), new Object[] { d.getProvinceID(), d.getCityID(),
						d.getDeviceType(), d.getPlateColor(), d.getSim(), d.getEnterpriseCode() });
				where2UpdateValueMap3.put(d.getVehicleID(), new Object[] { d.getPlateColor() });
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse DeviceReg error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		Update update1 = new Update(TABLE_NAME_1, WHERE_COLUMNS_1, UPDATE_COLUMNSS_1, where2UpdateValueMap1);
		preparedSqlAndValuesList.add(update1);
		Update update2 = new Update(TABLE_NAME_2, WHERE_COLUMNS_2, UPDATE_COLUMNSS_2, where2UpdateValueMap2);
		preparedSqlAndValuesList.add(update2);
		Update update3 = new Update(TABLE_NAME_3, WHERE_COLUMNS_3, UPDATE_COLUMNSS_3, where2UpdateValueMap3);
		preparedSqlAndValuesList.add(update3);
		String preparedUpdateSql1 = SQLUtils.buildPreparedUpdateSqlWithUpdateColumnValues(update1);
		LOG.error("deviceRegDb sql:" + preparedUpdateSql1);
		String preparedUpdateSql2 = SQLUtils.buildPreparedUpdateSqlWithUpdateColumnValues(update2);
		LOG.error("deviceRegDb sql:" + preparedUpdateSql2);
		String preparedUpdateSql3 = SQLUtils.buildPreparedUpdateSqlWithUpdateColumnValues(update3);
		LOG.error("deviceRegDb sql:" + preparedUpdateSql3);
		return preparedSqlAndValuesList;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}