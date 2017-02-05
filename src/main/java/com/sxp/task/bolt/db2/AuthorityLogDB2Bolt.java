package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.Update;
import com.sxp.task.protobuf.generated.AuthorityLogInfo.AuthorityLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AuthorityLogDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(AuthorityLogDB2Bolt.class);
	public static final String TABLE_NAME_1 = "T_TERMINAL_REGISTER";
	public static final Column WHERE_COLUMNS_1 = new Column("F_VEHICLE_ID", Types.BIGINT);
	public static final Column[] UPDATE_COLUMNSS_1 = new Column[] { new Column("F_AUTH_CODE", Types.VARCHAR, 32),//
			new Column("F_AUTH_DATE", Types.TIMESTAMP),//
			new Column("F_UPDATE_DATE", Types.TIMESTAMP),//
			new Column("F_ONLINE", Types.SMALLINT) //
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> list) throws InvalidProtocolBufferException {
		Map<Object, Object[]> where2UpdateValueMap1 = new HashMap<Object, Object[]>();
		for (int i = 0, size = list.size(); i < size; i++) {
			try {
				AuthorityLog d = AuthorityLog.parseFrom(list.get(i));
				where2UpdateValueMap1.put(d.getVehicleID(),
						new Object[] { d.getAuthorityCode(), new java.sql.Date(d.getAuthDate()), new java.sql.Date(d.getUpdateTime()), d.getOnLine() ? 1 : 0 });
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse AuthorityLog error", e);
			}
		}
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		preparedSqlAndValuesList.add(new Update(TABLE_NAME_1, WHERE_COLUMNS_1, UPDATE_COLUMNSS_1, where2UpdateValueMap1));
		return preparedSqlAndValuesList;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}