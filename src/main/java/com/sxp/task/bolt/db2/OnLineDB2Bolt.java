package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.Column;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import com.hsae.rdbms.db2.SmipleProcedure;
import com.sxp.task.protobuf.generated.OnLineInfo.OnLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OnLineDB2Bolt extends AbstractDB2Bolt {

	private static final Logger LOG = LoggerFactory.getLogger(OnLineDB2Bolt.class);
	static String NAME = "P_VEHICLE_CHANGEONLINE";
	public static Column[] INS = new Column[] { new Column("REGID", Types.BIGINT),// 1
			new Column("ONLINE", Types.SMALLINT),// 2
			new Column("DT", Types.TIMESTAMP),// 3
			new Column("REASON", Types.SMALLINT) // 4
	};

	@Override
	protected List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> onlineList) throws InvalidProtocolBufferException {
		if(onlineList.size() == 0)
			return null;
		List<PreparedSqlAndValues> preparedSqlAndValuesList = new ArrayList<PreparedSqlAndValues>();
		for (int i = 0, size = onlineList.size(); i < size; i++) {
			try {
				Map<Integer, Object> s = new HashMap<Integer, Object>();
				OnLine online = OnLine.parseFrom(onlineList.get(i));
				s = buildSqlObjectListFromOnLine(online);
				preparedSqlAndValuesList.add(new SmipleProcedure(NAME, INS, s));
			} catch (InvalidProtocolBufferException e) {
				LOG.error("Protobuf parse OnLine error", e);
			}
		}
		return preparedSqlAndValuesList;
	}

	private static Map<Integer, Object> buildSqlObjectListFromOnLine(OnLine online) {
		Map<Integer, Object> m = new HashMap<Integer, Object>();
		m.put(1, online.getDeviceRegID());
		m.put(2, online.getStatus() ? 1 : 0);
		m.put(3, new java.sql.Date(online.getChangeTime()));
		m.put(4, online.getReason());
		return m;
	}

	@Override
	protected List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList)
			throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		return null;
	}
}