package com.sxp.task.bolt.db2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hsae.rdbms.db2.DB2Excutor;
import com.hsae.rdbms.db2.PreparedSqlAndValues;
import org.apache.storm.tuple.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractDB2Bolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractDB2Bolt.class);
	protected static final String NULL = "NULL";
	protected static final char SQL_DELIMITER = ',';
	public List<PreparedSqlAndValues> preparedSqls = null;

	/**
	 * 根据数据包生成SQL
	 * 
	 * @param dataList
	 *            数据包列表
	 * @return Sql脚本列表
	 * @throws InvalidProtocolBufferException
	 */
	protected abstract List<PreparedSqlAndValues> buildPreparedSqlAndValuesList(List<byte[]> gpsList) throws InvalidProtocolBufferException;

	/**
	 * 数据库执行异常时插入对应的ERROR表
	 * 
	 * @param gpsList
	 *            数据包列表
	 * @return Sql脚本列表
	 * @throws InvalidProtocolBufferException
	 */
	protected abstract List<PreparedSqlAndValues> bulidErrorPreparedSqlAndValuesList(List<byte[]> gpsList) throws InvalidProtocolBufferException;

	/*
	 * <p>Title: execute</p> <p>Description: </p>
	 * 
	 * @param input
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		List<byte[]> dataList = (List<byte[]>) input.getValue(0);
		List<PreparedSqlAndValues> preparedSqlAndValuesList = null;
		long start = 0;
		if (dataList.size() == 0)
			return;
		try {
			preparedSqlAndValuesList = buildPreparedSqlAndValuesList(dataList);
			if (preparedSqlAndValuesList == null || preparedSqlAndValuesList.size() == 0) {
				LOG.warn("no preparedSqlAndValuesList can be built from " + dataList.size() + " dataList.");
			} else {
				preparedSqls=preparedSqlAndValuesList;
//				preparedSqls.addAll(preparedSqlAndValuesList);
//				if (preparedSqls.size() > 200) {
//					start = System.currentTimeMillis();
//					DB2Excutor.excuteBatchs(preparedSqlAndValuesList);
//					LOG.warn("execute sql for " + preparedSqls.size() + " records in " + ((System.currentTimeMillis() - start)) + "ms");
//					preparedSqls.clear();
//				}
			}
		} catch (InvalidProtocolBufferException e) {
			LOG.error(this.getClass().getName() + ".execute error!");
		} catch (IllegalArgumentException e) {
			LOG.error(this.getClass().getName() + ".execute error!");
		}
//		catch (SQLException e) {
//			List<PreparedSqlAndValues> _Sqls = new ArrayList<PreparedSqlAndValues>();
//			for (PreparedSqlAndValues tmp : preparedSqls) {
//				_Sqls.add(tmp);
//			}
//			if (_Sqls != null && _Sqls.size() > 0) {
////				_collector.emit("405", new Values(_Sqls));
//			}
//			preparedSqls.clear();
//		}
	}
	

	public void prepare(Map stormConf) {
		preparedSqls = new ArrayList<PreparedSqlAndValues>();
	}

}
