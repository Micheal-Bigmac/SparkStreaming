package com.sxp.hbase.common;

import com.google.common.collect.Lists;
import com.sxp.hbase.bolt.mapper.HBaseProjectionCriteria;
import com.sxp.hbase.security.HBaseSecurityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.UserProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

public class HBaseClient {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

	public static final String CONFIG_KEY = "hbase.conf";

	private HTable table;

	public HBaseClient(Map<String, Object> map, final Configuration configuration, final String tableName) {
		try {
			UserProvider provider = HBaseSecurityUtil.login(map, configuration);
			this.table = provider.getCurrent().getUGI().doAs(new PrivilegedExceptionAction<HTable>() {
				@Override
				public HTable run() throws IOException {
					return new HTable(configuration, tableName);
				}
			});
		} catch (Exception e) {
			throw new RuntimeException("HBase bolt preparation failed: " + e.getMessage(), e);
		}
	}

	public List<Mutation> constructMutationReq(byte[] rowKey, ColumnList cols, Durability durability) {
		List<Mutation> mutations = Lists.newArrayList();

		if (cols.hasColumns()) {
			Put put = new Put(rowKey);
			put.setDurability(durability);
			for (ColumnList.Column col : cols.getColumns()) {
				if (col.getTs() > 0) {
					put.add(col.getFamily(), col.getQualifier(), col.getTs(), col.getValue());
				} else {
					put.add(col.getFamily(), col.getQualifier(), col.getValue());
				}
			}
			mutations.add(put);
		}

		if (cols.hasCounters()) {
			Increment inc = new Increment(rowKey);
			inc.setDurability(durability);
			for (ColumnList.Counter cnt : cols.getCounters()) {
				inc.addColumn(cnt.getFamily(), cnt.getQualifier(), cnt.getIncrement());
			}
			mutations.add(inc);
		}

		if (mutations.isEmpty()) {
			mutations.add(new Put(rowKey));
		}
		return mutations;
	}

	public void batchMutate(List<Mutation> mutations) throws Exception {
		Object[] result = new Object[mutations.size()];
		try {
			table.batch(mutations, result);
		} catch (InterruptedException e) {
			LOG.warn("Error performing a mutation to HBase.", e);
			throw e;
		} catch (IOException e) {
			LOG.warn("Error performing a mutation to HBase.", e);
			throw e;
		}
	}

	public Get constructGetRequests(byte[] rowKey, HBaseProjectionCriteria projectionCriteria) {
		Get get = new Get(rowKey);

		if (projectionCriteria != null) {
			for (byte[] columnFamily : projectionCriteria.getColumnFamilies()) {
				get.addFamily(columnFamily);
			}

			for (HBaseProjectionCriteria.ColumnMetaData columnMetaData : projectionCriteria.getColumns()) {
				get.addColumn(columnMetaData.getColumnFamily(), columnMetaData.getQualifier());
			}
		}

		return get;
	}

	public Result[] batchGet(List<Get> gets) throws Exception {
		try {
			return table.get(gets);
		} catch (Exception e) {
			LOG.warn("Could not perform HBASE lookup.", e);
			throw e;
		}
	}

	/**
	 * @Title: scan
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param scan
	 * @return
	 * @throws IOException
	 *             ResultScanner
	 * @author: 韩欣宇
	 * @date 2015年6月1日 下午5:40:25
	 * @throws
	 */
	public ResultScanner scan(Scan scan) throws IOException {
		try {
			return table.getScanner(scan);
		} catch (IOException e) {
			LOG.warn("Could not perform HBASE scan.", e);
			throw e;
		}
	}
}
