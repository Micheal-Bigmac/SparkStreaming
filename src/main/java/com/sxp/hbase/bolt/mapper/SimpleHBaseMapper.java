package com.sxp.hbase.bolt.mapper;
import com.sxp.hbase.common.ColumnList;
import com.sxp.hbase.common.Utils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SimpleHBaseMapper implements HBaseMapper {
	private static final Logger LOG = LoggerFactory.getLogger(SimpleHBaseMapper.class);

	private String rowKeyField;
	// private String timestampField;
	private byte[] columnFamily;
	private Fields columnFields;
	private Fields counterFields;

	public SimpleHBaseMapper() {
	}

	public SimpleHBaseMapper withRowKeyField(String rowKeyField) {
		this.rowKeyField = rowKeyField;
		return this;
	}

	public SimpleHBaseMapper withColumnFields(Fields columnFields) {
		this.columnFields = columnFields;
		return this;
	}

	public SimpleHBaseMapper withCounterFields(Fields counterFields) {
		this.counterFields = counterFields;
		return this;
	}

	public SimpleHBaseMapper withColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily.getBytes();
		return this;
	}

	// public SimpleTridentHBaseMapper withTimestampField(String timestampField){
	// this.timestampField = timestampField;
	// return this;
	// }

	@Override
	public byte[] rowKey(Tuple tuple) {
		Object objVal = tuple.getValueByField(this.rowKeyField);
		return Utils.toBytes(objVal);
	}

	@Override
	public ColumnList columns(Tuple tuple) {
		ColumnList cols = new ColumnList();
		if (this.columnFields != null) {
			// TODO timestamps
			for (String field : this.columnFields) {
				cols.addColumn(this.columnFamily, field.getBytes(), Utils.toBytes(tuple.getValueByField(field)));
			}
		}
		if (this.counterFields != null) {
			for (String field : this.counterFields) {
				cols.addCounter(this.columnFamily, field.getBytes(), Utils.toLong(tuple.getValueByField(field)));
			}
		}
		return cols;
	}
}
