package com.sxp.task.bolt.hbase.mapper;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Maps a <code>backtype.task.tuple.Tuple</code> object to a row in an HBase table.
 */
public class HbaseMapper extends AbstractMapper {

//	abstract byte[] rowKey(Tuple tuple);
//
//	abstract ColumnList columns(Tuple tuple);

	@Override
	public List<Mutation> mutations(Tuple tuple) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Values> toValues(ITuple input, Result result) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}


}
