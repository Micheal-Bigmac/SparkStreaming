package com.sxp.hbase.bolt.mapper;

import com.sxp.hbase.common.ColumnList;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

/**
 * Maps a <code>backtype.task.tuple.Tuple</code> object to a row in an HBase table.
 */
public interface HBaseMapper extends Serializable {

	/**
	 * Given a tuple, return the HBase rowkey.
	 *
	 * @param tuple
	 * @return
	 */
	byte[] rowKey(Tuple tuple);

	/**
	 * Given a tuple, return a list of HBase columns to insert.
	 *
	 * @param tuple
	 * @return
	 */
	ColumnList columns(Tuple tuple);

}
