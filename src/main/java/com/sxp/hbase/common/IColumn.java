package com.sxp.hbase.common;
public interface IColumn {
	byte[] family();

	byte[] qualifier();

	byte[] value();

	long timestamp();
}
