package com.sxp.hbase.common;
public interface ICounter {
	byte[] family();

	byte[] qualifier();

	long increment();
}
