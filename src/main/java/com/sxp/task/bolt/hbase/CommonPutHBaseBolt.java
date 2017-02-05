package com.sxp.task.bolt.hbase;

import com.sxp.task.bolt.hbase.mapper.HbaseMapper;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author James
 *
 */
public class CommonPutHBaseBolt extends AbstractHBaseBolt {
	private static final Logger LOG = LoggerFactory.getLogger(CommonPutHBaseBolt.class);

	public CommonPutHBaseBolt(String tableName, HbaseMapper mapper) {
		super(tableName, mapper);
	}

	public void execute(Tuple input) {
		List<Mutation> mutations = mapper.mutations(input);
		try {
			long startTime = System.currentTimeMillis();
			this.hBaseClient.batchMutate(mutations);
			LOG.debug("hBaseClient.batchMutate " + mutations.size() + " mutations in " + ((System.currentTimeMillis() - startTime)) + "ms");
		} catch (Exception e) {
//			LOG.warn("Failing tuple. SourceComponent: " + input.getSourceComponent() + "SourceStreamId: " + input.getSourceStreamId() + "MessageId: " + input.getMessageId(), e);
//			this.collector.fail(input);
			return;
		}
		mutations = null;
//		this.collector.ack(input);
	}

}
