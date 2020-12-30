package com.book.projects.storm_kafka.bolts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class PrinterBolt extends BaseRichBolt {
	private Map<String, Long> counters;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Long>();
	}

	protected static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID);
	}

	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		String oldcount = input.getStringByField("count");
		Long count = Long.parseLong(oldcount);
		this.counters.put(word, count);

		if (isTickTuple(input)) {
			// Print the count of words after every 30 secs
			System.out.println("-- Received Tick Tuple event..");
			System.out.println("-- Word Count --");
			List<String> keys = new ArrayList<String>();
			keys.addAll(this.counters.keySet());

			Collections.sort(keys);

			for (String key : keys) {
				System.out.println(key + " : " + this.counters.get(key));
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit anything
	}

	@Override
	public void cleanup() {
	}

}
