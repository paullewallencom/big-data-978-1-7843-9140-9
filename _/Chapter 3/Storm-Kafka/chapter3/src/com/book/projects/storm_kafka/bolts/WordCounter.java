package com.book.projects.storm_kafka.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class WordCounter extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String, Long> counters;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counters = new HashMap<String, Long>();
	}

	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		System.out.println("Received Word : " + word);

		Long count = this.counters.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.counters.put(word, count);
		collector.emit(new Values(word, String.valueOf(count)));
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
	
}
