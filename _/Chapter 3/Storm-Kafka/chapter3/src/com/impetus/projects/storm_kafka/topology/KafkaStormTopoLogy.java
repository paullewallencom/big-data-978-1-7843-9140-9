package com.impetus.projects.storm_kafka.topology;

import java.util.ArrayList;
import java.util.List;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.book.projects.storm_kafka.bolts.PrinterBolt;
import com.book.projects.storm_kafka.bolts.WordCounter;
import com.book.projects.storm_kafka.bolts.WordNormalizer;

public class KafkaStormTopoLogy {
	private static final String WORD_COUNTER_SPOUT_ID = "words-spout";

	private static final String WORD_NORMALIZER_BOLT_ID = "split-bolt";
	private static final String WORD_COUNTER_BOLT_ID = "count-bolt";
	private static final String WORD_PRINTER_BOLT_ID = "report-bolt";

	private static final String TOPOLOGY_NAME = "Storm-Kafka-WC-Topology";
	private static final String TOPIC_NAME = "stormTopic";
	
	private static final String ZKNODE1 = "172.27.34.61";

	// private final BrokerHosts brokerHosts;
	private final ZkHosts abc;

	public KafkaStormTopoLogy(String kafkaZookeeper) {
		abc = new ZkHosts(kafkaZookeeper);
	}

	public StormTopology buildTopology(WordNormalizer wn, WordCounter wc,
			PrinterBolt pb) {
		SpoutConfig kafkaConfig = new SpoutConfig(abc,
				TOPIC_NAME, "172.27.34.61", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(WORD_COUNTER_SPOUT_ID, new KafkaSpout(kafkaConfig), 1);

		builder.setBolt(WORD_NORMALIZER_BOLT_ID, wn, 1)
				.shuffleGrouping(WORD_COUNTER_SPOUT_ID).setNumTasks(1);

		builder.setBolt(WORD_COUNTER_BOLT_ID, wc, 1)
				.fieldsGrouping(WORD_NORMALIZER_BOLT_ID, new Fields("word"))
				.setNumTasks(1);

		//builder.setBolt(WORD_PRINTER_BOLT_ID, pb).globalGrouping(WORD_COUNTER_BOLT_ID);
		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {

		WordNormalizer wn = new WordNormalizer();
		WordCounter wc = new WordCounter();
		PrinterBolt pb = new PrinterBolt();

		String kafkaZk = args[0];
		KafkaStormTopoLogy kafkaStormTopoLogy = new KafkaStormTopoLogy(kafkaZk);
		Config config = new Config();
		// config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		System.out.println("Building Topology... ");
		StormTopology stormTopology = kafkaStormTopoLogy.buildTopology(wn, wc,
				pb);
		System.out.println("Topology sucessfully built !! ");
		
		String dockerIp = args[1];

		List<String> zList = new ArrayList<String>();
		zList.add(ZKNODE1);
		
		// configure how often a tick tuple will be sent to our bolt
		//config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
		
		config.put(Config.NIMBUS_HOST, dockerIp);
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, zList);
		config.setNumWorkers(1);

		try {
			System.out.println("Submitting Topology... ");
			StormSubmitter.submitTopology(TOPOLOGY_NAME, config,
					stormTopology);
			System.out.println("Topology submitted successfully  !! ");
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	}
}
