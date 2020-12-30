package com.storm.trident.example;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.ImmutableList;

public class MergeTridentOperations {

	public static class Split extends BaseFunction {


		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sentence = tuple.getString(0);
			for (String word : sentence.split(" ")) {
				collector.emit(new Values(word));
			}

		}

		public static StormTopology buildTopology(LocalDRPC drpc) {
			FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("sentence"), 3, new Values("My name is Trident1"));
			FixedBatchSpout spout2 = new FixedBatchSpout(new Fields("sentence"), 3, new Values("My name is Trident2"));
			spout1.setCycle(true);
			spout2.setCycle(true);
			TridentTopology topology = new TridentTopology();
			Stream stream1 = topology.newStream("spout1", spout1);
			Stream stream2 = topology.newStream("spout2", spout2);

			// stream merge example
			Stream merge = topology.merge(stream1, stream2);
			TridentState wordCounts = merge.parallelismHint(1).each(new Fields("sentence"), new Split(), new Fields("word"))
					.groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
					.parallelismHint(16);

			topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word")).groupBy(new Fields("word"))
					.stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
					.each(new Fields("count"), new FilterNull()).aggregate(new Fields("count"), new Sum(), new Fields("sum"));
			return topology.build();
		}


		public static void main(String[] args) throws Exception {
			Config conf = new Config();
			conf.setMaxSpoutPending(20);
			if (args.length == 0) {
				LocalDRPC drpc = new LocalDRPC();
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("wordCounter", conf, aggregate());
				for (int i = 0; i < 100; i++) {
					 System.out.println("DRPC RESULT1: " +
					 drpc.execute("words", "Trident1"));
					 System.out.println("DRPC RESULT2: " +
					 drpc.execute("words", "Trident2"));
					 System.out.println("DRPC RESULT2: " +
					 drpc.execute("words", "name"));
					Thread.sleep(1000);
				}
			} else {
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
			}
		}

	}

}
