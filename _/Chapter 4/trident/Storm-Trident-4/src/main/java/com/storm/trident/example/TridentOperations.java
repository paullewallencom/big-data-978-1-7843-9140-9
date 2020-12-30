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

public class TridentOperations {

	public static class Split extends BaseFunction {

		private static FeederBatchSpout feederBatchSpout;
		/**
		 * 
		 */
		private static final long serialVersionUID = 1643068081148146793L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sentence = tuple.getString(0);
			for (String word : sentence.split(" ")) {
				collector.emit(new Values(word));
			}

		}

		public static StormTopology merge(LocalDRPC drpc) {
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

		public static StormTopology filter() {
			TridentTopology topology = new TridentTopology();

			FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values(1), new Values(2));
			spout.setCycle(true);
			topology.newStream("spout", spout).each(new Fields("sentence"), new MyTestFilter())
					.each(new Fields("sentence"), new PrintFilter());
			return topology.build();
		}

		public static StormTopology function() {
			TridentTopology topology = new TridentTopology();

			feederBatchSpout = new FeederBatchSpout(ImmutableList.of("a", "b"));
			topology.newStream("feederBatchSpout", feederBatchSpout).each(new Fields("a", "b"), new MyTestFunction(), new Fields("d"))
					.each(new Fields("d"), new PrintFilter());

			return topology.build();
		}

		public static StormTopology aggregate() {
			TridentTopology topology = new TridentTopology();

			

			topology.newStream("feederBatchSpout", feederBatchSpout).each(new Fields("a", "b"), new MyTestFunction(), new Fields("d"))
					.aggregate(new Fields("d"), new MyCount(), new Fields("count")).each(new Fields("count"), new PrintFilter());

			return topology.build();
		}

		public static StormTopology grouping() {
			TridentTopology topology = new TridentTopology();

			feederBatchSpout = new FeederBatchSpout(ImmutableList.of("a", "b"));

			TridentState tridentState = topology.newStream("feederBatchSpout", feederBatchSpout)
					.each(new Fields("a", "b"), new MyTestFunction(), new Fields("d")).groupBy(new Fields("d"))
					.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(1);
			return topology.build();
		}

		public static void main(String[] args) throws Exception {
			Config conf = new Config();
			conf.setMaxSpoutPending(20);
			if (args.length == 0) {
				LocalDRPC drpc = new LocalDRPC();
				LocalCluster cluster = new LocalCluster();
				feederBatchSpout = new FeederBatchSpout(ImmutableList.of("a", "b"));
				// uncomment only if you want to execute merge sample
				//cluster.submitTopology("wordCounter", conf, filter());
				// uncomment only if you want to execute aggregate sample
				//cluster.submitTopology("wordCounter", conf, aggregate());
				// uncomment only if you want to execute function sample
				//cluster.submitTopology("wordCounter", conf, function());
				// uncomment only if you want to execute merge sample
				//cluster.submitTopology("wordCounter", conf, merge());
							
				
				feederBatchSpout.feed(ImmutableList.of(new Values(1, 2)));
				feederBatchSpout.feed(ImmutableList.of(new Values(1, 2)));
				for (int i = 0; i < 100; i++) {
					// uncomment only if you want to execute merge sample
					/* System.out.println("DRPC RESULT1: " +
					 drpc.execute("words", "Trident1"));
					 System.out.println("DRPC RESULT2: " +
					 drpc.execute("words", "Trident2"));
					 System.out.println("DRPC RESULT2: " +
					 drpc.execute("words", "name"));*/
					Thread.sleep(1000);
				}
			} else {
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], conf, merge(null));
			}
		}

	}

}
