package com.storm.trident.example;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class MyTestFunction extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		int a = tuple.getInteger(0);
		int b = tuple.getInteger(1);
		collector.emit(new Values(a + b));

	}

}