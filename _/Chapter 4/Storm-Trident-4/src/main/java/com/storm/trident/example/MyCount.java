package com.storm.trident.example;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class MyCount implements CombinerAggregator<Long> {

	public Long init(TridentTuple tuple) {

		Integer integer = tuple.getInteger(0);
		return new Long(integer);
	}

	public Long combine(Long val1, Long val2) {
		return val1 + val2;
	}

	public Long zero() {
		return 0L;
	}

}
