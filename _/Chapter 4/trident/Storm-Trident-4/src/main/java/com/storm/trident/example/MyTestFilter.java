package com.storm.trident.example;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class MyTestFilter extends BaseFilter {

	public boolean isKeep(TridentTuple tuple) {
		return (tuple.getInteger(0) % 2 == 0);

	}
}