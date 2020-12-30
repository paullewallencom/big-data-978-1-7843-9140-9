package com.storm.trident.example;

import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class PrintFilter implements Filter {

	public void prepare(Map conf, TridentOperationContext context) {
	}

	public void cleanup() {
	}

	public boolean isKeep(TridentTuple tuple) {
		System.out.println(tuple);
		return true;
	}
}