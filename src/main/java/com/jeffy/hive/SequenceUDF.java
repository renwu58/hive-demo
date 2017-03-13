/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hive;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Jeffy<renwu58@gmail.com>
 *
 */
@UDFType(deterministic=false, stateful=true)
public class SequenceUDF extends UDF {
	private AtomicLong seq = new AtomicLong(1);
	public LongWritable evaluate(int prefix){
		return new LongWritable(Long.parseLong(prefix +"" + seq.getAndIncrement()));
	}
}
