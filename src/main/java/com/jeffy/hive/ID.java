/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

/**
 * 生成全局唯一ID
 * 
 * 目前仅为演示使用，采用JVM的高精度时间来生成
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
@Description(name="id", value="_FUNC_() - Returns a generated row id of a form {RANDOM_NUMBER}")
@UDFType(deterministic = true, stateful = false)
public class ID extends UDF{
	public ID(){
	}
	public LongWritable evaluate(){
		return new LongWritable(System.nanoTime());
	}
}
