/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * 在Hive中实现全局唯一ID， 此处直接继承Generic UDF来实现
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
@Description(name="rowid", value="_FUNC_() - Returns a generated row id of a form {TASK_ID}-{SEQUENCE_NUMBER}")
@UDFType(deterministic = false, stateful = true)
public class RowIdUDF extends GenericUDF {
	
	private static final Logger log = Logger.getLogger(RowIdUDF.class);
	
	private long sequence;
	private int taskId;
	
	public RowIdUDF() {
		sequence = 0L;
		taskId = -1;
	}
	/**
	 * 可选，该方法中可以通过context.getJobConf()获取job执行时候的Configuration；
	 * 可以通过Configuration传递参数值
	 * This is only called in runtime of MapRedTask. 也就是说如果作业不需要MapReduce来执行，则不会调用该方法。
	 */
	@Override
	public void configure(MapredContext context){
		log.info("Configure RowIdUDF.");
		if (context != null) {
			JobConf conf = context.getJobConf();
			if (conf == null) {
				throw new IllegalStateException("JobConf is not set.");
			}
			taskId = conf.getInt("mapred.task.partition", -1);
			if (taskId == -1) {
				taskId = conf.getInt("mapreduce.task.partition", -1);
				if (taskId == -1) {
					throw new IllegalStateException(
		                    "Both mapred.task.partition and mapreduce.task.partition are not set: "
		                            + conf);
				}
			}
		}
	}
	/**
	 * 必选，该方法用于函数初始化操作，并定义函数的返回值类型；
	 * 比如，在该方法中可以初始化对象实例，初始化数据库链接，初始化读取文件等；
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		log.info("Initialize RowIdUDF.");
		return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
	}
	/**
	 * 必选，函数处理的核心方法，用途和UDF中的evaluate一样；
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		sequence++;
		return taskId + "-" + sequence;
	}
	/**
	 * 必选，显示函数的帮助信息
	 */
	@Override
	public String getDisplayString(String[] children) {
		return "Usage: rowid()";
	}
	
}
