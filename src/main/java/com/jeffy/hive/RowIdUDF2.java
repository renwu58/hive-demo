/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * 在Hive中实现全局唯一ID， 此处直接继承 UDF来实现
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
@Description(name="rowid", value="_FUNC_() - Returns a generated row id of a form {TASK_ID}-{SEQUENCE_NUMBER}")
@UDFType(deterministic = false, stateful = true)
public class RowIdUDF2 extends UDF {
	
	private long sequence;
	private int taskId;
	
	public RowIdUDF2() {
		sequence = 0L;
		taskId = -1;
	}
	public Text evaluate(){
		if (taskId == -1) {
			taskId = getTaskId() +1;
		}
		sequence++;
		String rowid = taskId + "-" + sequence;
		return new Text(rowid);
	}
	/**
	 * 此方法中MapredContext的值只有当启动了MapReduce的时候才会有值，因此在执行简单的查询不走MapReduce的情况下就会报错。
	 * 
	 * @return Mapred的任务ID
	 */
	public static int getTaskId(){
		MapredContext ctx = MapredContext.get();
		if (ctx == null) {
			throw new IllegalStateException("MapredContext is not set.");
		}
		JobConf conf = ctx.getJobConf();
		if (conf == null) {
			throw new IllegalStateException("JobConf is not set.");
		}
		int taskid = conf.getInt("mapred.task.partition", -1);
		if (taskid == -1) {
			taskid = conf.getInt("mapreduce.task.partition", -1);
			if (taskid == -1) {
				throw new IllegalStateException(
	                    "Both mapred.task.partition and mapreduce.task.partition are not set: "
	                            + conf);
			}
		}
		return taskid;
	}
}
