/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Hive JDBC 连接操作数据库
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class HiveJdbcDemo {

	private final static String JDBC_URL = "jdbc:hive2://node1:10000/awm";
	//private final static String JDBC_URL = "jdbc:hive2://node1:2181,node2:2181,master:2181/awm;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
	private Connection  conn = null;
	
	public static void main(String[] args) throws Exception {
		HiveJdbcDemo hiveTestCase = new HiveJdbcDemo();
		hiveTestCase.connect();
		String queryMaxSQL = "select max(cv_id) from a_cv where cl_scm_id =117";
		hiveTestCase.executeStatment(queryMaxSQL, (rs)->{
			try{
				long cvid = 0L;
				if(rs.next()){
					cvid = rs.getLong(1);
				}
				System.out.println("a_cv max id for 117 is: " + cvid);
				rs.close();
			}catch (SQLException e) {
				e.printStackTrace();
			}
		});
		hiveTestCase.release();
	}

	public void connect() {
		try {
			if (conn == null) {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				conn = DriverManager.getConnection(JDBC_URL, "hive","hive");
			}else if (conn.isClosed()) {
				conn = DriverManager.getConnection(JDBC_URL);
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	public void release(){
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public interface ResultSetHandler{
		void accept(ResultSet set);
	}
	public void executeStatment(String sql, ResultSetHandler handler){
		try(Statement stmt = conn.createStatement();ResultSet rs = stmt.executeQuery(sql)){
			if (rs != null) {
				handler.accept(rs);
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
