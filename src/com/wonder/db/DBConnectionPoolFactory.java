package com.wonder.db;

import java.sql.Connection;

public class DBConnectionPoolFactory {
	private static DBConnectionPool pool;

	private DBConnectionPoolFactory() {

	}

	public static DBConnectionPool getInstance()
	{
		if (pool == null)
			pool = new DBConnectionPool(DBConnectionPool.Vendor.POSTGRESQL);
		return pool;
	}

	public Connection getConnection() throws Exception
	{
		return pool.getConnection();
	}

	public void putBackConnection(Connection conn) throws Exception
	{
		pool.putBackConnection(conn);
	}

}
