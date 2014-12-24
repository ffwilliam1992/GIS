package com.wonder.db;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import org.postgresql.ds.PGPoolingDataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.sap.db.jdbcext.DataSourceSAP;

public class DBConnectionPool {
	public enum Vendor {
		MYSQL, HANA, POSTGRESQL
	}

	private DataSource ds;

	public DBConnectionPool() {
		this(Vendor.MYSQL);
	}

	public DBConnectionPool(Vendor v) {
		Properties properties = new Properties();
		InputStream prop_in_s = DBConnectionPool.class
				.getResourceAsStream("/db.properties");
		try {
			properties.load(prop_in_s);
			v = Vendor.valueOf(properties.getProperty("vendor").toUpperCase());
			prop_in_s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		switch (v) {
		case MYSQL:
			ds = new MysqlDataSource();
			((MysqlDataSource) ds).setServerName(properties
					.getProperty("ServerName"));
			((MysqlDataSource) ds).setPortNumber(Integer.parseInt(properties
					.getProperty("port")));
			((MysqlDataSource) ds).setDatabaseName(properties
					.getProperty("schema"));
			((MysqlDataSource) ds).setUser(properties.getProperty("user"));
			((MysqlDataSource) ds).setPassword(properties
					.getProperty("password"));
			break;
		case HANA:
			ds = new DataSourceSAP();
			((DataSourceSAP) ds).setServerName(properties
					.getProperty("ServerName"));
			((DataSourceSAP) ds).setPortNumber(Integer.parseInt(properties
					.getProperty("port")));
			((DataSourceSAP) ds).setSchema(properties.getProperty("schema"));
			((DataSourceSAP) ds).setUser(properties.getProperty("user"));
			((DataSourceSAP) ds)
					.setPassword(properties.getProperty("password"));
			break;
		case POSTGRESQL:
			ds = new PGPoolingDataSource();
			((PGPoolingDataSource) ds).setServerName(properties
					.getProperty("ServerName"));
			((PGPoolingDataSource) ds).setPortNumber(Integer
					.parseInt(properties.getProperty("port")));
			((PGPoolingDataSource) ds).setDatabaseName(properties
					.getProperty("schema"));
			((PGPoolingDataSource) ds).setUser(properties.getProperty("user"));
			((PGPoolingDataSource) ds).setPassword(properties
					.getProperty("password"));
			break;
		default:
			System.err.println("vendor not supported!");
		}

	}

	public Connection getConnection() throws Exception
	{
		return ds.getConnection();
	}

	public void putBackConnection(Connection conn) throws Exception
	{
		conn.close();
	}
}
