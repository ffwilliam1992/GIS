package com.wonder.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {
	private static DBUtil dbInstance;

	public static DBUtil getInstance()
	{
		if (null == dbInstance) {
			dbInstance = new DBUtil();
		}
		return dbInstance;
	}

	private static Connection getConnection()
	{
		try {
			return DBConnectionPoolFactory.getInstance().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void closeConn(Connection conn)
	{
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void closeResultSetResource(ResultSet rs)
	{
		Statement stmt = null;
		Connection conn = null;
		try {
			try {
				stmt = rs.getStatement();
				conn = stmt.getConnection();
			} finally {
				rs.close();
				stmt.close();
				conn.close();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static void closeStatementResource(Statement stmt)
	{
		Connection conn = null;
		try {
			try {
				if (!stmt.isClosed())
					conn = stmt.getConnection();
			} finally {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static PreparedStatement createSqlStatement(String p_sql,
			Object... p_parameters)
	{
		Connection connection = DBUtil.getConnection();
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(p_sql);
			for (int i = 0; i < p_parameters.length; i++) {
				Object param = p_parameters[i];
				statement.setObject(i + 1, param);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return statement;
	}

	public static ResultSet executeSQL(String sql, Object... params)
	{
		try {
			PreparedStatement pst = createSqlStatement(sql);
			for (int i = 1; i <= params.length; i++) {
				pst.setObject(i, params[i - 1]);
			}
			return pst.executeQuery();
		} catch (Exception ex) {
			System.out.println("Execute sql error!");
			ex.printStackTrace();
		}
		return null;
	}

	public static PreparedStatement executeUpdate(String sql, Object... params)
	{
		try {
			PreparedStatement pst = createSqlStatement(sql);
			for (int i = 0; i < params.length; i++) {
				pst.setObject(i + 1, params[i]);
			}
			pst.executeUpdate();
			return pst;
		} catch (Exception ex) {
			System.out.println("Execute update error!");
			ex.printStackTrace();
		}
		return null;
	}
}
