package com.wonder.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.prefs.BackingStoreException;

public class GPS2PostgreImporter {
	private String file_name;
	private int batch_size = 1000;
	PreparedStatement insert_into_stmt;

	public GPS2PostgreImporter() {

	}

	public GPS2PostgreImporter(String file_name,
			PreparedStatement insert_into_stmt) {
		this.file_name = file_name;
		this.insert_into_stmt = insert_into_stmt;
	}

	public void run()
	{
		try {
			BufferedReader input = null;
			try {
				input = new BufferedReader(new FileReader(file_name));
				String line;
				String words[];
				int cnt = 0;
				while ((line = input.readLine()) != null) {
					words = line.split(",");
					insert_into_stmt.setObject(1, words[0],
							java.sql.Types.VARCHAR);
					insert_into_stmt.setObject(2, words[1],
							java.sql.Types.DOUBLE);
					insert_into_stmt.setObject(3, words[2],
							java.sql.Types.DOUBLE);
					insert_into_stmt.setObject(4, words[3],
							java.sql.Types.BOOLEAN);
					insert_into_stmt.setObject(5, words[4], java.sql.Types.BIGINT);
					insert_into_stmt.addBatch();
					++cnt;
					if (cnt % batch_size == 0) {
						insert_into_stmt.executeBatch();
						insert_into_stmt.clearBatch();
						insert_into_stmt.getConnection().commit();
						System.out.println(cnt + " inserted");
					}
				}
				insert_into_stmt.executeBatch();
				insert_into_stmt.clearBatch();
				insert_into_stmt.getConnection().commit();
				System.out.println(cnt + " inserted. Done!");
			} finally {
				if (input != null)
					input.close();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public int getBatch_size()
	{
		return batch_size;
	}

	public void setBatch_size(int batch_size)
	{
		this.batch_size = batch_size;
	}
}
