package com.wonder.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class GPS2PostgreImporter {
	private String file_name;
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
				while ((line = input.readLine()) != null) {
					words = line.split(",");
					insert_into_stmt.setObject(1, words[0], java.sql.Types.VARCHAR);
					insert_into_stmt.setObject(2, words[1], java.sql.Types.DOUBLE);
					insert_into_stmt.setObject(3, words[2], java.sql.Types.DOUBLE);
					insert_into_stmt.setObject(4, words[3], java.sql.Types.BOOLEAN);
					insert_into_stmt.setObject(5, words[4]);
					insert_into_stmt.execute();
				}
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
}
