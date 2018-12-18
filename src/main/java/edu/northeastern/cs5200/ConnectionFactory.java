package edu.northeastern.cs5200;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://cs5200-fall2018-ssaurav.cays9zoc70lu.us-east-1.rds.amazonaws.com/cs5200_fall2018_ssaurav_ext_cr";
	private static final String USER = "xyz";
	private static final String PASSWORD = "dshsdhds";
	private static java.sql.Connection dbConnection = null;

	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName(DRIVER);

		if (dbConnection == null) {
			dbConnection = DriverManager.getConnection(URL, USER, PASSWORD);
			return dbConnection;
		} else {
			return dbConnection;
		}
	}

	public static void closeConnection(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
