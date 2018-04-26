package simpledb.testing;

import simpledb.remote.SimpleDriver;

import java.sql.*;

public class SmartSortMergeJoinTest {
    public static void main(String[] args) {
		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("create table A(field1 int, field2 int)");
			System.out.println("Table A created");
			stmt.executeUpdate("insert into A(field1, field2) values (5,7)");
			System.out.println("Table A records inserted");
			stmt.executeUpdate("create table B(field2 int, field3 int)");
			System.out.println("Table B created");
			stmt.executeUpdate("insert into B(field2, field3) values (7,9)");
			System.out.println("Table B records inserted");

			String qry = "select field1, field2, field3 "
			           + "from A, B";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			System.out.println("field1\tfield2\tfield3");
			while (rs.next()) {
				int field1 = rs.getInt("field1");
				int field2 = rs.getInt("field2");
				int field3 = rs.getInt("field3");
				System.out.println(field1 + "\t" + field2 + "\t" + field3);
			}
			rs.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			// Step 4: close the connection
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
