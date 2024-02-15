package mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLTest { //please ignore this for now

    public static void main(String[] args) {
        // JDBC URL, username, and password of MySQL server
        String url = "jdbc:mysql://localhost:3306/xxxx";
        String user = "xxxxxx";
        String password = "xxxxx";

        // SQL query to insert data
        String InsertQuery = "INSERT INTO OP2_Temp_table (timestamp, OP2) VALUES (?, ?)";

        // SQL query to create a table
        String createQuery = "CREATE TABLE OP2_Temp_table (timestamp TIMESTAMP NOT NULL,OP2 int)";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement preparedStatement = conn.prepareStatement(InsertQuery)) {

            // Set parameters
            preparedStatement.setString(1, "2024-02-12T22:00");
            preparedStatement.setString(2, "48692.0");

            // Execute the query
            preparedStatement.executeUpdate();
            System.out.println("Data inserted successfully.");

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
