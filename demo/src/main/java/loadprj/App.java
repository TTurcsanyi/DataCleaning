package loadprj;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args){
        String csvFile = "C:\\Users\\turcs\\Downloads\\raw_product_data.csv"; // Replace with the path to your CSV file
        int maxRows = 20; // We want to read top 20 rows
        String sqlFilePath = "C:\\Users\\turcs\\DataCleaning\\demo\\src\\main\\java\\loadprj\\CreateTables.sql"; // Path to your SQL file
        String groupColumn = "column_name_to_group";  // Column you want to group by
        Map<String, Integer> linkMap = new HashMap<>(); // Map to store group value and its ID
        try (Reader reader = new FileReader(csvFile);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {

            int rowCount = 0;

            for (CSVRecord csvRecord : csvParser) {
                if (rowCount >= maxRows) {
                    break;
                }

                // Print each value in the row
                for (String value : csvRecord) {
                    System.out.print(value + " ");
                }
                System.out.println();

                rowCount++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        //Create the tables
        /* 
         try (var connection =  DB.connect();
            BufferedReader breader = new BufferedReader(new FileReader(sqlFilePath))) {
            
            // Read SQL file and execute queries
            StringBuilder sqlQuery = new StringBuilder();
            String line;
            while ((line = breader.readLine()) != null) {
                // Ignore comments or empty lines
                if (line.trim().isEmpty() || line.trim().startsWith("--")) {
                    continue;
                }
                sqlQuery.append(line);
                // If the line ends with a semicolon, it's the end of a query
                if (line.trim().endsWith(";")) {
                    executeQuery(connection, sqlQuery.toString());
                    sqlQuery.setLength(0); // Clear the StringBuilder for the next query
                }
            }
        } catch (SQLException | IOException e) {
            System.err.println(e.getMessage());
        }
        */
        
        
        
    }
    private static void executeQuery(Connection connection, String query) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query); // Execute the query
            System.out.println("Executed: " + query);
        } catch (SQLException e) {
            System.err.println("Error executing query: " + query);
            e.printStackTrace();
        }
    }
}
