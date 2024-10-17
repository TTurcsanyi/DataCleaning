package loadprj;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * This is my first JAVA application, I've put it together with the help of ChatGPT and Stackoverflow. 
 * It creates the DB tables for itself, parses input from a CSV file and stores the data in the tables.
 * The location of the .csv should be entered here - I think it should be configurable some nicer way.
 * Same for the CreateTable.sql.
 * Authentication data for the Database connection should be entered in the db.properties file.
 * 
 */
public class App 
{
    private static final String csvFile = "raw_product_data.csv"; // Replace with the path to your CSV file
    private static final String sqlFilePath = "CreateTables.sql"; // Path to CreateTable SQL file
    public static void main(String[] args){
        //Create the tables 
        //Ideally this part is only initialization, so it should not be part of the main function, as we don't want to create table every run.
        try (var connection =  DB.connect();) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Non-grouped columns variant_id product_id product_name product_type
        //Grouped columns     size_label brand color age_group gender size_type 
        //Considering grouping on product_name (originally I used the #2), given that from the 3 millions raw there are ~400k different product. With this it does not seem to worth, but maybe on a larger scale it would.
        String groupColumn1 = "size_label";  
        
        String groupColumn3 = "brand";  
        String groupColumn4 = "color";  
        String groupColumn5 = "age_group";
        String groupColumn6 = "gender";
        String groupColumn7 = "size_type";  

        // Maps to store unique group values and their corresponding IDs for each link table
        Map<String, Integer> linkMap1 = new HashMap<>();
        
        Map<String, Integer> linkMap3 = new HashMap<>();
        Map<String, Integer> linkMap4 = new HashMap<>();
        Map<String, Integer> linkMap5 = new HashMap<>();
        Map<String, Integer> linkMap6 = new HashMap<>();
        Map<String, Integer> linkMap7 = new HashMap<>();
        
        try (Reader reader = new FileReader(csvFile);
            var connection =  DB.connect();
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader())) {

            int batchCount = 0;
             // Prepare SQL insert statements for the link tables
             //Grouped columns     size_label brand color age_group gender size_type 
            PreparedStatement linkStmt1 = connection.prepareStatement(
                "INSERT INTO link_size_label (group_value) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement linkStmt3 = connection.prepareStatement(
                "INSERT INTO link_brand (group_value) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement linkStmt4 = connection.prepareStatement(
                    "INSERT INTO link_color (group_value) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement linkStmt5 = connection.prepareStatement(
                    "INSERT INTO link_age_group (group_value) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement linkStmt6 = connection.prepareStatement(
                    "INSERT INTO link_gender (group_value) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement linkStmt7 = connection.prepareStatement(
                 "INSERT INTO link_size_type (group_value) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        
            // Prepare SQL insert statement for the main data table
            PreparedStatement dataStmt = connection.prepareStatement(
                    "INSERT INTO data_table (variant_id, product_id, product_name, product_type, size_label, brand, color, age_group, gender, size_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            connection.setAutoCommit(false); // Use batch processing for better performance
            
            //Going over the raw input line by line, building up hash tables to fill up link tables
            for (CSVRecord record : csvParser) {
                
                // Step 1: Handle the group columns (case-insensitive grouping)
                // This would be the part where further data cleaning should happen.
                String groupValue1 = record.get(groupColumn1).toLowerCase(); 
                
                String groupValue3 = record.get(groupColumn3).toLowerCase();
                String groupValue4 = record.get(groupColumn4).toLowerCase();
                String groupValue5 = record.get(groupColumn5).toLowerCase();
                String groupValue6 = record.get(groupColumn6).toLowerCase();
                String groupValue7 = record.get(groupColumn7).toLowerCase(); 
                //TODO:
                // Investigate if any value is empty and try to fill it with values according to what I described in my email, or get rid of the row entirely.
                
                // Check if group value is already in link table
                // TODO: This is not a sophisticated solution, only checks for complete match, further refinement is possible as I noted in my email.
                // NOTE: Obviously this solution is only working for a clean state, does not load already stored values from DB, as I would expect it in case of a continously running service.
                // size_label  
                Integer linkId1 = linkMap1.get(groupValue1);
                if (linkId1 == null) {
                    // Insert new group value into the corresponding link table
                    linkStmt1.setString(1, groupValue1);
                    linkStmt1.executeUpdate();
                    ResultSet rs1 = linkStmt1.getGeneratedKeys();
                    if (rs1.next()) {
                        linkId1 = rs1.getInt(1); // Get the generated ID
                        linkMap1.put(groupValue1, linkId1); // Store in map for future reference
                    }
                }
                //brand 
                Integer linkId3 = linkMap3.get(groupValue3);
                if (linkId3 == null) {
                    linkStmt3.setString(1, groupValue3);
                    linkStmt3.executeUpdate();
                    ResultSet rs3 = linkStmt3.getGeneratedKeys();
                    if (rs3.next()) {
                        linkId3 = rs3.getInt(1); 
                        linkMap3.put(groupValue3, linkId3); 
                    }
                }
                //color 
                Integer linkId4 = linkMap4.get(groupValue4);
                if (linkId4 == null) {
                    linkStmt4.setString(1, groupValue4);
                    linkStmt4.executeUpdate();
                    ResultSet rs4 = linkStmt4.getGeneratedKeys();
                    if (rs4.next()) {
                        linkId4 = rs4.getInt(1); 
                        linkMap4.put(groupValue4, linkId4); 
                    }
                }
                //age_group
                Integer linkId5 = linkMap5.get(groupValue5);
                if (linkId5 == null) {
                    linkStmt5.setString(1, groupValue5);
                    linkStmt5.executeUpdate();
                    ResultSet rs5 = linkStmt5.getGeneratedKeys();
                    if (rs5.next()) {
                        linkId5 = rs5.getInt(1); 
                        linkMap5.put(groupValue5, linkId5); 
                    }
                }
                //gender 
                Integer linkId6 = linkMap6.get(groupValue6);
                if (linkId6 == null) {
                    linkStmt6.setString(1, groupValue6);
                    linkStmt6.executeUpdate();
                    ResultSet rs6 = linkStmt6.getGeneratedKeys();
                    if (rs6.next()) {
                        linkId6 = rs6.getInt(1); 
                        linkMap6.put(groupValue6, linkId6); 
                    }
                }
                //size_type
                Integer linkId7 = linkMap7.get(groupValue7);
                if (linkId7 == null) {
                    linkStmt7.setString(1, groupValue7);
                    linkStmt7.executeUpdate();
                    ResultSet rs7 = linkStmt7.getGeneratedKeys();
                    if (rs7.next()) {
                        linkId7 = rs7.getInt(1);
                        linkMap7.put(groupValue7, linkId7);
                    }
                }

                
                // Step 2: Insert other columns (variant_id product_id, product_name, product_type) into data_table
                dataStmt.setString(1, record.get("variant_id"));
                // Convert "product_id" to an integer and handle the potential exception
                try {
                    int prod_idValue = Integer.parseInt(record.get("product_id"));
                    dataStmt.setInt(2, prod_idValue); // Insert as an integer
                } catch (NumberFormatException e) {
                    System.err.println("Invalid integer in product_id: " + record.get("product_id"));
                    dataStmt.setNull(2, java.sql.Types.INTEGER); // Insert NULL if the value is not an integer
                }
                
                dataStmt.setString(3, record.get("product_name"));
                dataStmt.setString(4, record.get("product_type"));
                
                
                // Reference to link_tables
                dataStmt.setInt(5, linkId1);
                dataStmt.setInt(6, linkId3); 
                dataStmt.setInt(7, linkId4); 
                dataStmt.setInt(8, linkId5); 
                dataStmt.setInt(9, linkId6); 
                dataStmt.setInt(10, linkId7); 
                dataStmt.addBatch(); // Add to batch
                                
                // Execute batch and commit every 1000 rows for performance reasons
                if (csvParser.getCurrentLineNumber() % 1000 == 0) {
                    batchCount++;
                    dataStmt.executeBatch();
                    connection.commit(); // Commit the batch
                    System.out.println(batchCount + ". batch of 1000 records commited.");
                } 
            }
            // Final execution for remaining records
            dataStmt.executeBatch();
            connection.commit(); // Commit all transactions
            System.out.println("Committed the LAST batch.");


        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }    
    }

    private static void createTables(Connection connection){
        try (BufferedReader breader = new BufferedReader(new FileReader(sqlFilePath))) {
        
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
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
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
