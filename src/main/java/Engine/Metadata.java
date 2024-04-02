package Engine;

import Table.Table;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class Metadata {
    private String fileName = "metadata.csv";
    private static Metadata metadataInstance;

    private Metadata() {

    }

    public static Metadata getInstance() {
        if (metadataInstance == null) {
            return new Metadata();
        }

        return metadataInstance;
    }

    /**
     * Saves the table metadata to a file.
     *
     * @param table the table to save
     * @throws IOException if an I/O error occurs while writing to the file
     */
    public void saveTable(Table table) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));

        // Write each column data to the metadata file
        for (String key : table.getHtblColNameType().keySet()) {
            writer.write(table.name + "," + key + "," + table.getHtblColNameType().get(key) + "," +
                    (key == table.getClusteringKey()) + ",null,null\n");
        }

        // Close the writer
        writer.close();
    }

    /**
     * Saves the index name and type for a specified table and column in the
     * metadata file.
     *
     * @param strTableName the name of the table
     * @param strColName   the name of the column
     * @param strIndexName the name of the index
     * @throws IOException if an I/O error occurs while reading or writing the
     *                     metadata file
     */
    public void saveIndex(String strTableName, String strColName, String strIndexName) throws IOException {
        List<String> lines = new ArrayList<>();
        int row = 0;

        // Update row to include the index name & type
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] cells = line.split(",");
            lines.add(line);
            if (cells[0].equals(strTableName) && cells[1].equals(strColName)) {
                cells[4] = strIndexName;
                cells[5] = "B+tree";
                lines.set(row, String.join(",", cells));
            }
            row++;
        }
        reader.close();

        // Write the updated content back to the metadata file
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for (String updatedLine : lines) {
            writer.write(updatedLine);
            writer.newLine();
        }

        writer.close();
    }

    /**
     * Loads the column types for a given table name from the metadata file.
     * 
     * @param tableName the name of the table
     * @return a Hashtable containing the column names as keys and their
     *         corresponding types as values
     * @throws IOException if an I/O error occurs while reading the metadata file
     */
    public Hashtable<String, String> loadColumnTypes(String tableName) throws IOException {
        Hashtable<String, String> htblColNameTypes = new Hashtable<String, String>();

        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] cells = line.split(",");
            String currtableName = cells[0];
            String colName = cells[1];
            String colType = cells[2];

            if (!currtableName.equals(tableName))
                continue;

            htblColNameTypes.put(colName, colType);
        }
        reader.close();

        return htblColNameTypes;
    }
}