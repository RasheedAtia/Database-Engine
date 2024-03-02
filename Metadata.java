import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Metadata {
    private String fileName = "metadata.csv";

    public Metadata() throws IOException {
        // Create FileWriter object with the file path
        FileWriter writer = new FileWriter(fileName);

        // Close the writer
        writer.close();
    }

    public void saveTable(Table t) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));

        // Write each column data to the metadata file
        for (String key : t.getHtblColNameType().keySet()) {
            writer.write(t.name + "," + key + "," + t.getHtblColNameType().get(key) + "," +
                    (key == t.getClusteringKey()) + ",null,null\n");
        }

        // Close the writer
        writer.close();
    }

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
}
