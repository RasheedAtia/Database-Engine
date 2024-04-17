package Table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class FileHandler implements Serializable {
    public void saveInstance(String path, String fileName) throws IOException {

        // Create the directory if it doesn't exist
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdirs(); // Create all necessary directories in the path
        }

        path += fileName + ".class";

        // Open streams for writing
        try (FileOutputStream fileOut = new FileOutputStream(path);
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            // Write object and close streams
            out.writeObject(this);
        }
    }

    public Object loadInstance(String path, String fileName) throws IOException, ClassNotFoundException {
        // Check if directory exists
        File directory = new File(path);
        if (!directory.exists()) {
            return null;
        }

        // Check if file exists
        path += fileName + ".class";
        directory = new File(path);
        if (!directory.exists()) {
            return null;
        }

        Object o;
        try (FileInputStream fileIn = new FileInputStream(path);
                ObjectInputStream objIn = new ObjectInputStream(fileIn)) {

            o = objIn.readObject();
        }
        return o;
    }
}
