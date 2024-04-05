package Table;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class FileHandler implements Serializable {
    protected void saveInstance(String path) throws IOException {
        // Open streams for writing
        FileOutputStream fileOut = new FileOutputStream(path);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);

        // Write object and close streams
        out.writeObject(this);
        out.close();
        fileOut.close();
    }

    public Object loadInstance(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objIn = new ObjectInputStream(fileIn);
        Object o = objIn.readObject();

        objIn.close();
        fileIn.close();

        return o;
    }
}
