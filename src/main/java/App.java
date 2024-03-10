import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.github.davidmoten.bplustree.*;

public class App {
    public static void main(String[] args) {
        String directoryPath = "indexDirectory";
        Path path = Paths.get(directoryPath);

        // Check if directory exists, if not, create it
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                e.printStackTrace();
                return; // If directory creation failed, exit the program
            }
        }

        BPlusTree<Long, String> tree = BPlusTree
                .file()
                .directory(directoryPath)
                .maxLeafKeys(32)
                .maxNonLeafKeys(8)
                .segmentSizeMB(1)
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.utf8())
                .naturalOrder();

        // insert some values
        tree.insert(1000L, "Hello");
        tree.insert(2000L, "World");

        // search the tree for values with keys between 0 and 3000
        // and print out values only
        tree.find(0L, 3000L).forEach(System.out::println);
    }
}