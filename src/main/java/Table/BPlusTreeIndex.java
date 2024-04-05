package Table;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.Serializer;

import Exceptions.DBAppException;

public class BPlusTreeIndex {
    public BPlusTree<Object, String> tree;
    public String indexPath;

    public BPlusTreeIndex(String indexPath, String colType) throws IOException, DBAppException {
        this.indexPath = indexPath;

        // Create the directory if it doesn't exist
        File directory = new File(indexPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        this.create(colType);
    }

    @SuppressWarnings("unchecked")
    private void create(String columnType) throws DBAppException {
        Serializer<?> keySerializer = getSerializer(columnType);

        // Build BPlusTree with retrieved serializer
        this.tree = (BPlusTree<Object, String>) BPlusTree.file()
                .directory(indexPath)
                .maxLeafKeys(32)
                .maxNonLeafKeys(8)
                .segmentSizeMB(1)
                .keySerializer(keySerializer)
                .valueSerializer(Serializer.utf8())
                .naturalOrder();
    }

    public static Serializer<?> getSerializer(String columnType) {
        Map<String, Serializer<?>> serializerMap = new HashMap<>();

        serializerMap.put("java.lang.String", Serializer.utf8(100));
        serializerMap.put("java.lang.Integer", Serializer.INTEGER);
        serializerMap.put("java.lang.Double", Serializer.DOUBLE);

        return serializerMap.get(columnType);
    }
}
