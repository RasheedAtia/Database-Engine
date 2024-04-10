package Table;

import java.io.IOException;
import java.util.Vector;

import Exceptions.DBAppException;
import Table.BTree.BTree;

public class BPlusTreeIndex extends FileHandler {
    public BTree<String, Vector<String>> tree;
    public String tableName;
    public String colName;

    public BPlusTreeIndex(String tableName, String colName, String colType) throws IOException, DBAppException {
        this.tableName = tableName;
        this.colName = colName;
        this.tree = new BTree<String, Vector<String>>();
        // fillTree();
        this.saveTree();
    }

    public void saveTree() throws IOException {
        String indexPath = "src\\main\\java\\Table\\" + this.tableName + "\\Indicies\\";
        super.saveInstance(indexPath, colName);
    }
}
