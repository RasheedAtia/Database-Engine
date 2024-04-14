package Table;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;

import Engine.Metadata;
import Engine.Utils;
import Exceptions.DBAppException;
import Table.BTree.BTree;

public class BPlusTreeIndex extends FileHandler {
    public BTree<String, Vector<String>> tree;
    public String tableName;
    public String colName;

    public BPlusTreeIndex(String tableName, String colName, String colType)
            throws IOException, DBAppException, ClassNotFoundException {
        this.tableName = tableName;
        this.colName = colName;
        this.tree = new BTree<String, Vector<String>>();
        fillTree();
        saveTree();
    }

    /**
     * Fills the B+ tree index with data from the table.
     * This method retrieves the table data, iterates through each page, and inserts
     * the values into the B+ tree index.
     * The B+ tree index is used for efficient searching and retrieval of data based
     * on the specified column.
     *
     * @throws IOException            if an I/O error occurs while loading the table
     *                                or page data
     * @throws ClassNotFoundException if the class of a serialized object cannot be
     *                                found during loading
     */
    private void fillTree() throws IOException, ClassNotFoundException {
        String tableDirectory = "src\\main\\java\\Table\\" + tableName + "\\";
        Table table = (Table) super.loadInstance(tableDirectory, tableName);

        if (table == null)
            return;

        Metadata metadata = Metadata.getInstance();
        Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(tableName);
        int colIdx = Utils.getColIndex(htblColNameType, colName);

        for (int pageNum : table.pageNums) {
            Page currPage = table.loadPage(pageNum);

            for (Tuple row : currPage.getTuples()) {
                Vector<String> pageRefs = tree.search(row.getFields()[colIdx].toString());

                if (pageRefs == null) {
                    pageRefs = new Vector<>();
                }
                pageRefs.add("page " + pageNum);

                tree.delete(row.getFields()[colIdx].toString());
                tree.insert(row.getFields()[colIdx].toString(), pageRefs);
            }
        }
    }

    /**
     * Saves the B+ tree index to a file.
     *
     * @throws IOException if an I/O error occurs while saving the tree.
     */
    public void saveTree() throws IOException {
        String indexPath = "src\\main\\java\\Table\\" + this.tableName + "\\Indicies\\";
        super.saveInstance(indexPath, colName);
    }
}
