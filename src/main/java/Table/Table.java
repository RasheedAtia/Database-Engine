package Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import com.github.davidmoten.bplustree.BPlusTree;

import Exceptions.DBAppException;

/**
 * The Table class represents a table in a database.
 * It contains a vector of pages, each of which contains a vector of tuples
 * It also maintains a B+ tree index for each column in the table.
 */
public class Table implements Serializable {
    // The name of the table
    public String name;

    // The column name of the clustering key
    private String clusteringKey;

    // A hashtable mapping column names to their types
    private Hashtable<String, String> htblColNameType;

    // A hashtable mapping column names to their B+ tree indices
    private Hashtable<String, BPlusTree<?, ?>> colIdx;

    // A vector of pages in the table
    private Vector<Page> pages;

    /**
     * Constructs a new Table with the given name, clustering key, and column types.
     */
    public Table(String name, String clusteringKeyColumn, Hashtable<String, String> htblColNameType) {
        pages = new Vector<Page>();
        colIdx = new Hashtable<String, BPlusTree<?, ?>>();

        this.name = name;
        this.clusteringKey = clusteringKeyColumn;
        this.htblColNameType = htblColNameType;
    }

    /**
     * @return the name of the clustering key column.
     */
    public String getClusteringKey() {
        return clusteringKey;
    }

    /**
     * @param clusteringKey the name of the clustering key column.
     */
    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    /**
     * @return the hashtable mapping column names to their types.
     */
    public Hashtable<String, String> getHtblColNameType() {
        return htblColNameType;
    }

    /**
     * @param htblColNameType Sets the hashtable mapping column names to their
     *                        types.
     */
    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }

    /**
     * @return the hashtable mapping column names to their B+ tree indices.
     */
    public Hashtable<String, BPlusTree<?, ?>> getColIdx() {
        return colIdx;
    }

    /**
     * @param colIdx Sets the hashtable mapping column names to their B+ tree
     *               indices.
     */
    public void setColIdx(Hashtable<String, BPlusTree<?, ?>> colIdx) {
        this.colIdx = colIdx;
    }

    /**
     * @return the vector of pages in the table.
     */
    public Vector<Page> getPages() {
        return pages;
    }

    /**
     * @param pages Sets the vector of pages in the table.
     */
    public void setPages(Vector<Page> pages) {
        this.pages = pages;
    }

    /**
     * @param newRow Adds a new page to the table.
     */
    public void addPage(Tuple newRow) throws IOException {
        String pageName = this.name + "_page_" + (this.pages.size() + 1);
        Page page = new Page(pageName, newRow);
        pages.add(page);
        page.savePage();
    }

    /**
     * @return true if the table is full, false otherwise.
     */
    public boolean isFull() {
        return (this.pages.size() == 0) || (this.pages.get(this.pages.size() - 1).isFull());
    }

    /**
     * @param newRow the tuple that should be inserted
     * @return the position where the new row should be inserted based on clustering
     *         key column.
     */
    public String getInsertionPos(Tuple newRow) throws DBAppException {
        String pageNum_RowNum = "";
        int clusteringKeyIndex = 0;

        for (String col : this.htblColNameType.keySet()) {
            if (col.equals(clusteringKey)) {
                break;
            }
            clusteringKeyIndex++;
        }

        Object targetClusteringKey = newRow.getFields()[clusteringKeyIndex];
        int pageStart = 0;
        int pageEnd = this.pages.size() - 1;
        int pageMid = 0;

        while (pageStart <= pageEnd) {
            pageMid = pageStart + (pageEnd - pageStart) / 2;

            Vector<Tuple> currPageContent = this.pages.get(pageMid).getTuples();

            Object firstRow = currPageContent.get(0).getFields()[clusteringKeyIndex];
            Object lastRow = currPageContent.get(currPageContent.size() - 1).getFields()[clusteringKeyIndex];

            String type = this.getHtblColNameType().get(clusteringKey).toLowerCase();
            int comparison1 = compareClusteringKey(targetClusteringKey, firstRow, type);
            int comparison2 = compareClusteringKey(targetClusteringKey, lastRow, type);

            if (comparison1 < 0) {
                pageEnd = pageMid - 1;
                pageNum_RowNum = pageMid + "_0";
            } else if (comparison2 >= 0) {
                pageStart = pageMid + 1;
                pageNum_RowNum = pageMid + "_" + this.pages.get(pageMid).getTuples().size();
            } else {
                int rowNum = this.pages.get(pageMid).findInsertionRow(newRow, clusteringKeyIndex, type);
                pageNum_RowNum = pageMid + "_" + rowNum;
                break;
            }
        }

        return pageNum_RowNum;
    }

    /**
     * @param targetKey         first clustering key in comparison.
     * @param currKey           second clustering key in comparison.
     * @param clusteringKeyType type of clustering key.
     * @return which key is larger (> 0 means targetKey is larger,
     *         < 0 means currKey is larger, = 0 means both keys are equal)
     */
    public static int compareClusteringKey(Object targetKey, Object currKey, String clusteringKeyType)
            throws DBAppException {
        switch (clusteringKeyType) {
            case "java.lang.integer":
                return (Integer) targetKey - (Integer) currKey;
            case "java.lang.double":
                return Double.compare((Double) targetKey, (Double) currKey);
            case "java.lang.string":
                return ((String) targetKey).compareTo((String) currKey);
            default:
                throw new DBAppException("Unsupported column type");
        }
    }

}
