package Table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

    // A hashtable mapping column names to their B+ tree indices
    private Hashtable<String, BPlusTree<?, ?>> colIdx;

    public int numOfPages = 0;

    /**
     * Constructs a new Table with the given name, clustering key, and column types.
     */
    public Table(String name, String clusteringKeyColumn) {
        colIdx = new Hashtable<String, BPlusTree<?, ?>>();
        this.name = name;
        this.clusteringKey = clusteringKeyColumn;
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
     * @param newRow Adds a new page to the table.
     */
    public void addPage(Tuple newRow) throws IOException {
        Page page = new Page("page " + numOfPages++, newRow);
        page.savePage(this.name);
    }

    public Page loadPage(int pageNum) throws IOException, ClassNotFoundException {
        String relativePagePath = "src\\main\\java\\Table\\" + this.name + "\\page " + pageNum + ".class";
        FileInputStream fileIn = new FileInputStream(relativePagePath);
        ObjectInputStream objIn = new ObjectInputStream(fileIn);
        Page p = (Page) objIn.readObject();

        objIn.close();
        fileIn.close();
        return p;
    }

    public void saveTable() throws IOException {
        // Create the directory path with the table name
        String directoryPath = "src\\main\\java\\Table\\" + this.name + "\\";

        // Create the directory if it doesn't exist
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs(); // Create all necessary directories in the path
        }

        // Build the full file path with table name directory
        String filePath = directoryPath + this.name + ".class";

        // Open streams for writing
        FileOutputStream fileOut = new FileOutputStream(filePath);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);

        // Write object and close streams
        out.writeObject(this);
        out.close();
        fileOut.close();
    }

    /**
     * Inserts new row in correct position.
     * Handle different cases of inserting in new page, or in existing page.
     * 
     * @param htblColNameValue contains mapping of column name to value to insert.
     */
    public void insertRow(Hashtable<String, String> htblColNameType, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, ClassNotFoundException, IOException {
        Tuple newRow = this.convertInputToTuple(htblColNameType, htblColNameValue);

        if (numOfPages == 0) {
            this.addPage(newRow);
            return;
        }

        String[] insertionPos = this.getInsertionPos(newRow, htblColNameType).split("_");

        int targetPageNum = Integer.parseInt(insertionPos[0]);
        int targetRowNum = Integer.parseInt(insertionPos[1]);

        Page lastPage = loadPage(numOfPages - 1);
        if (lastPage.isFull()) {

            // insert new row in new page
            if (targetPageNum > numOfPages - 1) {
                this.addPage(newRow);
                return;
            }

            Page newPage = new Page("page " + numOfPages++);
            newPage.savePage(this.name);
        }

        this.shiftRowsDown(targetPageNum);

        Page newPage, targetPage = loadPage(targetPageNum);
        Vector<Tuple> currPageTuples = targetPage.getTuples();

        if (targetRowNum == 0) {
            newPage = new Page("page " + targetPageNum, newRow);
        } else {
            newPage = new Page("page " + targetPageNum);
        }

        for (int row = 0; row <= currPageTuples.size(); row++) {
            if (row == targetRowNum && row != 0)
                newPage.addTuple(newRow);

            if (row < Math.min(Page.maximumRowsCountInPage - 1, currPageTuples.size()))
                newPage.addTuple(currPageTuples.get(row));
        }

        newPage.savePage(this.name);
    }

    /**
     * converts form of input for easier insertion.
     * 
     * @param htblColNameValue maps column name to value of insertion.
     * @return tuple containing values to insert.
     */
    public Tuple convertInputToTuple(Hashtable<String, String> htblColNameType,
            Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException {

        // fill tuple with values from input parameter htblColNameValue
        Tuple newTuple = new Tuple();

        Object[] newFields = new Object[htblColNameType.size()];
        int pos = 0;

        for (String col : htblColNameValue.keySet()) {
            checkColTypeValidity(htblColNameValue, htblColNameType, col);

            newFields[pos++] = htblColNameValue.get(col);
        }

        newTuple.setFields(newFields);

        return newTuple;
    }

    private void checkColTypeValidity(Hashtable<String, Object> htblColNameValue,
            Hashtable<String, String> htblColNameType, String col)
            throws ClassNotFoundException, DBAppException {

        Object existingColValueType = Class.forName(htblColNameType.get(col));
        Object newColValueType = htblColNameValue.get(col).getClass();

        if (!existingColValueType.equals(newColValueType)) {
            throw new DBAppException("Invalid insert type for column " + col);
        }
    }

    /**
     * Shifts last row in each page (if full) to next page to free a row to insert
     * into.
     * 
     * @param targetPageNum threshold to shift all rows from after this page till
     *                      the end.
     */
    private void shiftRowsDown(int targetPageNum) throws DBAppException, IOException, ClassNotFoundException {
        for (int pageNum = numOfPages - 1; pageNum > targetPageNum; pageNum--) {
            Page prevPage = loadPage(pageNum - 1);
            Page currPage = loadPage(pageNum);
            Vector<Tuple> prevPageTuples = prevPage.getTuples();
            Vector<Tuple> currPageTuples = currPage.getTuples();

            Tuple lastRowPrevPage = prevPageTuples.get(prevPageTuples.size() - 1);
            Page newPage = new Page("page " + pageNum, lastRowPrevPage);

            for (int row = 0; row < Math.min(currPageTuples.size(), Page.maximumRowsCountInPage - 1); row++) {
                newPage.addTuple(currPageTuples.get(row));
            }

            newPage.savePage(this.name);
        }
    }

    /**
     * @param newRow the tuple that should be inserted
     * @return the position where the new row should be inserted based on clustering
     *         key column.
     */
    public String getInsertionPos(Tuple newRow, Hashtable<String, String> htblColNameType)
            throws DBAppException, IOException, ClassNotFoundException {
        String pageNum_RowNum = "";
        int clusteringKeyIndex = getClusteringKeyIndex(htblColNameType);

        String targetClusteringKey = newRow.getFields()[clusteringKeyIndex] + "";
        int pageStart = 0;
        int pageEnd = numOfPages - 1;
        int pageMid = 0;

        while (pageStart <= pageEnd) {
            pageMid = pageStart + (pageEnd - pageStart) / 2;

            Page currPage = loadPage(pageMid);
            Vector<Tuple> currPageContent = currPage.getTuples();

            String firstRow = currPageContent.get(0).getFields()[clusteringKeyIndex] + "";
            String lastRow = currPageContent.get(currPageContent.size() - 1).getFields()[clusteringKeyIndex] + "";

            String type = htblColNameType.get(clusteringKey).toLowerCase();
            int comparison1 = compareClusteringKey(targetClusteringKey, firstRow, type);
            int comparison2 = compareClusteringKey(targetClusteringKey, lastRow, type);

            if (comparison1 < 0) {
                pageEnd = pageMid - 1;
                pageNum_RowNum = pageMid + "_0";
            } else if (comparison2 >= 0) {
                pageStart = pageMid + 1;
                pageNum_RowNum = pageMid + "_" + currPageContent.size();
            } else {
                int rowNum = currPage.findInsertionRow(newRow, clusteringKeyIndex, type);
                pageNum_RowNum = pageMid + "_" + rowNum;
                break;
            }
        }

        int pageNum = Integer.parseInt(pageNum_RowNum.split("_")[0]);
        int rowNum = Integer.parseInt(pageNum_RowNum.split("_")[1]);

        if (rowNum == Page.maximumRowsCountInPage) {
            return (pageNum + 1) + "_0";
        }

        return pageNum_RowNum;
    }

    private int getClusteringKeyIndex(Hashtable<String, String> htblColNameType) {
        int clusteringKeyIndex = 0;
        for (String col : htblColNameType.keySet()) {
            if (col.equals(clusteringKey)) {
                break;
            }
            clusteringKeyIndex++;
        }

        return clusteringKeyIndex;
    }

    /**
     * @param targetKey         first clustering key in comparison.
     * @param currKey           second clustering key in comparison.
     * @param clusteringKeyType type of clustering key.
     * @return which key is larger (> 0 means targetKey is larger,
     *         < 0 means currKey is larger, = 0 means both keys are equal)
     */
    public static int compareClusteringKey(String targetKey, String currKey, String clusteringKeyType)
            throws DBAppException {
        switch (clusteringKeyType) {
            case "java.lang.integer":
                return Integer.parseInt(targetKey) - Integer.parseInt(currKey);
            case "java.lang.double":
                return Double.compare(Double.parseDouble(targetKey), Double.parseDouble(currKey));
            case "java.lang.string":
                return targetKey.compareTo(currKey);
            default:
                throw new DBAppException("Unsupported column type");
        }
    }

    public void updateRow(Hashtable<String, String> htblColNameType, Hashtable<String, Object> htblColNameValue,
            String strClusteringKeyValue)
            throws DBAppException, ClassNotFoundException, IOException {

        String clusteringKey = this.getClusteringKey();
        int clusteringKeyIndex = this.getClusteringKeyIndex(htblColNameType);
        int pageStart = 0;
        int pageEnd = numOfPages - 1;
        int pageMid = 0;

        while (pageStart <= pageEnd) {
            pageMid = pageStart + (pageEnd - pageStart) / 2;

            Page currPage = loadPage(pageMid);
            Vector<Tuple> currPageContent = currPage.getTuples();

            String firstRow = currPageContent.get(0).getFields()[clusteringKeyIndex] + "";
            String lastRow = currPageContent.get(currPageContent.size() - 1).getFields()[clusteringKeyIndex] + "";

            String type = htblColNameType.get(clusteringKey).toLowerCase();
            int comparison1 = compareClusteringKey(strClusteringKeyValue, firstRow, type);
            int comparison2 = compareClusteringKey(strClusteringKeyValue, lastRow, type);

            if (comparison1 < 0) {
                pageEnd = pageMid - 1;
            } else if (comparison2 >= 0) {
                pageStart = pageMid + 1;
            } else {
                int begin = 0;
                int end = currPageContent.size();

                while (begin <= end) {
                    int mid = begin + (end - begin) / 2;
                    String currRow = currPageContent.get(mid).getFields()[clusteringKeyIndex] + "";
                    int comparison = compareClusteringKey(strClusteringKeyValue, currRow, type);

                    if (comparison == 0) {
                        Tuple t = currPageContent.get(mid);

                        for (String col : htblColNameValue.keySet()) {
                            int colIndex = 0;
                            for (String colName : htblColNameType.keySet()) {
                                if (colName.equals(col)) {
                                    break;
                                }
                                colIndex++;
                            }
                            t.getFields()[colIndex] = htblColNameValue.get(col);
                        }
                        currPage.savePage(this.name);
                        return;
                    } else if (comparison < 0) {
                        end = mid - 1;
                    } else {
                        begin = mid + 1;
                    }
                }
            }
        }
    }
}
