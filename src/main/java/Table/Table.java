package Table;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Vector;

import com.github.davidmoten.bplustree.BPlusTree;

import Exceptions.DBAppException;

/**
 * The Table class represents a table in a database.
 * It contains a vector of pages, each of which contains a vector of tuples
 * It also maintains a B+ tree index for each column in the table.
 */
public class Table extends FileHandler {
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
     * Adds a new page to the table with the given tuple as the first row.
     * The page is saved with a unique name based on the number of pages in the
     * table.
     *
     * @param newRow the tuple to be added as the first row of the new page
     * @throws IOException if an I/O error occurs while saving the page
     */
    public void addPage(Tuple newRow) throws IOException {
        Page page = new Page("page " + numOfPages++, newRow);
        page.savePage(this.name);
    }

    /**
     * Deserializes a specific page from the table.
     *
     * @param pageNum the page number to load
     * @return the deserialized page
     * @throws IOException            if an I/O error occurs while reading the page
     *                                file
     * @throws ClassNotFoundException if the class of a serialized object cannot be
     *                                found
     */
    public Page loadPage(int pageNum) throws IOException, ClassNotFoundException {
        String relativePagePath = "src\\main\\java\\Table\\" + this.name + "\\page " + pageNum + ".class";
        return (Page) super.loadInstance(relativePagePath);
    }

    /**
     * Saves/Serializes the table to a file.
     * 
     * @throws IOException if an I/O error occurs while saving the table.
     */
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
        super.saveInstance(filePath);
    }

    /**
     * Inserts new row in correct position.
     * Handle different cases of inserting in new page, or in existing page.
     * 
     * @param htblColNameType  a Hashtable representing the column names and their
     *                         corresponding data types.
     * @param htblColNameValue a Hashtable representing the column names and their
     *                         corresponding values for the new row.
     * @throws DBAppException         if an error occurs while performing the
     *                                database operation.
     * @throws ClassNotFoundException if the specified class cannot be found during
     *                                serialization/deserialization of a page.
     * @throws IOException            if an I/O error occurs during
     *                                serialization/deserialization of a page.
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
        if (lastPage.isFull() && targetPageNum > numOfPages - 1) {
            // insert new row in new page
            this.addPage(newRow);
            return;
        }

        Page targetPage = loadPage(targetPageNum);
        if (targetRowNum >= targetPage.getTuples().size()) {
            targetPage.addTuple(newRow);
            targetPage.savePage(this.name);
            return;
        }

        this.shiftRowsDownAndInsert(targetPageNum, targetRowNum, newRow);
    }

    /**
     * converts form of input for easier insertion.
     * 
     * @param htblColNameType  maps column name to its data type.
     * @param htblColNameValue maps column name to value of insertion.
     * @return tuple containing values to insert.
     */
    private Tuple convertInputToTuple(Hashtable<String, String> htblColNameType,
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

    /**
     * Checks the validity of the column type for a given column in the table.
     * Compares the existing column type with the type of the new value to be
     * inserted.
     * 
     * @param htblColNameValue a Hashtable representing the column names and their
     *                         corresponding values
     * @param htblColNameType  a Hashtable representing the column names and their
     *                         corresponding types
     * @param col              the name of the column to check the type validity for
     * @throws ClassNotFoundException if the class for the existing column type
     *                                cannot be found
     * @throws DBAppException         if the type of the new value does not match
     *                                the existing column type
     */
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
     * Shifts the rows down in the target page and inserts a new tuple at the
     * specified position.
     * 
     * @param targetPageNum The page number of the target page.
     * @param targetRowNum  The row number in the target page where the new tuple
     *                      should be inserted.
     * @param newRow        The new tuple to be inserted.
     * @throws DBAppException         If adding a tuple in a full page.
     * @throws IOException            If an I/O error occurs.
     * @throws ClassNotFoundException If the class of a serialized object cannot be
     *                                found.
     */
    private void shiftRowsDownAndInsert(int targetPageNum, int targetRowNum, Tuple newRow)
            throws DBAppException, IOException, ClassNotFoundException {
        Page targetPage = loadPage(targetPageNum);
        Tuple curr, prev = targetPage.getTuples().get(targetRowNum);
        targetPage.getTuples().set(targetRowNum, newRow);

        for (int row = targetRowNum + 1; row < targetPage.getTuples().size(); row++) {
            curr = targetPage.getTuples().get(row);
            targetPage.getTuples().set(row, prev);
            prev = curr;
        }

        if (targetPage.getTuples().size() < Page.maximumRowsCountInPage) {
            targetPage.addTuple(prev);
            targetPage.savePage(this.name);
            return;
        }

        targetPage.savePage(this.name);

        for (int pageNum = targetPageNum + 1; pageNum < numOfPages; pageNum++) {
            Page currPage = loadPage(pageNum);

            for (int row = 0; row < currPage.getTuples().size(); row++) {
                curr = currPage.getTuples().get(row);
                currPage.getTuples().set(row, prev);
                prev = curr;
            }

            if (currPage.getTuples().size() < Page.maximumRowsCountInPage) {
                currPage.addTuple(prev);
                currPage.savePage(this.name);
                return;
            }

            currPage.savePage(this.name);
        }

        this.addPage(prev);
    }

    /**
     * Returns the insertion position of a new row in the table.
     * 
     * @param newRow          the new row to be inserted
     * @param htblColNameType a hashtable containing the column names and their
     *                        corresponding types
     * @return the insertion position in the format "pageNum_rowNum"
     * @throws DBAppException         if an error occurs in inserting a row
     * @throws IOException            if an I/O error occurs while reading or
     *                                writing data
     * @throws ClassNotFoundException if the specified class cannot be found during
     *                                deserialization
     */
    private String getInsertionPos(Tuple newRow, Hashtable<String, String> htblColNameType)
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

    /**
     * Returns the index of the clustering key in the given hashtable.
     *
     * @param htblColNameType a hashtable that maps column names to their data types
     * @return the index of the clustering key in the hashtable
     */
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
    public static int compareClusteringKey(String targetKey, String currKey, String clusteringKeyType) {
        switch (clusteringKeyType) {
            case "java.lang.integer":
                return Integer.parseInt(targetKey) - Integer.parseInt(currKey);
            case "java.lang.double":
                return Double.compare(Double.parseDouble(targetKey), Double.parseDouble(currKey));
            case "java.lang.string":
                return targetKey.compareTo(currKey);
            default:
                return 0;
        }
    }

    /**
     * Updates a row in the table with the specified column name-value pairs, based
     * on the given clustering key value.
     *
     * @param htblColNameType       a Hashtable containing the column names and
     *                              their corresponding data types
     * @param htblColNameValue      a Hashtable containing the column names and
     *                              their new values
     * @param strClusteringKeyValue the value of the clustering key for the row to
     *                              be updated
     * @throws DBAppException         if an error occurs during the update operation
     * @throws ClassNotFoundException if the required class is not found during
     *                                deserializing
     * @throws IOException            if an I/O error occurs during deserializing a
     *                                page
     */
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
            } else if (comparison2 > 0) {
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
