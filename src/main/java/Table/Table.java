package Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import com.github.davidmoten.bplustree.BPlusTree;

import Exceptions.DBAppException;

public class Table implements Serializable {
    public String name;
    private String clusteringKey;

    private Hashtable<String, BPlusTree<?, ?>> colIdx;
    private Hashtable<String, String> htblColNameType;

    private Vector<Page> pages;

    public Table(String name, String clusteringKeyColumn, Hashtable<String, String> htblColNameType) {
        pages = new Vector<Page>();
        colIdx = new Hashtable<String, BPlusTree<?, ?>>();

        this.name = name;
        this.clusteringKey = clusteringKeyColumn;
        this.htblColNameType = htblColNameType;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public Hashtable<String, String> getHtblColNameType() {
        return htblColNameType;
    }

    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }

    public Hashtable<String, BPlusTree<?, ?>> getColIdx() {
        return colIdx;
    }

    public void setColIdx(Hashtable<String, BPlusTree<?, ?>> colIdx) {
        this.colIdx = colIdx;
    }

    public Vector<Page> getPages() {
        return pages;
    }

    public void setPages(Vector<Page> pages) {
        this.pages = pages;
    }

    public void addPage(Tuple newRow) throws IOException {
        String pageName = this.name + "_page_" + (this.pages.size() + 1);
        Page page = new Page(pageName, newRow);
        pages.add(page);
        page.savePage();
    }

    public boolean isFull() {
        return (this.pages.size() == 0) || (this.pages.get(this.pages.size() - 1).isFull());
    }

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
                int rowNum = this.pages.get(pageMid).findRow(newRow, clusteringKeyIndex, type);
                pageNum_RowNum = pageMid + "_" + rowNum;
                break;
            }
        }

        return pageNum_RowNum;
    }

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
