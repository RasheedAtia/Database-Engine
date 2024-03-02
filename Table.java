import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    String name;
    private String clusteringKey;

    private Hashtable<String, bplustree> colIdx;
    private Hashtable<String, String> htblColNameType;

    private Vector<String> pages;

    public Table(String name, String clusteringKeyColumn, Hashtable<String, String> htblColNameType) {
        pages = new Vector<String>();
        colIdx = new Hashtable<String, bplustree>();

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

    public Hashtable<String, bplustree> getColIdx() {
        return colIdx;
    }

    public void setColIdx(Hashtable<String, bplustree> colIdx) {
        this.colIdx = colIdx;
    }

    public Vector<String> getPages() {
        return pages;
    }

    public void setPages(Vector<String> pages) {
        this.pages = pages;
    }
}
