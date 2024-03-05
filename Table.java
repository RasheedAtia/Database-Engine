import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    String name;
    private String clusteringKey;

    private Hashtable<String, bplustree> colIdx;
    private Hashtable<String, String> htblColNameType;

    private Vector<Page> pages;

    public Table(String name, String clusteringKeyColumn, Hashtable<String, String> htblColNameType) {
        pages = new Vector<Page>();
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
}
