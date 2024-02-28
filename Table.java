import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
    String name;
    private String clusteringKey;
    
    private Hashtable<String,String> colIdx;
    private Vector<String> pages;
    
    public Table(String name, String clusteringKeyColumn, Hashtable<String,String> htblColNameType){
        pages = new Vector<String>();
        colIdx = new Hashtable<String,String>();

        this.name = name;
        this.clusteringKey = clusteringKeyColumn;

        for(String key : htblColNameType.keySet()){
			this.colIdx.put(key, "null");
		}
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public Hashtable<String, String> getColIdx() {
        return colIdx;
    }

    public void setColIdx(Hashtable<String, String> colIdx) {
        this.colIdx = colIdx;
    }


    public Vector<String> getPages() {
        return pages;
    }

    public void setPages(Vector<String> pages) {
        this.pages = pages;
    }
}
