import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable{
    private Vector<String> pages;
    static int pageCount = 0;

    public Table(){
        pages = new Vector<String>();
    }

    public Vector<String> getPages() {
        return pages;
    }

    public void setPages(Vector<String> pages) {
        this.pages = pages;
    }
}
