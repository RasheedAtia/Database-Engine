package Table;

import Exceptions.DBAppException;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

/**
 * The Page class represents a page in a database table.
 * It contains a vector of tuples, which represent the rows in the page.
 */
public class Page implements Serializable {
    // Vector of tuples in the page
    private Vector<Tuple> tuples;
    public String name;
    private int maximumRowsCountInPage;

    public Page(String name, Tuple tuple) {
        this.name = name;
        this.tuples = new Vector<Tuple>();
        this.tuples.add(tuple);
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(
                    "C:\\Users\\tefah\\OneDrive\\Desktop\\Github\\Database-Engine\\src\\main\\java\\resources\\DBApp.config"));
            String maxRowsCountInPageStr = prop.getProperty("MaximumRowsCountinPage");
            maximumRowsCountInPage = Integer.parseInt(maxRowsCountInPageStr);
            System.out.println("maximumRowsCountInPage: " + maximumRowsCountInPage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the vector of tuples in the page.
     * 
     * @return Vector of tuples
     */
    public Vector<Tuple> getTuples() {
        return tuples;
    }

    /**
     * Sets the vector of tuples in the page.
     * 
     * @param tuples Vector of tuples
     */
    public void setTuples(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }

    /**
     * Returns a string representation of the page.
     * The string is a comma-separated list of the string representations of the
     * tuples.
     * 
     * @return String representation of the page
     */
    public String toString() {
        String res = "";
        for (int i = 0; i < tuples.size(); i++) {
            res += tuples.get(i).toString() + ",";
        }
        return res.substring(0, res.length() - 1);
    }

    public boolean isFull() {
        return tuples.size() == maximumRowsCountInPage;
    }

    public void addTuple(Tuple t) throws DBAppException {
        if (isFull()) {
            throw new DBAppException("Page is full");
        }
        tuples.add(t);
    }

    public void removeTuple(Tuple t) {
        // if page is empty after removing, throw exception

        tuples.remove(t);
    }

    public void updateTuple(Tuple oldRow, Tuple newRow) {
        int index = tuples.indexOf(oldRow);
        tuples.set(index, newRow);
    }

    public void savePage() throws IOException {
        FileOutputStream fileOut = new FileOutputStream(this.name + ".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
    }

    /**
     * Main method for testing the Page class.
     * Creates a page and adds two tuples to it, then prints the page.
     * 
     * @param args Command-line arguments (not used)
     */
    public static void main(String[] args) {
        Tuple v1 = new Tuple("Yousef", 20, "zdfg");
        Tuple v2 = new Tuple("Seif", 19, "xcfhbf");
        Page p = new Page("page1", v1);

        p.tuples.add(v2);
        System.out.print(p);

        try {
            p.savePage();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }
}