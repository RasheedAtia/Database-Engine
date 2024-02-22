import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

/**
 * The Page class represents a page in a database table.
 * It contains a vector of tuples, which represent the rows in the page.
 */
public class Page implements Serializable{
    // Vector of tuples in the page
    private Vector<Tuple> tuples;

    /**
     * Returns the vector of tuples in the page.
     * @return Vector of tuples
     */
    public Vector<Tuple> getTuples() {
        return tuples;
    }

    /**
     * Sets the vector of tuples in the page.
     * @param tuples Vector of tuples
     */
    public void setTuples(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }

    /**
     * Returns a string representation of the page.
     * The string is a comma-separated list of the string representations of the tuples.
     * @return String representation of the page
     */
    public String toString(){
        String res = "";
        for(int i = 0; i < tuples.size(); i++){
            res += tuples.get(i).toString() + ",";
        }
        return res.substring(0,res.length() - 1);
    }

    public boolean isFull(){
        return tuples.size() == 200;
    }

    /**
     * Main method for testing the Page class.
     * Creates a page and adds two tuples to it, then prints the page.
     * @param args Command-line arguments (not used)
     */
    public static void main(String[] args){
        Page p = new Page();
        Tuple v1 = new Tuple("Yousef", 20, "zdfg");
        Tuple v2 = new Tuple("Seif", 19, "xcfhbf");

        p.tuples = new Vector<Tuple>();
        p.tuples.add(v1);
        p.tuples.add(v2);
        System.out.print(p);

        try {
         FileOutputStream fileOut = new FileOutputStream("page1.class");
         ObjectOutputStream out = new ObjectOutputStream(fileOut);
         out.writeObject(p);
         out.close();
         fileOut.close();
      } catch (IOException i) {
         i.printStackTrace();
      }
    }
}