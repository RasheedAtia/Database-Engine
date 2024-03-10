package Table;

import Exceptions.DBAppException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

/**
 * The Page class represents a page in a database table.
 * It contains a vector of tuples, which represent the rows in the page.
 */
public class Page implements Serializable {
    // Vector of tuples in the page
    private Vector<Tuple> tuples;
    public String name;

    /**
     * Constructor for the Page class.
     * Initializes the name of the page and adds the first tuple to the page.
     *
     * @param name  The name of the page
     * @param tuple The first tuple to be added to the page
     */
    public Page(String name, Tuple tuple) {
        this.name = name;
        this.tuples = new Vector<Tuple>();
        this.tuples.add(tuple);
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

    /**
     * Checks if the page is full.
     * Currently, a page is considered full if it contains 200 tuples.
     * TODO: Get the page size from DBApp.config
     *
     * @return true if the page is full, false otherwise
     */
    public boolean isFull() {
        return tuples.size() == 200;
    }

    /**
     * Adds a tuple to the page.
     * Throws a DBAppException if the page is full.
     *
     * @param t The tuple to be added
     * @throws DBAppException If the page is full
     */
    public void addTuple(Tuple t) throws DBAppException {
        if (isFull()) {
            throw new DBAppException("Page is full");
        }
        tuples.add(t);
    }

    /**
     * Removes a tuple from the page.
     *
     * @param t The tuple to be removed
     */
    public void removeTuple(Tuple t) {
        // if page is empty after removing, throw exception

        tuples.remove(t);
    }

    /**
     * Updates a tuple in the page.
     *
     * @param oldRow The old tuple
     * @param newRow The new tuple
     */
    public void updateTuple(Tuple oldRow, Tuple newRow) {
        int index = tuples.indexOf(oldRow);
        tuples.set(index, newRow);
    }

    /**
     * Saves the page to disk.
     *
     * @throws IOException If an I/O error occurs
     */
    public void savePage() throws IOException {
        FileOutputStream fileOut = new FileOutputStream(this.name + ".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
    }

    /**
     * Finds the appropriate row index for inserting a target row into the page
     * based on the clustering key.
     *
     * @param targetRow          the target row to be inserted
     * @param clusteringKeyIndex the index of the clustering key in the row
     * @param clusteringKeyType  the data type of the clustering key
     * @return the index of the row where the target row should be inserted
     * @throws DBAppException if an error occurs during the insertion process
     */
    public int findInsertionRow(Tuple targetRow, int clusteringKeyIndex, String clusteringKeyType)
            throws DBAppException {
        Object targetRowClusteringKey = targetRow.getFields()[clusteringKeyIndex];
        int start = 0;
        int end = this.tuples.size() - 1;
        int row = 0;

        while (start <= end) {
            int mid = start + (end - start) / 2;
            Object currRow = this.tuples.get(mid).getFields()[clusteringKeyIndex];

            int comparison = Table.compareClusteringKey(targetRowClusteringKey, currRow, clusteringKeyType);
            if (comparison < 0) {
                end = mid - 1;
            } else {
                row = mid;
                start = mid + 1;
            }
        }

        return row;
    }
}