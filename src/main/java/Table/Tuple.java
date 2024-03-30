package Table;

import java.io.Serializable;
import java.util.Arrays;

/**
 * The Tuple class represents a row in a database table.
 * It contains a set of fields, which represent the columns in the row.
 */
public class Tuple implements Serializable {
    // Fields in the tuple
    private Object[] fields;

    /**
     * Constructor for the Tuple class.
     * Initializes the fields with the provided values.
     * 
     * @param fields Values for the fields
     */
    public Tuple(Object... fields) {
        this.fields = fields;
    }

    /**
     * Returns the fields in the tuple.
     * 
     * @return Array of fields
     */
    public Object[] getFields() {
        return fields;
    }

    /**
     * Sets the fields in the tuple.
     * 
     * @param fields Array of fields
     */
    public void setFields(Object[] fields) {
        this.fields = fields;
    }

    /**
     * Returns a string representation of the tuple.
     * The string is a comma-separated list of the string representations of the
     * fields.
     * 
     * @return String representation of the tuple
     */
    @Override
    public String toString() {
        String res = Arrays.toString(fields);
        return res.replace(", ", ",")
                .replace("[", "")
                .replace("]", "");
    }
}