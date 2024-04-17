package Engine;

import java.io.IOException;
import java.util.Hashtable;

import Exceptions.DBAppException;
import Table.Tuple;

public class Utils {
    /**
     * converts form of input for easier insertion.
     * 
     * @param htblColNameType  maps column name to its data type.
     * @param htblColNameValue maps column name to value of insertion.
     * @return tuple containing values to insert.
     */
    public static Tuple convertInputToTuple(Hashtable<String, String> htblColNameType,
            Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException {

        // fill tuple with values from input parameter htblColNameValue
        Tuple newTuple = new Tuple();

        Object[] newFields = new Object[htblColNameType.size()];
        int pos = 0;

        for (String col : htblColNameValue.keySet()) {
            newFields[pos++] = htblColNameValue.get(col);
        }

        newTuple.setFields(newFields);

        return newTuple;
    }

    public static Hashtable<String, Object> convertTupleToHashtable(Hashtable<String, String> htblColNameType,
            Tuple tuple)
            throws DBAppException, IOException, ClassNotFoundException {

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        for (String col : htblColNameType.keySet()) {
            int colIndex = getColIndex(htblColNameType, col);
            htblColNameValue.put(col, tuple.getFields()[colIndex]);
        }

        return htblColNameValue;
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
     * @throws ClassNotFoundException if the class for the existing column type
     *                                cannot be found
     * @throws DBAppException         if the type of the new value does not match
     *                                the existing column type
     */
    public static void checkColsTypeValidity(Hashtable<String, Object> htblColNameValue,
            Hashtable<String, String> htblColNameType)
            throws ClassNotFoundException, DBAppException {

        for (String col : htblColNameValue.keySet()) {
            if (htblColNameType.get(col) == null) {
                throw new DBAppException("Column " + col + " does not exist in table");
            }

            Object existingColValueType = Class.forName(htblColNameType.get(col));
            Object newColValueType = htblColNameValue.get(col).getClass();

            if (!existingColValueType.equals(newColValueType)) {
                throw new DBAppException("Invalid insert type for column " + col);
            }
        }
    }

    /**
     * @param targetKey         first clustering key in comparison.
     * @param currKey           second clustering key in comparison.
     * @param clusteringKeyType type of clustering key.
     * @return which key is larger (> 0 means targetKey is larger,
     *         < 0 means currKey is larger, = 0 means both keys are equal)
     */
    public static int compareKeys(String targetKey, String currKey, String clusteringKeyType) {
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
     * Returns the index of the clustering key in the given hashtable.
     *
     * @param htblColNameType a hashtable that maps column names to their data types
     * @return the index of the clustering key in the hashtable
     */
    public static int getColIndex(Hashtable<String, String> htblColNameType, String targetCol) {
        int colIndex = 0;
        for (String col : htblColNameType.keySet()) {
            if (col.equals(targetCol)) {
                break;
            }
            colIndex++;
        }

        return colIndex;
    }
}
