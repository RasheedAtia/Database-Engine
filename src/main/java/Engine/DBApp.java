package Engine;

/** * @author Wael Abouelsaadat */
import java.util.Iterator;

import Exceptions.DBAppException;
import Table.Table;
import Table.Tuple;

import java.util.Hashtable;
import java.io.IOException;

public class DBApp {
	private Hashtable<String, Table> tables;
	private Metadata metadata;

	/**
	 * Constructs a new DBApp.
	 * Initializes the tables hashtable and the metadata object.
	 */
	public DBApp() {
		tables = new Hashtable<String, Table>();

		try {
			metadata = new Metadata();
			System.out.println("Metadata file created successfully!");
		} catch (IOException e) {
			System.out.println("An error occurred while creating the Metadata file.");
			e.printStackTrace();
		}
	}

	/**
	 * This method does whatever initialization you would like
	 * or leave it empty if there is no code you want to
	 * execute at application startup.
	 */
	public void init() {

	}

	/**
	 * @param strTableName           The name of the table to be created
	 * @param strClusteringKeyColumn The name of the column that will be the primary
	 *                               key
	 * @param htblColNameType        A hashtable mapping column names to their types
	 * @throws DBAppException If an error occurs during table creation
	 */
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		// create a new table object
		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		tables.put(strTableName, t);

		// save the metadata
		try {
			metadata.saveTable(t);
			System.out.println("Table added to Metadata file successfully!");
		} catch (IOException e) {
			System.out.println("An error occurred while saving the table to Metadata file.");
		}
	}

	/**
	 * Creates a B+tree index on a specified column of a table.
	 * TODO: create new BPlusTree for this table column
	 *
	 * @param strTableName the name of the table to create the index on
	 * @param strColName   the name of the column to create the index on
	 * @param strIndexName the name of the index to be created
	 * @throws DBAppException if an error occurs while creating the index
	 */
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException {

		// get table to insert index into
		Table t = tables.get(strTableName);

		// get the column type
		String colType = t.getHtblColNameType().get(strColName);

		try {
			metadata.saveIndex(strTableName, strColName, strIndexName);
			System.out.println("Index added to Metadata file successfully!");
		} catch (IOException e) {
			System.out.println("An error occurred while saving the Index to Metadata file.");
			e.printStackTrace();
		}
	}

	/**
	 * Inserts a new tuple into the specified table with the given column-value
	 * pairs.
	 * 
	 * TODO: insert new row into bplustree for each column if applicable
	 * TODO: get insertion position and shift rows down to insert new row
	 * 
	 * @param strTableName     the name of the table to insert into
	 * @param htblColNameValue a Hashtable containing the column-value pairs for the
	 *                         new tuple
	 * @throws DBAppException         if there is an error in the database operation
	 * @throws ClassNotFoundException if the specified class is not found
	 * @throws IOException            if there is an error in the input/output
	 *                                operation
	 */
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

		// get table to insert into
		Table table = tables.get(strTableName);
		Tuple newTuple = new Tuple();

		Object[] newFields = new Object[table.getHtblColNameType().size()];
		int pos = 0;

		for (String col : table.getHtblColNameType().keySet()) {
			Object existingColValueType = Class.forName(table.getHtblColNameType().get(col));
			Object newColValueType = htblColNameValue.get(col).getClass();

			if (!existingColValueType.equals(newColValueType)) {
				throw new DBAppException("Invalid insert type for column " + col);
			}

			newFields[pos++] = htblColNameValue.get(col);
		}
		newTuple.setFields(newFields);

		if (table.isFull()) {
			table.addPage(newTuple);
		} else {
			table.getPages().get(table.getPages().size() - 1).addTuple(newTuple);
		}
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException {

		return null;
	}

	public static void main(String[] args) {

		try {
			String strTableName = "Student";
			DBApp dbApp = new DBApp();

			Hashtable<String, String> htblColNameType = new Hashtable<>();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");

			// CHANGED java.lang.double -> java.lang.Double

			htblColNameType.put("gpa", "java.lang.Double");
			dbApp.createTable(strTableName, "id", htblColNameType);
			dbApp.createIndex(strTableName, "gpa", "gpaIndex");

			for (int i = 0; i <= 200; i++) {
				Hashtable<String, Object> htblColNameValue = new Hashtable<>();
				htblColNameValue.put("id", new Integer(2343432));
				htblColNameValue.put("name", new String("Ahmed Noor"));
				htblColNameValue.put("gpa", new Double(0.95));
				dbApp.insertIntoTable(strTableName, htblColNameValue);
			}
			System.out.println(dbApp.tables.get("Student").getPages().get(0));
			System.out.println();
			System.out.println(dbApp.tables.get("Student").getPages().get(1));

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(453455));
			// htblColNameValue.put("name", new String("Ahmed Noor"));
			// htblColNameValue.put("gpa", new Double(0.95));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(5674567));
			// htblColNameValue.put("name", new String("Dalia Noor"));
			// htblColNameValue.put("gpa", new Double(1.25));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(23498));
			// htblColNameValue.put("name", new String("John Noor"));
			// htblColNameValue.put("gpa", new Double(1.5));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(78452));
			// htblColNameValue.put("name", new String("Zaky Noor"));
			// htblColNameValue.put("gpa", new Double(0.88));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// SQLTerm[] arrSQLTerms;
			// arrSQLTerms = new SQLTerm[2];
			// arrSQLTerms[0]._strTableName = "Student";
			// arrSQLTerms[0]._strColumnName = "name";
			// arrSQLTerms[0]._strOperator = "=";
			// arrSQLTerms[0]._objValue = "John Noor";

			// arrSQLTerms[1]._strTableName = "Student";
			// arrSQLTerms[1]._strColumnName = "gpa";
			// arrSQLTerms[1]._strOperator = "=";
			// arrSQLTerms[1]._objValue = new Double(1.5);

			// String[] strarrOperators = new String[1];
			// strarrOperators[0] = "OR";
			// // select * from Student where name = "John Noor" or gpa = 1.5;
			// Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}

}