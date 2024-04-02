package Engine;

/** * @author Wael Abouelsaadat */
import java.util.Iterator;
import java.util.Properties;

import Exceptions.DBAppException;
import Table.Page;
import Table.Table;

import java.util.Hashtable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class DBApp {
	private Metadata metadata;

	/**
	 * Constructs a new DBApp.
	 * Loads max number of rows in page from DBApp.config.
	 * Initializes the tables hashtable and the metadata object.
	 */
	public DBApp() throws IOException {
		Properties prop = new Properties();
		prop.load(new FileInputStream(
				"src\\main\\java\\resources\\DBApp.config"));
		String maxRowsCountInPageStr = prop.getProperty("MaximumRowsCountinPage");
		Page.maximumRowsCountInPage = Integer.parseInt(maxRowsCountInPageStr);

		metadata = Metadata.getInstance();
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
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		// create a new table object
		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		t.saveTable();

		// save the table metadata
		metadata.saveTable(t);
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
			String strIndexName) throws DBAppException, IOException, ClassNotFoundException {

		// get table to insert index into
		Table t = loadTable(strTableName);

		metadata.saveIndex(strTableName, strColName, strIndexName);
	}

	/**
	 * Inserts a new tuple into the specified table with the given column-value
	 * pairs.
	 * 
	 * TODO: insert new row into bplustree for each column if applicable
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

		Table table = loadTable(strTableName);

		checkColTypesValidity(table.name, htblColNameValue);

		int prevNumOfPages = table.numOfPages;
		table.insertRow(htblColNameValue);

		if (table.numOfPages != prevNumOfPages)
			table.saveTable();
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

		Table table = loadTable(strTableName);
		table.updateRow(htblColNameValue, strClusteringKeyValue);
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

	private Table loadTable(String tableName) throws IOException, ClassNotFoundException {
		String tableDirectory = "src\\main\\java\\Table\\" + tableName + "\\" + tableName + ".class";
		FileInputStream fileIn = new FileInputStream(tableDirectory);
		ObjectInputStream obj = new ObjectInputStream(fileIn);
		Table t = (Table) obj.readObject();

		obj.close();
		fileIn.close();
		return t;
	}

	private void checkColTypesValidity(String tableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, DBAppException {
		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(tableName);

		for (String colName : htblColNameType.keySet()) {
			String inputColType = htblColNameValue.get(colName).getClass().getName().toLowerCase();
			String actualColType = htblColNameType.get(colName).toLowerCase();

			if (!inputColType.equals(actualColType)) {
				throw new DBAppException("invalid type for column " + colName);
			}
		}
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

			Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			for (int i = 0; i < 400; i++) {
				htblColNameValue.clear();
				htblColNameValue.put("id", i);
				htblColNameValue.put("name", "a");
				htblColNameValue.put("gpa", 0.0);
				dbApp.insertIntoTable(strTableName, htblColNameValue);
			}

			htblColNameValue.clear();
			htblColNameValue.put("name", "b");
			htblColNameValue.put("gpa", 1.0);

			dbApp.updateTable(strTableName, "401", htblColNameValue);

			Table testTable = dbApp.loadTable(strTableName);
			for (int i = 0; i < testTable.numOfPages; i++) {
				Page p = testTable.loadPage(i);
				System.out.println(p);
				System.out.println();
			}

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