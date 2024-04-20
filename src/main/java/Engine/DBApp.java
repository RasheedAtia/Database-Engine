package Engine;

/** * @author Wael Abouelsaadat */
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import Exceptions.DBAppException;
import Table.Page;
import Table.BPlusTreeIndex;
import Table.FileHandler;
import Table.Table;

import java.util.Hashtable;
import java.io.FileInputStream;
import java.io.IOException;

public class DBApp {
	private Metadata metadata;
	private FileHandler fileHandler;

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
		fileHandler = new FileHandler();
	}

	/**
	 * This method does whatever initialization you would like
	 * or leave it empty if there is no code you want to
	 * execute at application startup.
	 * 
	 * @throws DBAppException
	 * @throws IOException
	 */
	public void init() throws IOException, DBAppException {

	}

	/**
	 * @param strTableName           The name of the table to be created
	 * @param strClusteringKeyColumn The name of the column that will be the primary
	 *                               key
	 * @param htblColNameType        A hashtable mapping column names to their types
	 * @throws DBAppException         If an error occurs during table creation
	 * @throws ClassNotFoundException
	 */
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException, ClassNotFoundException {

		Table table = loadTable(strTableName);
		if (table != null)
			throw new DBAppException("Table already exists");

		// create a new table object
		Table t = new Table(strTableName, strClusteringKeyColumn);

		// save the table metadata
		metadata.saveTable(t, htblColNameType);
	}

	/**
	 * Creates a B+tree index on a specified column of a table.
	 *
	 * @param strTableName the name of the table to create the index on
	 * @param strColName   the name of the column to create the index on
	 * @param strIndexName the name of the index to be created
	 * @throws DBAppException if an error occurs while creating the index
	 */
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException, IOException, ClassNotFoundException {

		Table table = loadTable(strTableName);
		if (table == null)
			throw new DBAppException("Table does not exist");

		BPlusTreeIndex tree = table.loadIndex(strColName);
		if (tree != null)
			throw new DBAppException("Index already exists");

		Hashtable<String, String> colTypes = metadata.loadColumnTypes(strTableName);

		for (String col : colTypes.keySet()) {
			if (col.equals(strColName)) {
				tree = new BPlusTreeIndex(strTableName, strColName, colTypes.get(col));
				metadata.saveIndex(strTableName, strColName, strIndexName);
				return;
			}
		}

		throw new DBAppException("invalid Column Name " + strColName);
	}

	/**
	 * Inserts a new tuple into the specified table with the given column-value
	 * pairs.
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

		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(strTableName);
		if (htblColNameType.size() != htblColNameValue.size())
			throw new DBAppException("Number of columns in table does not match number of columns in input");

		Utils.checkColsTypeValidity(htblColNameValue, htblColNameType);
		Table table = loadTable(strTableName);

		table.insertRow(htblColNameType, htblColNameValue);
		table.saveTable();
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(strTableName);
		Utils.checkColsTypeValidity(htblColNameValue, htblColNameType);
		Table table = loadTable(strTableName);

		table.updateRow(htblColNameType, htblColNameValue, strClusteringKeyValue);
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(strTableName);
		Utils.checkColsTypeValidity(htblColNameValue, htblColNameType);
		Table t = loadTable(strTableName);
		if (htblColNameValue.isEmpty()) {
			t.pageNums = new Vector<>();
		}

		t.deleteRow(htblColNameValue, htblColNameType);
		t.saveTable();
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException, IOException, ClassNotFoundException {

		Table t = loadTable(arrSQLTerms[0]._strTableName);
		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(arrSQLTerms[0]._strTableName);
		return t.selectFromTable(arrSQLTerms, strarrOperators, htblColNameType);
	}

	public Table loadTable(String tableName) throws IOException, ClassNotFoundException {
		String tableDirectory = "src\\main\\java\\Table\\" + tableName + "\\";
		return (Table) fileHandler.loadInstance(tableDirectory, tableName);
	}

	public static void main(String[] args) {

	}
}