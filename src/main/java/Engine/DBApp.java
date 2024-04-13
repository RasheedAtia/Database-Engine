package Engine;

/** * @author Wael Abouelsaadat */
import java.util.Iterator;
import java.util.Properties;

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

		Table table = loadTable(strTableName);
		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(strTableName);

		int prevNumOfPages = table.pageNums.size();
		table.insertRow(htblColNameType, htblColNameValue);

		if (table.pageNums.size() != prevNumOfPages)
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
		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(strTableName);

		table.updateRow(htblColNameType, htblColNameValue, strClusteringKeyValue);
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

		Table t = loadTable(strTableName);
		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(strTableName);
		t.deleteRow(htblColNameValue, htblColNameType);
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException, IOException, ClassNotFoundException {

		Table t = loadTable(arrSQLTerms[0]._strTableName);
		Hashtable<String, String> htblColNameType = metadata.loadColumnTypes(arrSQLTerms[0]._strTableName);
		return t.selectFromTable(arrSQLTerms, strarrOperators, htblColNameType);
	}

	private Table loadTable(String tableName) throws IOException, ClassNotFoundException {
		String tableDirectory = "src\\main\\java\\Table\\" + tableName + "\\";
		return (Table) fileHandler.loadInstance(tableDirectory, tableName);
	}

	public static void main(String[] args) {

		try {
			String strTableName = "Student";
			DBApp dbApp = new DBApp();

			// Hashtable<String, String> htblColNameType = new Hashtable<>();
			// htblColNameType.put("id", "java.lang.Integer");
			// htblColNameType.put("name", "java.lang.String");

			// // CHANGED java.lang.double -> java.lang.Double

			// htblColNameType.put("gpa", "java.lang.Double");
			// dbApp.createTable(strTableName, "id", htblColNameType);
			// dbApp.createIndex(strTableName, "name", "gpaIndex");

			Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			// for (int i = 0; i < 500; i++) {
			// htblColNameValue.clear();
			// htblColNameValue.put("id", i);
			// if (i < 200) {
			// htblColNameValue.put("name", "b");
			// } else {
			// htblColNameValue.put("name", "a");
			// }
			// htblColNameValue.put("gpa", 0.5 + i);
			// dbApp.insertIntoTable(strTableName, htblColNameValue);
			// }
			// htblColNameValue.clear();
			// htblColNameValue.put("name", "a");

			// dbApp.deleteFromTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("name", "c");
			htblColNameValue.put("gpa", 1.0);

			dbApp.updateTable(strTableName, "110", htblColNameValue);

			Table testTable = dbApp.loadTable(strTableName);
			BPlusTreeIndex tree = testTable.loadIndex("name");
			// tree.tree.print();
			System.out.println(tree.tree.search("c"));
			// for (int i = 0; i < testTable.pageNums.size(); i++) {
			// // System.out.println(testTable.pageNums.get(i));
			// Page p = testTable.loadPage(testTable.pageNums.get(i));
			// System.out.println(p);
			// System.out.println();
			// }

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(453455));
			// htblColNameValue.put("name", new String("Ahmed Noor"));
			// htblColNameValue.put("gpa", new Double(0.95));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(5674567));
			// htblColNameValue.put("name", new String("Dalia Noor"));
			// htblColNameValue.put("gpa", new Double(1.5));
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
			// arrSQLTerms = new SQLTerm[6];
			// arrSQLTerms[0] = new SQLTerm(); // YOU MISSED THIS LINE
			// arrSQLTerms[0]._strTableName = "Student";
			// arrSQLTerms[0]._strColumnName = "id";
			// arrSQLTerms[0]._strOperator = "=";
			// arrSQLTerms[0]._objValue = new Integer(200);

			// arrSQLTerms[1] = new SQLTerm(); // YOU MISSED THIS LINE
			// arrSQLTerms[1]._strTableName = "Student";
			// arrSQLTerms[1]._strColumnName = "id";
			// arrSQLTerms[1]._strOperator = ">";
			// arrSQLTerms[1]._objValue = new Integer(3000);

			// arrSQLTerms[2] = new SQLTerm(); // YOU MISSED THIS LINE
			// arrSQLTerms[2]._strTableName = "Student";
			// arrSQLTerms[2]._strColumnName = "id";
			// arrSQLTerms[2]._strOperator = "=";
			// arrSQLTerms[2]._objValue = new Integer(300);

			// arrSQLTerms[3] = new SQLTerm(); // YOU MISSED THIS LINE
			// arrSQLTerms[3]._strTableName = "Student";
			// arrSQLTerms[3]._strColumnName = "id";
			// arrSQLTerms[3]._strOperator = "=";
			// arrSQLTerms[3]._objValue = new Integer(200);

			// arrSQLTerms[4] = new SQLTerm(); // YOU MISSED THIS LINE
			// arrSQLTerms[4]._strTableName = "Student";
			// arrSQLTerms[4]._strColumnName = "id";
			// arrSQLTerms[4]._strOperator = ">";
			// arrSQLTerms[4]._objValue = new Integer(6000);

			// arrSQLTerms[5] = new SQLTerm(); // YOU MISSED THIS LINE
			// arrSQLTerms[5]._strTableName = "Student";
			// arrSQLTerms[5]._strColumnName = "id";
			// arrSQLTerms[5]._strOperator = "=";
			// arrSQLTerms[5]._objValue = new Integer(300);

			// String[] strarrOperators = new String[5];
			// strarrOperators[0] = "AND";
			// strarrOperators[1] = "OR";
			// strarrOperators[2] = "AND";
			// strarrOperators[3] = "AND";
			// strarrOperators[4] = "AND";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			// Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
			// while (resultSet.hasNext()) {
			// System.out.println(resultSet.next());
			// }
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}
}