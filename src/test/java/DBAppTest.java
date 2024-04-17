import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import Engine.DBApp;
import Engine.SQLTerm;
import Exceptions.DBAppException;
import Table.BPlusTreeIndex;
import Table.Page;
import Table.Table;

public class DBAppTest {
    static DBApp engine;
    static String strTableName;

    public static void createTableAndIndicies() throws ClassNotFoundException, DBAppException, IOException {
        Hashtable<String, String> htblColNameType = new Hashtable<>();

        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");

        engine.createTable(strTableName, "id", htblColNameType);

        engine.createIndex(strTableName, "name", "gpaIndex");
    }

    public static void insertRows() throws ClassNotFoundException, DBAppException, IOException {
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        for (int i = 0; i < 500; i++) {
            htblColNameValue.clear();
            htblColNameValue.put("id", i);
            if (i < 200) {
                htblColNameValue.put("name", "b");
            } else {
                htblColNameValue.put("name", "a");
            }
            htblColNameValue.put("gpa", 0.5 + i);
            engine.insertIntoTable(strTableName, htblColNameValue);
        }
    }

    public static void deleteRows() throws ClassNotFoundException, DBAppException, IOException {

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        // CONDITIONS
        htblColNameValue.put("name", "a");

        engine.deleteFromTable(strTableName, htblColNameValue);
    }

    public static void updateRow() throws ClassNotFoundException, DBAppException, IOException {

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        // NEW VALUES
        htblColNameValue.put("name", "c");
        htblColNameValue.put("gpa", 1.0);

        engine.updateTable(strTableName, "110", htblColNameValue);
    }

    public static void select() throws ClassNotFoundException, DBAppException, IOException {
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[6];
        arrSQLTerms[0] = new SQLTerm("Student", "id", "=", new Integer(200));
        arrSQLTerms[1] = new SQLTerm("Student", "id", ">", new Integer(3000));
        arrSQLTerms[2] = new SQLTerm("Student", "id", "=", new Integer(300));
        arrSQLTerms[3] = new SQLTerm("Student", "id", "=", new Integer(200));
        arrSQLTerms[4] = new SQLTerm("Student", "id", ">", new Integer(6000));
        arrSQLTerms[5] = new SQLTerm("Student", "id", "=", new Integer(300));

        String[] strarrOperators = new String[5];
        strarrOperators[0] = "AND";
        strarrOperators[1] = "OR";
        strarrOperators[2] = "AND";
        strarrOperators[3] = "AND";
        strarrOperators[4] = "AND";

        Iterator resultSet = engine.selectFromTable(arrSQLTerms, strarrOperators);
        while (resultSet.hasNext()) {
            System.out.println(resultSet.next());
        }
    }

    public static void printTable() throws ClassNotFoundException, IOException {
        Table testTable = engine.loadTable(strTableName);

        for (int i = 0; i < testTable.pageNums.size(); i++) {
            Page p = testTable.loadPage(testTable.pageNums.get(i));
            System.out.println(p);
            System.out.println();
        }
    }

    public static void printPageRanges() throws ClassNotFoundException, IOException {
        Table testTable = engine.loadTable(strTableName);

        for (Integer i : testTable.pageRanges.keySet()) {
            System.out.print(
                    i + "\t" + testTable.pageRanges.get(i)[0] + "\t" +
                            testTable.pageRanges.get(i)[1] + "\n");
        }
    }

    public static void printTree(String col) throws ClassNotFoundException, IOException {
        Table testTable = engine.loadTable(strTableName);
        BPlusTreeIndex tree = testTable.loadIndex(col);
        tree.tree.print();
        // System.out.println(tree.tree.search("c"));
    }

    public static void main(String[] args) {
        try {
            engine = new DBApp();
            strTableName = "Student";

            createTableAndIndicies();
            insertRows();
            deleteRows();
            // updateRow();
            // select();
            // printTable();
            // printPageRanges();
            printTree("name");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
