import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

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

        engine.createIndex(strTableName, "gpa", "gpaIndex");
        engine.createIndex(strTableName, "name", "nameIndex");
        // engine.createIndex(strTableName, "id", "idIndex");
    }

    public static void insertRows() throws ClassNotFoundException, DBAppException, IOException {
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        for (int i = 0; i < 500; i++) {
            htblColNameValue.clear();
            htblColNameValue.put("id", i);
            if (i >= 200 && i <= 399) {
                htblColNameValue.put("name", "a");
            } else {
                htblColNameValue.put("name", "b");
            }
            htblColNameValue.put("gpa", 0.5);
            engine.insertIntoTable(strTableName, htblColNameValue);
        }
    }

    public static void deleteRows() throws ClassNotFoundException, DBAppException, IOException {

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        // CONDITIONS
        // htblColNameValue.put("name", "a");

        engine.deleteFromTable(strTableName, htblColNameValue);
    }

    public static void updateRow() throws ClassNotFoundException, DBAppException, IOException {

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        // NEW VALUES
        htblColNameValue.put("name", "c");
        htblColNameValue.put("gpa", 1.0);

        engine.updateTable(strTableName, "305", htblColNameValue);
    }

    public static void select() throws ClassNotFoundException, DBAppException, IOException {
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[4];
        arrSQLTerms[0] = new SQLTerm("Student", "id", "=", new Integer(17));
        arrSQLTerms[1] = new SQLTerm("Student", "name", "!=", "a");
        arrSQLTerms[2] = new SQLTerm("Student", "id", ">", new Integer(15));
        arrSQLTerms[3] = new SQLTerm("Student", "id", "<=", new Integer(25));

        String[] strarrOperators = new String[3];
        strarrOperators[0] = "XOR";
        strarrOperators[1] = "AND";
        strarrOperators[2] = "AND";

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
        if (tree == null)
            return;

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
            printTable();
            printPageRanges();
            // printTree("id");
            // printTree("name");
            // printTree("gpa");
            // Table testTable = engine.loadTable(strTableName);
            // BPlusTreeIndex tree = testTable.loadIndex("gpa");
            // BPlusTreeIndex tree1 = testTable.loadIndex("id");
            // BPlusTreeIndex tree2 = testTable.loadIndex("name");

            // Vector<Vector<String>> res = tree.tree.searchLower(new Double(0.6),
            // "java.lang.double");
            // for (Vector<String> v : res) {
            // System.out.println(v);
            // }
            // System.out.println("sadf");
            // Vector<Vector<String>> res1 = tree1.tree.searchLower(new Integer(90),
            // "java.lang.integer");
            // for (Vector<String> v : res1) {
            // System.out.println(v);
            // }
            // System.out.println("asdf");
            // Vector<Vector<String>> res2 = tree2.tree.searchLower("b",
            // "java.lang.string");
            // for (Vector<String> v : res2) {
            // System.out.println(v);
            // }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
