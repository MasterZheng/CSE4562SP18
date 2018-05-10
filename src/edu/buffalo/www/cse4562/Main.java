package edu.buffalo.www.cse4562;


import edu.buffalo.www.cse4562.RA.RANode;
import edu.buffalo.www.cse4562.SQLparser.SelectParser;
import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.logging.Logger;

import static edu.buffalo.www.cse4562.SQLparser.CreatParser.CreatFunction;
import static edu.buffalo.www.cse4562.processData.processSelect.SelectData;

public class Main {
    static Logger logger = Logger.getLogger(Main.class.getName());
    static int c = 1;

    static String prompt = "$> "; // expected prompt
    static HashMap<String, TableObject> tableMap = new HashMap<>();

    public static void main(String[] argsArray) throws Exception {
        // ready to read stdin, print out prompt
        System.out.println(prompt);
        System.out.flush();

        Reader in = new InputStreamReader(System.in);
        CCJSqlParser parser = new CCJSqlParser(in);
        Statement s;

        // project here
        int flag = 0;
        while ((s = parser.Statement()) != null) {
            process(s, tableMap);
            if (s instanceof CreateTable){
                flag++;
            }
            if (flag>7){
                System.out.println(prompt);
                System.out.flush();
            }
//            System.out.println(prompt);
//            System.out.flush();
        }
    }


    public static void process(Statement stmt, HashMap<String, TableObject> tableMap) throws Exception {
        try {
            //HashMap<String, Object> parsedSQL = new HashMap<>();
            while (stmt != null) {
                if (stmt instanceof Select) {
                    Select select = (Select) stmt;
                    SelectBody body = select.getSelectBody();
                    SelectParser parser = new SelectParser(body);
                    RANode raTree = parser.SelectFunction(body,tableMap);
                    TableObject queryResult = SelectData(raTree, tableMap,null);
                    if (queryResult != null) {
                        queryResult.print();
                    }
                    stmt = null;

                    //执行完清除临时表
                } else if (stmt instanceof CreateTable) {
                    boolean flag = CreatFunction((CreateTable) stmt, tableMap);

                    stmt = null;
                    if (flag) {
                        logger.info("Create table successfully");
                    } else {
                        logger.warning("Failed to create a table ");
                    }
                } else {
                    stmt = null;
                    throw new Exception("Cannot handle the statement" + stmt);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
