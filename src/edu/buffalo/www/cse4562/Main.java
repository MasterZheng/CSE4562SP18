package edu.buffalo.www.cse4562;


import edu.buffalo.www.cse4562.RA.RANode;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.TempTable;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;


import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import static edu.buffalo.www.cse4562.SQLparser.CreatParser.CreatFunction;
import static edu.buffalo.www.cse4562.SQLparser.SelectParser.SelectFunction;
import static edu.buffalo.www.cse4562.processData.processSelect.SelectData;

public class Main {
    static Logger logger = Logger.getLogger(Main.class.getName());

    static String prompt = "$> "; // expected prompt

    public static void main(String[] argsArray) throws Exception {
        // ready to read stdin, print out prompt
        System.out.println(prompt);
        System.out.flush();
        HashMap<String, TableObject> tableMap = new HashMap<>();

        // project here
        while (true) {
            Reader in = new InputStreamReader(System.in);
            CCJSqlParser parser = new CCJSqlParser(in);
            Statement stmt;
            if ((stmt = parser.Statement()) != null){
                in = new InputStreamReader(System.in);
                parser = new CCJSqlParser(in);
                process(stmt, tableMap);

                System.out.println();// print results line by line);
                // read for next query
                System.out.println(prompt);
                System.out.flush();
            }

        }

        //todo readfile
    }


    public static void process(Statement stmt, HashMap<String, TableObject> tableMap) throws Exception {
        //HashMap<String, Object> parsedSQL = new HashMap<>();
        while (stmt != null) {
            if (stmt instanceof Select) {
                Select select = (Select) stmt;
                SelectBody body = select.getSelectBody();
                //parsedSQL = SelectFunction(body, 0);
                RANode raTree = SelectFunction(body);
                TempTable tempTable = SelectData(raTree, tableMap);
                stmt=null;
                Iterator<Tuple> iterator = tempTable.getIterator();
                while (iterator.hasNext()){
                    iterator.next().print();
                }
            } else if (stmt instanceof CreateTable) {
                boolean flag = CreatFunction((CreateTable) stmt, tableMap);
                stmt=null;
                if (flag) {
                    logger.info("Create table successfully");
                } else {
                    logger.warning("Failed to create a table ");
                }
            } else {
                stmt=null;
                throw new Exception("Cannot handle the statement" + stmt);
            }
        }
    }

}
