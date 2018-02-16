package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;


import java.util.HashMap;

import static edu.buffalo.www.cse4562.SQLparser.CreatParser.CreatFunction;
import static edu.buffalo.www.cse4562.SQLparser.SelectParser.SelectFunction;
import static edu.buffalo.www.cse4562.processData.processSelect.SelectData;
import static edu.buffalo.www.cse4562.processData.processCreate.CreateTable;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World");
        CCJSqlParser parser = new CCJSqlParser(System.in);
        Statement stmt = null;
        processSQL(parser);

        //todo readfile

    }

    public static void processSQL(CCJSqlParser parser) throws Exception {
        Statement stmt = parser.Statement();
        HashMap<String, Object> parsedSQL = new HashMap<>();
        while (stmt != null) {
            if (stmt instanceof Select) {
                Select select = (Select) stmt;
                SelectBody body = select.getSelectBody();
                parsedSQL = SelectFunction(body, 0);
                SelectData(parsedSQL);
                stmt = null;
            } else if (stmt instanceof CreateTable) {
                CreateTable create = (CreateTable) stmt;
                parsedSQL = CreatFunction(create);
                CreateTable(parsedSQL);
                stmt = null;
            } else {
                throw new Exception("Cannot handle the statement" + stmt);
            }
        }
    }

}
