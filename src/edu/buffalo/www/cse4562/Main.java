package edu.buffalo.www.cse4562;

import edu.buffalo.www.cse4562.RA.RATree;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;


import java.util.HashMap;

import static edu.buffalo.www.cse4562.SQLparser.CreatParser.CreatFunction;
import static edu.buffalo.www.cse4562.SQLparser.SelectParser.SelectFunction;


public class Main {
    public static void main(String[] args) throws Exception{
        System.out.println("Hello, World");
        CCJSqlParser parser = new CCJSqlParser(System.in);
        Statement stmt = null;
        HashMap<String,Object> ratree = new HashMap<>();
        try {
            ratree = parser(parser);
            //todo readfile
        }catch (Exception e){
            throw new Exception(e);
        }
    }

    public static HashMap<String,Object> parser(CCJSqlParser parser) throws Exception{
        Statement stmt = parser.Statement();
        HashMap<String,Object> afterParse = new HashMap<>();
        while(stmt!=null){
            if (stmt instanceof Select){
                Select select = (Select)stmt;
                SelectBody body = select.getSelectBody();
                afterParse = SelectFunction(body,0);
                stmt = null;
            }else if (stmt instanceof CreateTable){
                CreateTable create = (CreateTable)stmt;
//                afterParse = CreatFunction(create);
            }else{
                throw new Exception("Cannot handle the statement"+stmt);
            }
        }
        return afterParse;
    }

}
