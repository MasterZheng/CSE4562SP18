package edu.buffalo.www.cse4562.SQLparser;

import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.HashMap;

public class CreatParser {
    //        public static HashMap<String,Object> CreatFunction(CreateTable create){
//            HashMap<String,Object> createMap = new HashMap<String,Object>();
//
//            //todo
//            List<ColumnDefinition> colDefList = create.getColumnDefinitions();
//            String tableName = create.getTable().getName();
//            String alias = create.getTable().getAlias();
//              String schemaName= create.getTable().getSchemaName();
//              List<Index> indexList = create.getIndexes();
//              List tblOptStr = create.getTableOptionsStrings();
//
//            createMap.put("TABLENAME",tableName);
//            if (colDefList!=null) {
//                for (int i = 0;i<colDefList.size();i++){
//                    createMap.put("COLNAME"+i,colDefList.get(i));
//                }
//            }
//            if (alias!=null){
//                createMap.put("ALIAS",alias);
//            }
//            return createMap;
//        }
    public static boolean CreatFunction(CreateTable create, HashMap<String,TableObject> tableMap) {
        String tableName = create.getTable().getName().toUpperCase();
        if (tableMap.containsKey(tableName)){
            return false;
        }
        Table table = create.getTable();
        TableObject tableObject = new TableObject(create,table,tableName);
        tableMap.put(tableName,tableObject);
        return true;
    }

}
