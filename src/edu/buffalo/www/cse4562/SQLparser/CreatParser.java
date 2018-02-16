package edu.buffalo.www.cse4562.SQLparser;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import java.util.HashMap;
import java.util.List;

public class CreatParser {
        public static HashMap<String,Object> CreatFunction(CreateTable create){
            HashMap<String,Object> createMap = new HashMap<String,Object>();
            //todo
            List<ColumnDefinition> colDefList = create.getColumnDefinitions();
            String tableName = create.getTable().getName();
            String alias = create.getTable().getAlias();
//            String schemaName= create.getTable().getSchemaName();
//            List<Index> indexList = create.getIndexes();
//            List tblOptStr = create.getTableOptionsStrings();

            createMap.put("TABLENAME",tableName);
            if (colDefList!=null) {
                for (int i = 0;i<colDefList.size();i++){
                    createMap.put("COLNAME"+i,colDefList.get(i));
                }
            }
            if (alias!=null){
                createMap.put("ALIAS",alias);
            }
            return createMap;
        }
}
