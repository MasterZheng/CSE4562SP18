package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class processSelect {

    static Logger logger = Logger.getLogger(processSelect.class.getName());

    public static TableObject SelectData(RANode raTree, HashMap<String, TableObject> tableMap) throws Exception {
        //TODO
        List<Tuple> queryResult = new ArrayList<>();
        RANode endPointer = raTree;
        TableObject result = new TableObject();
        RANode pointer = raTree;

        TableObject tableLeft = new TableObject();
        TableObject tableRight = new TableObject();
        ArrayList<TableObject> involvedTables = new ArrayList<>();

        CSVFormat formator = CSVFormat.DEFAULT.withDelimiter('|');
        CSVParser parserLeft;
        CSVParser parserRight;
        Iterator leftIterator = null;
        Iterator rightIterator = null;
        List<Object> selectItems = new ArrayList();
        while (pointer.hasNext()) {
            //find the first join
            pointer = pointer.getLeftNode();
            if (pointer.getOperation().equals("JOIN")) {
                RANode left = pointer.getLeftNode();
                if (left.getOperation().equals("TABLE")) {
                    //join left node is a table
                    tableLeft = tableMap.get(((RATable) left).getTable().getName().toUpperCase());
                    parserLeft = new CSVParser(new FileReader(tableLeft.getFileDir()), formator);
                    leftIterator = parserLeft.iterator();
                    involvedTables.add(tableLeft);
                } else {
                    // join left node is a subSelect tree
                    tableLeft = subSelect(left, tableMap, pointer);
                    leftIterator = tableLeft.getIterator();
                    involvedTables.add(tableLeft);
                    //finish subSelect, stop loop.
                    break;
                }
                if (pointer.getRightNode() != null) {
                    RANode right = pointer.getRightNode();
                    if (right.getOperation().equals("TABLE")) {
                        // join right node is a table
                        tableRight = tableMap.get(((RATable) right).getTable().getName().toUpperCase());
                        parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                        rightIterator = parserRight.iterator();
                        involvedTables.add(tableRight);

                    } else {
                        // join right node is a subSelect tree
                        tableRight = subSelect(right, tableMap, pointer);
                        rightIterator = tableRight.getIterator();
                        involvedTables.add(tableRight);
                        break;
                    }
                }
            }
        }

        pointer = pointer.getParentNode();

        while (pointer != null) {
            String operation = pointer.getOperation();
            if (operation.equals("JOIN")) {
                // no action
            } else if (operation.equals("SELECTION")) {
                Tuple tupleLeft;
                Tuple tupleRight;
                while (leftIterator != null && leftIterator.hasNext()) {
                    //get left tuple
                    if (leftIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")){
                        //CSV iterator
                        tupleLeft = new Tuple(tableLeft, (CSVRecord) leftIterator.next());
                    }else {
                        //list iterator
                        tupleLeft = (Tuple) leftIterator.next();
                    }

                    while (rightIterator != null && rightIterator.hasNext()){
                        if (rightIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")){
                            //CSV iterator
                            tupleRight = new Tuple(tableRight, (CSVRecord) rightIterator.next());
                        }else {
                            //list iterator
                            tupleRight = (Tuple) rightIterator.next();
                        }
                        queryResult = ((RASelection) pointer).Eval(queryResult, tupleLeft, tupleRight, involvedTables);
                    }
                    if (rightIterator == null){
                        queryResult = ((RASelection) pointer).Eval(queryResult, tupleLeft, null, involvedTables);
                    }
                }
                result.settupleList(queryResult);
            } else if (operation.equals("PROJECTION")) {
                //before process projection, check
                //if no where ,add all tuple into the queryResult List
                selectItems = ((RAProjection) pointer).getSelectItem();
                result.setColumnDefinitions(tempColDef(selectItems, involvedTables));
                result = ((RAProjection) pointer).Eval(result, involvedTables);

            } else if (operation.equals("ORDERBY")) {
                result = ((RAOrderby) pointer).Eval(result);
                //TODO
            } else if (operation.equals("DISTINCT")) {
                //todo
            } else if (operation.equals("LIMIT")) {
                result = ((RALimit) pointer).Eval(result);
            }
            if (pointer == endPointer) {
                break;
            }
            pointer = pointer.getParentNode();
        }

//        result.settupleList(queryResult);
//        result.setColumnDefinitions(columnDefinitions);
        return result;
    }


    public static TableObject subSelect(RANode raTree, HashMap<String, TableObject> tableMap, RANode pointer) throws Exception {
        TableObject Result = SelectData(raTree, tableMap);
        String name = ((RAJoin) pointer).getFromItem().getAlias();
        if (((RAJoin) pointer).getFromItem().getAlias() != null) {
            String alias = ((RAJoin) pointer).getFromItem().getAlias();
            Result.setTableName(alias);
        }
        Result.setTemp(true);
        tableMap.put(name, Result);
        for (int i = 0; i < Result.getTupleList().size(); i++) {
            Result.getTupleList().get(i).setTableName(name);
        }
        return Result;
    }

    public static List<ColumnDefinition> tempColDef(List selectItems, ArrayList<TableObject> involvedTables) {
        //todo create the temptable coldef
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        if (selectItems.get(0) instanceof AllColumns) {
            for (TableObject t : involvedTables)
                columnDefinitions.addAll(t.getColumnDefinitions());
        } else {
            for (Object s : selectItems) {
                Expression expression = ((SelectExpressionItem) s).getExpression();

                if (s.toString().contains("*")) {
                    //todo SELECT X.*
//                    if (column.getTable().getName().equals(tableLeft.getTable().getName())){
//                        columnDefinitions.addAll(tableLeft.getColumnDefinitions());
//                    }else if (column.getTable().getName().equals(tableRight.getTable().getName())){
//                        columnDefinitions.addAll(tableRight.getColumnDefinitions());
//                    }
                } else {
                    ColumnDefinition colDef = new ColumnDefinition();
                    //todo Select R.S
                    if (expression instanceof Column) {
                        Column column = (Column) expression;
                        String colName = column.getColumnName();
                        String tableName = column.getTable().getName();
                        if (tableName == null) {
                            //SELECT A FROM B,C
                            for (TableObject t : involvedTables) {
                                for (ColumnDefinition c : t.getColumnDefinitions()) {
                                    if (c.getColumnName().equals(colName)) {

                                        if (((SelectExpressionItem) s).getAlias() != null) {
                                            colDef.setColumnName(((SelectExpressionItem) s).getAlias());
                                        } else {
                                            colDef.setColumnName(colName);
                                        }
                                        colDef.setColDataType(c.getColDataType());
                                        break;
                                    }
                                }
                            }

                        } else {
                            //SELECT B.A FROM B,C
                            for (TableObject t : involvedTables) {
                                if (t.getTableName().equals(tableName)) {
                                    colDef.setColumnName(colName);
                                    for (int i = 0; i < t.getColumnDefinitions().size(); i++) {
                                        if (t.getColumnDefinitions().get(i).getColumnName().equals(colName)) {
                                            colDef.setColDataType(t.getColumnDefinitions().get(i).getColDataType());
                                            break;
                                        }
                                    }
                                    break;
                                }
                            }

                        }

//                        if (column.getTable().getName() != null) {
//                            colDef.setColumnName(colName);
//                            for (int i = 0; i < tableLeft.getColumnDefinitions().size(); i++) {
//                                if (tableLeft.getColumnDefinitions().get(i).getColumnName().equals(colName)) {
//                                    colDef.setColDataType(tableLeft.getColumnDefinitions().get(i).getColDataType());
//                                }
//                            }
//                        } else {
//                            for (ColumnDefinition c : tableLeft.getColumnDefinitions()) {
//                                if (colName.equals(c.getColumnName())) {
//                                    if (((SelectExpressionItem) s).getAlias() != null) {
//                                        colDef.setColumnName(((SelectExpressionItem) s).getAlias());
//                                    } else {
//                                        colDef.setColumnName(colName);
//                                    }
//                                    colDef.setColDataType(c.getColDataType());
//                                    break;
//                                }
//                            }
//                            //todo
////                            for (ColumnDefinition c:tableRight.getColumnDefinitions()){
////                                if (colName.equals(c.getColumnName())){
////                                    colDef.setColumnName(colName);
////                                    colDef.setColDataType(c.getColDataType());
////                                }
////                            }
//                        }
                    } else {
                        colDef.setColumnName(((SelectExpressionItem) s).getAlias());
                        ColDataType colDataType = new ColDataType();
                        colDataType.setDataType("LONG");
                        colDef.setColDataType(colDataType);
                    }

                    columnDefinitions.add(colDef);
                }
            }
        }
        return columnDefinitions;
    }


}
