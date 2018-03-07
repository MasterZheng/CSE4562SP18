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


        FileReader fileReaderLeft;
        FileReader fileReaderRight;
        CSVFormat formator = CSVFormat.DEFAULT.withDelimiter('|');
        CSVParser parserLeft;
        CSVParser parserRight;
        Iterator<CSVRecord> CSVInteratorLeft = null;
        Iterator<CSVRecord> CSVInteratorRight = null;
        Iterator<Tuple> tempIteratorLeft = null;
        Iterator<Tuple> tempIteratorRight = null;
        List<Object> selectItems = new ArrayList();
        List<OrderByElement> orderBy = new ArrayList<>();
        while (pointer.hasNext()) {
            //find the first join
            pointer = pointer.getLeftNode();
            if (pointer.getOperation().equals("JOIN")) {
                RANode left = pointer.getLeftNode();
                if (left.getOperation().equals("TABLE")) {
                    //join left node is a table
                    tableLeft = tableMap.get(((RATable) left).getTable().getName().toUpperCase());
                    fileReaderLeft = new FileReader(tableLeft.getFileDir());
                    parserLeft = new CSVParser(fileReaderLeft, formator);
                    CSVInteratorLeft = parserLeft.iterator();
                } else {
                    // join left node is a subSelect tree
                    tableLeft = subSelect(left, tableMap, pointer);
                    tempIteratorLeft = tableLeft.getIterator();
                    //finish subSelect, stop loop.
                    break;
                }
                if (pointer.getRightNode() != null) {
                    RANode right = pointer.getRightNode();
                    if (right.getOperation().equals("TABLE")) {
                        // join right node is a table
                        fileReaderRight = new FileReader(tableRight.getFileDir());
                        parserRight = new CSVParser(fileReaderRight, formator);
                        CSVInteratorRight = parserRight.iterator();
                    } else {
                        // join right node is a subSelect tree
                        tableRight = subSelect(right, tableMap, pointer);
                        tempIteratorRight = tableRight.getIterator();
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
                if (CSVInteratorLeft != null) {
                    while (CSVInteratorLeft.hasNext()) {
                        //CSVrecord iterator
                        tupleLeft = new Tuple(tableLeft, CSVInteratorLeft.next());
                        queryResult = ((RASelection) pointer).Eval(queryResult, tupleLeft, tableLeft, tableMap);
                    }
                    result.settupleList(queryResult);
                } else if (tempIteratorLeft != null) {
                    // tuple iterator
                    while (tempIteratorLeft.hasNext()) {
                        tupleLeft = tempIteratorLeft.next();
                        queryResult = ((RASelection) pointer).Eval(queryResult, tupleLeft, tableLeft, tableMap);
                        result.settupleList(queryResult);
                    }
                }
            } else if (operation.equals("PROJECTION")) {
                //before process projection, check
                //if no where ,add all tuple into the queryResult List
                selectItems = ((RAProjection) pointer).getSelectItem();
                //columnDefinitions = tempColDef(selectItems,tableLeft,tableRight);
                //queryResult = ((RAProjection) pointer).Eval(queryResult,columnDefinitions,tableMap);
                result.setColumnDefinitions(tempColDef(selectItems, tableLeft, tableRight));

                result = ((RAProjection) pointer).Eval(result, tableMap);

            } else if (operation.equals("ORDERBY")) {
                result = ((RAOrderby) pointer).Eval(result);
                //TODO
            } else if (operation.equals("DISTINCT")) {
                //todo
            } else if (operation.equals("LIMIT")) {
                //todo
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

    public static List<ColumnDefinition> tempColDef(List selectItems, TableObject tableLeft,
                                                    TableObject tableRight) {
        //todo create the temptable coldef
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        if (selectItems.get(0) instanceof AllColumns) {
            columnDefinitions.addAll(tableLeft.getColumnDefinitions());
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
                    if (expression instanceof Column) {
                        Column column = (Column) expression;
                        String colName = column.getColumnName();
                        if (column.getTable().getName() != null) {
                            colDef.setColumnName(colName);
                            for (int i = 0; i < tableLeft.getColumnDefinitions().size(); i++) {
                                if (tableLeft.getColumnDefinitions().get(i).getColumnName().equals(colName)) {
                                    colDef.setColDataType(tableLeft.getColumnDefinitions().get(i).getColDataType());
                                }
                            }
                        } else {
                            for (ColumnDefinition c : tableLeft.getColumnDefinitions()) {
                                if (colName.equals(c.getColumnName())) {
                                    if (((SelectExpressionItem) s).getAlias() != null) {
                                        colDef.setColumnName(((SelectExpressionItem) s).getAlias());
                                    } else {
                                        colDef.setColumnName(colName);
                                    }
                                    colDef.setColDataType(c.getColDataType());
                                    break;
                                }
                            }
                            //todo
//                            for (ColumnDefinition c:tableRight.getColumnDefinitions()){
//                                if (colName.equals(c.getColumnName())){
//                                    colDef.setColumnName(colName);
//                                    colDef.setColDataType(c.getColDataType());
//                                }
//                            }
                        }
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
