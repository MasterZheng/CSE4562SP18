package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
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
    private static int BLOCKSIZE = 2000;

    private static CSVFormat formator = CSVFormat.DEFAULT.withDelimiter('|');

    public static TableObject SelectData(RANode raTree, HashMap<String, TableObject> tableMap, String tableName) throws Exception {
        //tableName 子查询结果表的 alisa
        TableObject result = new TableObject();
        RANode pointer = raTree;
        RANode end = pointer;
        TableObject tableLeft = new TableObject();
        TableObject tableRight = new TableObject();
        ArrayList<TableObject> involvedTables = new ArrayList<>();

        CSVParser parserLeft;
        CSVParser parserRight;
        Iterator leftIterator = null;
        Iterator rightIterator = null;
        List<SelectExpressionItem> selectItems;
        List<Column> groupByReferences;

        while (pointer.hasNext()) {
            //find the first join
            pointer = pointer.getLeftNode();
            if (pointer.getOperation().equals("JOIN") && !pointer.getLeftNode().getOperation().equals("JOIN")) {
                break;
            }
        }


        while (pointer != null) {
            String operation = pointer.getOperation();
            if (operation.equals("JOIN")) {
                //get iterator of table related with the join
                RANode left = pointer.getLeftNode();
                if (left.getOperation().equals("TABLE")) {
                    //join left node is a table
                    tableLeft = new TableObject(tableMap.get(((RATable) left).getTable().getName().toUpperCase()), left);
                    tableLeft.setAlisa(((RATable) left).getTable().getAlias());
                    //optimize colDef and colInfo
                    tableLeft.MapRelation(((RATable) left).getUsedColInf());
                    parserLeft = new CSVParser(new FileReader(tableLeft.getFileDir()), formator);
                    if (left.getExpression() == null) {
                        leftIterator = parserLeft.iterator();
                    } else {
                        tableLeft.settupleList(SelectAndJoin(parserLeft.iterator(), null, tableLeft, null, left));
                        leftIterator = tableLeft.getIterator();
                    }
                    involvedTables.add(tableLeft);
                } else if (left.getOperation().equals("JOIN")) {
                    leftIterator = result.getIterator();
                } else {
                    // join left node is a subSelect tree
                    tableLeft = subSelect(left, tableMap, pointer);
                    result = tableLeft;
                    leftIterator = tableLeft.getIterator();
                    involvedTables.add(tableLeft);
                    //finish subSelect, stop loop.
                }
                if (pointer.getRightNode() != null) {
                    RANode right = pointer.getRightNode();
                    if (right.getOperation().equals("TABLE")) {
                        // join right node is a table
                        tableRight = new TableObject(tableMap.get(((RATable) right).getTable().getName().toUpperCase()), right);
                        tableRight.setAlisa(((RATable) right).getTable().getAlias());
                        //optimize colDef and colInfo
                        tableRight.MapRelation(((RATable) right).getUsedColInf());
                        parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                        if (right.getExpression() == null) {
                            rightIterator = parserRight.iterator();
                        } else {
                            tableRight.settupleList(SelectAndJoin(parserRight.iterator(), null, tableRight, null, right));
                            ;
                            rightIterator = tableRight.getIterator();
                        }
                        involvedTables.add(tableRight);
                    } else {
                        // join right node is a subSelect tree
                        tableRight = subSelect(right, tableMap, pointer);
                        rightIterator = tableRight.getIterator();
                        involvedTables.add(tableRight);
                    }
                } else {
                    rightIterator = null;
                }
//                if (((RAJoin) pointer).getJoin() != null && pointer.getExpression() != null) {
                if (pointer.getExpression() != null) {
                    //the join has 2 children,skip 1=1 in join.expression
                    if (pointer.getParentNode().getOperation().equals("SELECTION") && pointer.getExpression().toString().equals("1 = 1")) {

                    } else {
                        result.settupleList(SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer));
                        leftIterator = result.getIterator();
                        rightIterator = null;
                    }

                }
            } else if (operation.equals("SELECTION") && pointer.getExpression() != null) {
                List<Tuple> queryResult = SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer);
                if (queryResult != null) {
                    result.settupleList(queryResult);
                }

            } else if (operation.equals("GROUPBY")) {
                groupByReferences = ((RAGroupBy) pointer).getGroupByReferences();
                if (result.getTupleList() != null) {
                    result = ((RAGroupBy) pointer).Eval(result, groupByReferences);
                }
            } else if (operation.equals("PROJECTION")) {
                //before process projection, check
                //if no where ,add all tuple into the queryResult List
                selectItems = ((RAProjection) pointer).getSelectItem();
                tempColDef(result, selectItems, involvedTables);
                result = ((RAProjection) pointer).Eval(result, tableName);

            } else if (operation.equals("ORDERBY")) {
                result = ((RAOrderby) pointer).Eval(result);
            } else if (operation.equals("DISTINCT")) {
                //todo
            } else if (operation.equals("LIMIT")) {
                result = ((RALimit) pointer).Eval(result);
            }
            if (pointer == end) {
                break;
            }
            pointer = pointer.getParentNode();
        }
        result.setTableName(tableName);
        return result;
    }


    private static TableObject subSelect(RANode raTree, HashMap<String, TableObject> tableMap, RANode pointer) throws Exception {
        String name = ((RAJoin) pointer).getFromItem().getAlias();
        TableObject Result = SelectData(raTree, tableMap, name);
        if (((RAJoin) pointer).getFromItem().getAlias() != null) {
            String alias = ((RAJoin) pointer).getFromItem().getAlias();
            Result.setTableName(alias);
        }
        for (int i = 0; i < Result.getTupleList().size(); i++) {
            Result.getTupleList().get(i).setTableName(name);
        }
        Table table = new Table(name);
        Result.setTable(table);
        for (int i = 0; i < Result.getColumnInfo().size(); i++) {
            Result.getColumnInfo().get(i).setTable(table);
        }
        tableMap.put(name, Result);

        return Result;
    }

    private static void tempColDef(TableObject tableObject, List selectItems, ArrayList<TableObject> involvedTables) {
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        List<Column> columnInfo = new ArrayList<>();
        if (selectItems.get(0) instanceof AllColumns) {
            // select *
            for (TableObject t : involvedTables) {
                List<Column> c = t.getColumnInfo();
                for (int j = 0; j < c.size(); j++) {
                    Column col = new Column(new Table(t.getTable().getName()),c.get(j).getColumnName());
                    columnInfo.add(col);
                }
                columnDefinitions.addAll(t.getColumnDefinitions());
            }
        } else {
            for (Object s : selectItems) {
                if (s instanceof AllTableColumns) {
                    // select R.*
                    Table allColumnsTable = ((AllTableColumns) s).getTable();
                    for (int i = 0; i < involvedTables.size(); i++) {
                        if (involvedTables.get(i).getTableName().equals(allColumnsTable.getName())
                                || involvedTables.get(i).getAlisa().equals(allColumnsTable.getName())) {
                            List<Column> c = involvedTables.get(i).getColumnInfo();
                            for (int j = 0; j < c.size(); j++) {
                                Column col = new Column(new Table(allColumnsTable.getName()),c.get(j).getColumnName());
                                columnInfo.add(col);
                            }
                            columnDefinitions.addAll(involvedTables.get(i).getColumnDefinitions());
                            break;
                        }
                    }
                } else {
                    Expression expression = ((SelectExpressionItem) s).getExpression();
                    String columnAlisa = ((SelectExpressionItem) s).getAlias();
                    ColumnDefinition colDef = new ColumnDefinition();
                    Column colInfo = new Column();
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
                                        if (columnAlisa != null) {
                                            colDef.setColumnName(columnAlisa);
                                            colInfo.setColumnName(columnAlisa);
                                        } else {
                                            colDef.setColumnName(colName);
                                            colInfo.setColumnName(colName);
                                        }
                                        colInfo.setTable(new Table(t.getTable().getName()));

                                        colDef.setColDataType(c.getColDataType());
                                        break;
                                    }
                                }
                            }
                        } else {
                            //SELECT B.A FROM B,C
                            for (TableObject t : involvedTables) {
                                if (t.getTableName().equals(tableName) || tableName.equals(t.getAlisa())) {
                                    if (columnAlisa != null) {
                                        colDef.setColumnName(columnAlisa);
                                        colInfo.setColumnName(columnAlisa);
                                    } else {
                                        colDef.setColumnName(colName);
                                        colInfo.setColumnName(colName);
                                    }
                                    colInfo.setTable(new Table(tableName));

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
                    } else if (expression instanceof Function) {
                        if (((SelectExpressionItem) s).getAlias() != null) {
                            colDef.setColumnName(((SelectExpressionItem) s).getAlias());

                        } else {
                            colDef.setColumnName(s.toString());
                        }
                        List<Expression> paramList = ((Function) expression).getParameters().getExpressions();
                        if (paramList != null) {
                            if(paramList.get(0) instanceof Column){
                                Column paramCol = (Column) paramList.get(0);
                                if (paramCol.getTable() != null) {
                                    colInfo.setTable(paramCol.getTable());
                                } else {
                                    colInfo.setTable(null);
                                }
                            }else {
                                colInfo.setTable(null);
                            }
                        }
                        colInfo.setColumnName(colDef.getColumnName());
                        ColDataType colDataType = new ColDataType();
                        //todo type
                        colDataType.setDataType("LONG");
                        colDef.setColDataType(colDataType);

                    } else {
                        colDef.setColumnName(((SelectExpressionItem) s).getAlias());
                        colInfo.setTable(null);
                        colInfo.setColumnName(colDef.getColumnName());
                        ColDataType colDataType = new ColDataType();
                        colDataType.setDataType("LONG");
                        colDef.setColDataType(colDataType);
                    }

                    columnDefinitions.add(colDef);
                    columnInfo.add(colInfo);
                }
            }
        }
        tableObject.setColumnInfo(columnInfo);
        tableObject.setColumnDefinitions(columnDefinitions);
    }

//    private static List<Tuple> SelectAndJoin(Iterator leftIterator, Iterator rightIterator, TableObject tableLeft, TableObject tableRight, RANode pointer) throws Exception {
//        List<Tuple> queryResult = new ArrayList<>();
//        Tuple tupleLeft, tupleRight;
//        //original
//        while (leftIterator != null && leftIterator.hasNext())
//
//        {
//            //get left tuple
//            if (leftIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
//                //CSV iterator
//                tupleLeft = new Tuple(tableLeft, (CSVRecord) leftIterator.next());
//            } else {
//                //list iterator
//                tupleLeft = (Tuple) leftIterator.next();
//            }
//
//            if (rightIterator != null) {
//                while (rightIterator.hasNext()) {
//                    if (rightIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
//                        //CSV iterator
//                        tupleRight = new Tuple(tableRight, (CSVRecord) rightIterator.next());
//                    } else {
//                        //list iterator
//                        tupleRight = (Tuple) rightIterator.next();
//                    }
//                    evaluate eva = new evaluate(tupleLeft, tupleRight, pointer.getExpression());
//                    queryResult = eva.Eval(queryResult);
//                }
//                //when the loop finish,the iterator should be initiated.
//                if (!rightIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
//                    rightIterator = tableRight.getIterator();
//                } else {
//                    CSVParser parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
//                    rightIterator = parserRight.iterator();
//                }
//            }
//            if (rightIterator == null) {
//                evaluate eva = new evaluate(tupleLeft, null, pointer.getExpression());
//                queryResult = eva.Eval(queryResult);
//            }
//        }
//        return queryResult;
//    }

    private static List<Tuple> SelectAndJoin(Iterator leftIterator, Iterator rightIterator, TableObject tableLeft, TableObject tableRight, RANode pointer) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        List<Tuple> leftBlock, rightBlock;
        if (rightIterator == null) {
            //if no right table ,just evaluate left tuple
            while (leftIterator.hasNext()) {
                leftBlock = getTupleBlock(leftIterator, tableLeft);
                for (int i = 0; i < leftBlock.size(); i++) {
                    evaluate eva = new evaluate(leftBlock.get(i), null, pointer.getExpression());
                    queryResult = eva.Eval(queryResult);
                }
            }
        } else {
            while (leftIterator.hasNext()) {
                leftBlock = getTupleBlock(leftIterator, tableLeft);
                rightBlock = getTupleBlock(rightIterator,tableRight);
                for (int i = 0; i < leftBlock.size(); i++) {
                    for(int j = 0;j<rightBlock.size();j++){
                        evaluate eva = new evaluate(leftBlock.get(i), rightBlock.get(j), pointer.getExpression());
                        queryResult = eva.Eval(queryResult);
                    }
                }
            }
        }
        return queryResult;
    }

    private static List<Tuple> getTupleBlock(Iterator iterator, TableObject tableObject) {
        List<Tuple> tupleBlock = new ArrayList<>();
        int counter = 0;
        if (iterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
            while (iterator.hasNext() && counter < BLOCKSIZE) {
                tupleBlock.add(new Tuple(tableObject, (CSVRecord) iterator.next()));
                counter++;
            }
        } else {
            while (iterator.hasNext() && counter < BLOCKSIZE) {
                tupleBlock.add((Tuple) iterator.next());
                counter++;
            }
        }
        return tupleBlock;
    }
}
