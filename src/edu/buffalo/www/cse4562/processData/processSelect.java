package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
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

    private static CSVFormat formator = CSVFormat.DEFAULT.withDelimiter('|');

    public static TableObject SelectData(RANode raTree, HashMap<String, TableObject> tableMap, String tableName) throws Exception {
        //tableName 子查询结果表的 alisa
        logger.info("processSelect");
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
        List<Object> selectItems;

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
                        parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                        if (right.getExpression() == null) {
                            rightIterator = parserRight.iterator();
                        } else {
                            tableRight.settupleList(SelectAndJoin(parserRight.iterator(), null, tableRight, null, right));;
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
                if (((RAJoin) pointer).getJoin() != null && pointer.getExpression() != null) {
                    //the join has 2 children,skip 1=1 in join.expression
                    result.settupleList(SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer));;
                    leftIterator = result.getIterator();
                    rightIterator = null;
                }
            } else if (operation.equals("SELECTION") && pointer.getExpression() != null) {
                    //当 where 不为 null 且不为1=1时，执行 selection
                    List<Tuple> queryResult = SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer);
                    if (queryResult != null) {
                        result.settupleList(queryResult);
                    }

            } else if (operation.equals("PROJECTION")) {
                //before process projection, check
                //if no where ,add all tuple into the queryResult List
                selectItems = ((RAProjection) pointer).getSelectItem();
                tempColDef(result, selectItems, involvedTables);
                result = ((RAProjection) pointer).Eval(result, tableName);

            } else if (operation.equals("ORDERBY")) {
                result = ((RAOrderby) pointer).Eval(result);
                //TODO
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
        logger.info("SubSelect result size:" + Result.getTupleList().size());

        return Result;
    }

    private static void tempColDef(TableObject tableObject, List selectItems, ArrayList<TableObject> involvedTables) {
        //todo create the temptable coldef
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        List<Column> columnInfo = new ArrayList<>();
        if (selectItems.get(0) instanceof AllColumns) {
            for (TableObject t : involvedTables) {
                columnDefinitions.addAll(t.getColumnDefinitions());
                columnInfo.addAll(t.getColumnInfo());
            }
        } else {
            for (Object s : selectItems) {
                if (s instanceof AllTableColumns) {
                    Table allColumnsTable = ((AllTableColumns) s).getTable();
                    for (int i = 0; i < involvedTables.size(); i++) {
                        if (involvedTables.get(i).getTableName().equals(allColumnsTable.getName())
                                || involvedTables.get(i).getAlisa().equals(allColumnsTable.getName())) {
                            List<Column> c = involvedTables.get(i).getColumnInfo();
                            for (int j = 0; j < c.size(); j++) {
                                c.get(j).setTable(allColumnsTable);
                            }
                            columnDefinitions.addAll(involvedTables.get(i).getColumnDefinitions());
                            columnInfo.addAll(c);
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
                                if (t.getTableName().equals(tableName) || t.getAlisa().equals(tableName)) {
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

    public static List<Tuple> SelectAndJoin(Iterator leftIterator, Iterator rightIterator, TableObject tableLeft, TableObject tableRight, RANode pointer) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        Tuple tupleLeft, tupleRight;
        while (leftIterator != null && leftIterator.hasNext()) {
            //get left tuple
            if (leftIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
                //CSV iterator
                tupleLeft = new Tuple(tableLeft, (CSVRecord) leftIterator.next());
            } else {
                //list iterator
                tupleLeft = (Tuple) leftIterator.next();
            }

            if (rightIterator != null) {
                while (rightIterator.hasNext()) {
                    if (rightIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
                        //CSV iterator
                        tupleRight = new Tuple(tableRight, (CSVRecord) rightIterator.next());
                    } else {
                        //list iterator
                        tupleRight = (Tuple) rightIterator.next();
                    }
                    evaluate eva = new evaluate(tupleLeft, tupleRight, pointer.getExpression());
                    queryResult = eva.Eval(queryResult);
                }
                if (!rightIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
                    rightIterator = tableRight.getIterator();
                } else {
                    CSVParser parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                    rightIterator = parserRight.iterator();
                }
            }
            if (rightIterator == null) {
                evaluate eva = new evaluate(tupleLeft, null, pointer.getExpression());
                queryResult = eva.Eval(queryResult);
            }
        }
        return queryResult;
    }

}
