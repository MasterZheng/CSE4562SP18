package edu.buffalo.www.cse4562.processData;


import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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
    private static int BLOCKSIZE = 30000;

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
            //自顶向下寻找位于最下层的join节点
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
                        //过滤不需要的数据根据 left.A=1类型expression
                        tableLeft.settupleList(SelectAndJoin(parserLeft.iterator(), null, tableLeft, null, left));
                        leftIterator = tableLeft.getIterator();
                        //过滤后，index清空
                        tableLeft.setIndex(new HashMap<>());
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
                            //过滤
                            tableRight.settupleList(SelectAndJoin(parserRight.iterator(), null, tableRight, null, right));
                            ;
                            rightIterator = tableRight.getIterator();
                            //过滤后 index作废
                            tableRight.setIndex(new HashMap<>());
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
                if (pointer.getExpression() != null) {
                    //the join has 2 children,skip 1=1 in join.expression
                    if (pointer.getParentNode().getOperation().equals("SELECTION") && pointer.getExpression().toString().equals("1 = 1")) {

                    } else {
                        //process the expression in joinNode
                        result.settupleList(SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer));
                        leftIterator = result.getIterator();
                        rightIterator = null;
                        tableRight = null;
                        tableLeft = result;
                    }
                }
            } else if (operation.equals("SELECTION") && pointer.getExpression() != null) {
                List<Tuple> queryResult = SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer);
                if (queryResult != null) {
                    result.settupleList(queryResult);
                }
                tableLeft = null;
                tableRight = null;
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
                    Column col = new Column(new Table(t.getTable().getName()), c.get(j).getColumnName());
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
                                Column col = new Column(new Table(allColumnsTable.getName()), c.get(j).getColumnName());
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
                            if (paramList.get(0) instanceof Column) {
                                Column paramCol = (Column) paramList.get(0);
                                if (paramCol.getTable() != null) {
                                    colInfo.setTable(paramCol.getTable());
                                } else {
                                    colInfo.setTable(null);
                                }
                            } else {
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


    private static List<Tuple> hashJoin(Iterator leftIterator, Iterator rightIterator, TableObject tableLeft, TableObject tableRight, RANode pointer) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        Expression exp = pointer.getExpression();
        Column left = (Column) ((EqualsTo) exp).getLeftExpression();
        Column right = (Column) ((EqualsTo) exp).getRightExpression();
        //todo
        Column colLeft = null;
        Column colRight = null;

        if (left.getTable().getName().equals(tableLeft.getTableName()) ||
                left.getTable().getName().equals(tableLeft.getAlisa()) ||
                right.getTable().getName().equals(tableRight.getTableName()) ||
                right.getTable().getName().equals(tableRight.getAlisa())) {
            colLeft = left;
            colRight = right;
        } else {
            colLeft = right;
            colRight = left;
        }
        if (tableLeft.getIndex().size() != 0 && tableRight.getIndex().size() != 0) {
            // 左右都是原始表
            HashMap<PrimitiveValue, ArrayList<Integer>> leftCol = tableLeft.getIndex().get(colLeft);
            HashMap<PrimitiveValue, ArrayList<Integer>> rightCol = tableRight.getIndex().get(colRight);
            for (PrimitiveValue p : leftCol.keySet()) {
                ArrayList<Integer> rightList = rightCol.get(p);
                if (rightList.size() != 0) {
                    queryResult.addAll(indexHashJoin(null, leftCol.get(p), rightList, leftIterator, rightIterator, tableLeft, tableRight));
                    CSVParser parserLeft = new CSVParser(new FileReader(tableLeft.getFileDir()), formator);
                    CSVParser parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                    leftIterator = parserLeft.iterator();
                    rightIterator = parserRight.iterator();
                }
            }
        } else if (tableLeft.getIndex().size() == 0 && tableRight.getIndex().size() != 0) {
            //左边是查询结果，右边是原始表
            HashMap<PrimitiveValue, ArrayList<Integer>> rightCol = tableRight.getIndex().get(colRight);
            while (leftIterator.hasNext()) {
                Tuple t = getTuple(leftIterator, tableLeft);
                PrimitiveValue p = t.getAttributes().get(colLeft);
                queryResult.addAll(indexHashJoin(t, null, rightCol.get(p), null, rightIterator, null, tableRight));
                CSVParser parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                rightIterator = parserRight.iterator();
            }
        } else if (tableLeft.getIndex().size() != 0 && tableRight.getIndex().size() == 0) {
            //左边是原始表，右边是查询结果
            HashMap<PrimitiveValue, ArrayList<Integer>> leftCol = tableLeft.getIndex().get(colLeft);
            while (rightIterator.hasNext()) {
                Tuple t = getTuple(rightIterator, tableRight);
                PrimitiveValue p = t.getAttributes().get(colRight);
                queryResult.addAll(indexHashJoin(t, leftCol.get(p), null, leftIterator, null, tableLeft, null));
                CSVParser parserLeft = new CSVParser(new FileReader(tableLeft.getFileDir()), formator);
                leftIterator = parserLeft.iterator();
            }
        } else {
            // 左右都是查询结果
            //if the table  is not parsed
            if (tableLeft.getTupleList() == null) {
                List<Tuple> list = new ArrayList<>();
                while (leftIterator.hasNext()) {
                    list.add(new Tuple(tableLeft, (CSVRecord) leftIterator.next()));
                }
                tableLeft.settupleList(list);
            }
            if (tableRight.getTupleList() == null) {
                List<Tuple> list = new ArrayList<>();
                while (rightIterator.hasNext()) {
                    list.add(new Tuple(tableRight, (CSVRecord) rightIterator.next()));
                }
                tableRight.settupleList(list);
            }
            //将exp的左右列与join的左右表匹配
            if (left.getTable() != null && right.getTable() != null) {
                List<String> nameLeft = new ArrayList<>();
                List<String> nameRight = new ArrayList<>();
                if (tableLeft.getTupleList() != null && tableLeft.getTupleList().size() != 0) {
                    nameLeft = tableLeft.getTupleList().get(0).getTableName();
                }
                if (tableRight.getTupleList() != null && tableRight.getTupleList().size() != 0) {
                    nameRight = tableRight.getTupleList().get(0).getTableName();
                }
                if (nameLeft.contains(left.getTable().getName())) {
                    if (tableLeft.getAlisa() != null) {
                        colLeft = new Column(new Table(tableLeft.getAlisa()), left.getColumnName());
                    } else {
                        colLeft = new Column(tableLeft.getTable(), left.getColumnName());
                    }
                } else if (nameLeft.contains(right.getTable().getName())) {
                    if (tableLeft.getAlisa() != null) {
                        colLeft = new Column(new Table(tableLeft.getAlisa()), right.getColumnName());
                    } else {
                        colLeft = new Column(tableLeft.getTable(), right.getColumnName());
                    }
                }
                if (nameRight.contains(left.getTable().getName())) {
                    if (tableRight.getAlisa() != null) {
                        colRight = new Column(new Table(tableRight.getAlisa()), left.getColumnName());
                    } else {
                        colRight = new Column(tableRight.getTable(), left.getColumnName());
                    }
                } else if (nameRight.contains(right.getTable().getName())) {
                    if (tableRight.getAlisa() != null) {
                        colRight = new Column(new Table(tableRight.getAlisa()), right.getColumnName());
                    } else {
                        colRight = new Column(tableRight.getTable(), right.getColumnName());
                    }
                }
            }

            HashMap<Integer, ArrayList<Integer>> rightjoinHash = new HashMap<>();
            for (int i = 0; i < tableRight.getTupleList().size(); i++) {
                String val = tableRight.getTupleList().get(i).getAttributes().get(colRight).toRawString();
                int hascode = val.hashCode();
                if (rightjoinHash.containsKey(hascode)) {
                    rightjoinHash.get(hascode).add(i);
                } else {
                    ArrayList<Integer> list = new ArrayList<>();
                    list.add(i);
                    rightjoinHash.put(hascode, list);
                }
            }

            for (int i = 0; i < tableLeft.getTupleList().size(); i++) {
                Tuple tleft = tableLeft.getTupleList().get(i);
                int key = tleft.getAttributes().get(colLeft).toRawString().hashCode();
                List<Integer> rightCols = rightjoinHash.get(key);
                if (rightCols != null && rightCols.size() > 0) {
                    for (int j = 0; j < rightCols.size(); j++) {
                        evaluate eva = new evaluate(tleft, tableRight.getTupleList().get(rightCols.get(j)), exp);
                        queryResult = eva.Eval(queryResult);
                    }
                }
            }
        }
        return queryResult;
    }

    private static List<Tuple> indexHashJoin(Tuple tuple, ArrayList<Integer> leftCol, ArrayList<Integer> rightCol,
                                             Iterator leftIterator, Iterator rightIterator,
                                             TableObject tableLeft, TableObject tableRight) {
        List<Tuple> result = new ArrayList<>();
        if (tuple != null) {
            ArrayList<Integer> colList = leftCol != null ? leftCol : rightCol;
            if (colList.size() == 0)
                return result;
            Iterator iterator = leftIterator != null ? leftIterator : rightIterator;
            TableObject tableObject = tableLeft != null ? tableLeft : tableRight;
            Iterator<Integer> iteratorList = colList.iterator();
            int counter = 1;
            int index = iteratorList.next();
            while (iterator.hasNext()) {
                if (counter == index) {
                    result.add(tuple.joinTuple(new Tuple(tableObject, (CSVRecord) iterator.next())));
                    if (iteratorList.hasNext())
                        index = iteratorList.next();
                    else
                        break;
                } else {
                    iterator.next();
                }
                counter++;
            }
        } else {
            int leftCounter = 1;
            int rightCounter = 1;
            if (leftCol.size() == 0 || rightCol.size() == 0) {
                return result;
            }
            Iterator<Integer> leftListIter = leftCol.iterator();
            Iterator<Integer> rightListIter = rightCol.iterator();
            int leftIndex = leftListIter.next();
            int rightIndex = rightListIter.next();
            List<Tuple> leftList = new ArrayList<>();
            List<Tuple> rightList = new ArrayList<>();
            while (leftIterator.hasNext()) {
                if (leftCounter == leftIndex) {
                    leftList.add(new Tuple(tableLeft, (CSVRecord) leftIterator.next()));
                    if (leftListIter.hasNext())
                        leftIndex = leftListIter.next();
                    else
                        break;
                } else {
                    leftIterator.next();
                }
                leftCounter++;
            }
            while (rightIterator.hasNext()) {
                if (rightCounter == rightIndex) {
                    rightList.add(new Tuple(tableRight, (CSVRecord) rightIterator.next()));
                    ;
                    if (rightListIter.hasNext())
                        rightIndex = rightListIter.next();
                    else
                        break;
                } else {
                    rightIterator.next();
                }
                rightCounter++;
            }
            for (int i = 0; i < leftList.size(); i++) {
                for (int j = 0; j < rightList.size(); j++) {
                    result.add(leftList.get(i).joinTuple(rightList.get(j)));
                }
            }
        }
        return result;
    }

    private static List<Tuple> SelectAndJoin(Iterator leftIterator, Iterator rightIterator,
                                             TableObject tableLeft, TableObject tableRight, RANode pointer) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        List<Tuple> leftBlock, rightBlock;
        Expression exp = pointer.getExpression();
        if (tableRight == null) {
            //if no right table ,just evaluate left tuple 右表为空
            if (exp instanceof EqualsTo) {
                //add more options for >,<,>=,<=
                String colName = "";
                PrimitiveValue colVal = null;
                if (((EqualsTo) exp).getRightExpression() instanceof Column) {
                    colName = ((Column) ((EqualsTo) exp).getRightExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((EqualsTo) exp).getLeftExpression());
                } else {
                    colName = ((Column) ((EqualsTo) exp).getLeftExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((EqualsTo) exp).getRightExpression());
                }
                boolean flag = tableLeft.getIndex().containsKey(new Column(tableLeft.getTable(), colName));
                if (flag) {
                    List<Integer> tupleIndex = tableLeft.getIndex().get(new Column(tableLeft.getTable(), colName)).get(colVal);
                    if (tupleIndex.size() != 0) {
                        Iterator<Integer> iterator = tupleIndex.iterator();
                        int counter = 1;
                        int index = iterator.next();
                        while (leftIterator.hasNext()) {
                            if (counter == index) {
                                queryResult.add(new Tuple(tableLeft, (CSVRecord) leftIterator.next()));
                                if (iterator.hasNext())
                                    index = iterator.next();
                                else
                                    break;
                            } else {
                                leftIterator.next();
                            }
                            counter++;
                        }
                    }
                } else {
                    while (leftIterator.hasNext()) {
                        leftBlock = getTupleBlock(leftIterator, tableLeft);
                        for (int i = 0; i < leftBlock.size(); i++) {
                            evaluate eva = new evaluate(leftBlock.get(i), null, exp);
                            queryResult = eva.Eval(queryResult);
                        }
                    }
                }
            } else{
                //size == 0 : the tableobject is the results of a subselect
                while (leftIterator.hasNext()) {
                    leftBlock = getTupleBlock(leftIterator, tableLeft);
                    for (int i = 0; i < leftBlock.size(); i++) {
                        evaluate eva = new evaluate(leftBlock.get(i), null, exp);
                        queryResult = eva.Eval(queryResult);
                    }
                }
            }

        } else if (exp instanceof BinaryExpression &&
                ((BinaryExpression) exp).getLeftExpression() instanceof Column &&
                ((BinaryExpression) exp).getRightExpression() instanceof Column) {
            //右表存在，且左右join
            // A.C=B.C
            queryResult = hashJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer);
        } else if (exp instanceof EqualsTo && exp.toString().equals("1 = 1")) {
            if (tableRight.getTupleList() != null && tableRight.getTupleList().size() != 0) {
                while (leftIterator.hasNext()) {
                    Tuple tleft = getTuple(leftIterator, tableLeft);
                    for (int i = 0; i < tableRight.getTupleList().size(); i++) {
                        queryResult.add(tleft.joinTuple(tableRight.getTupleList().get(i)));
                    }
                }
            } else {
                while (leftIterator.hasNext()) {
                    leftBlock = getTupleBlock(leftIterator, tableLeft);
                    while (rightIterator.hasNext()) {
                        rightBlock = getTupleBlock(rightIterator, tableRight);
                        for (int i = 0; i < leftBlock.size(); i++) {
                            for (int j = 0; j < rightBlock.size(); j++) {
                                queryResult.add(leftBlock.get(i).joinTuple(rightBlock.get(j)));
                            }
                        }
                    }
                    if (!rightIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
                        rightIterator = tableRight.getIterator();
                    } else {
                        CSVParser parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                        rightIterator = parserRight.iterator();
                    }
                }
            }
        } else {
            //process other kinds of exp like: >,<
            if (rightIterator != null) {
                while (leftIterator.hasNext()) {
                    leftBlock = getTupleBlock(leftIterator, tableLeft);
                    while (rightIterator.hasNext()) {
                        rightBlock = getTupleBlock(rightIterator, tableRight);
                        for (int i = 0; i < leftBlock.size(); i++) {
                            for (int j = 0; j < rightBlock.size(); j++) {
                                evaluate eva = new evaluate(leftBlock.get(i), rightBlock.get(j), exp);
                                queryResult = eva.Eval(queryResult);
                            }
                        }
                    }
                    if (!rightIterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
                        rightIterator = tableRight.getIterator();
                    } else {
                        CSVParser parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
                        rightIterator = parserRight.iterator();
                    }
                }
            } else {
                while (leftIterator.hasNext()) {
                    leftBlock = getTupleBlock(leftIterator, tableLeft);
                    for (int i = 0; i < leftBlock.size(); i++) {
                        evaluate eva = new evaluate(leftBlock.get(i), null, exp);
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

    private static Tuple getTuple(Iterator iterator, TableObject tableObject) {
        Tuple t = null;
        if (iterator.getClass().getName().equals("org.apache.commons.csv.CSVParser$1")) {
            if (iterator.hasNext()) {
                t = new Tuple(tableObject, (CSVRecord) iterator.next());
            }
        } else {
            if (iterator.hasNext()) {
                t = (Tuple) iterator.next();
            }
        }
        return t;
    }
}
