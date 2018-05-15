package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

public class processSelect {

    static Logger logger = Logger.getLogger(processSelect.class.getName());
    private static int BLOCKSIZE = 10000;
    private static int FLUSHSIZE = 1;
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
                        long startTime = System.currentTimeMillis();   //获取开始时间
                        tableLeft.settupleList(SelectAndJoin(parserLeft.iterator(), null, tableLeft, null, left.getExpression()));
                        long endTime = System.currentTimeMillis();   //获取开始时间
                        logger.info("left filter" + String.valueOf(endTime - startTime) + "ms");
                        leftIterator = tableLeft.getIterator();
                        tableLeft.setOriginal(false);
//                        tableLeft.getTupleList().clear();
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
                            tableRight.settupleList(SelectAndJoin(parserRight.iterator(), null, tableRight, null, right.getExpression()));
                            ;
                            rightIterator = tableRight.getIterator();
                            tableRight.setOriginal(false);
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
                        if ((tableLeft.getCurrentTuple() == null || tableLeft.getCurrentTuple().size() != 0) &&
                                (tableRight.getCurrentTuple() == null || tableRight.getCurrentTuple().size() != 0)) {
                            //= null :未过滤，size=0 过滤后无值
//                            if (tableLeft.getIndexFileName().size()!=0){
//                                tableLeft.getIndexTuple();
//                                tableLeft.getIndexFileName().clear();
//                            }
//                            if (tableRight.getIndexFileName().size()!=0){
//                                tableRight.getIndexTuple();
//                                tableRight.getIndexFileName().clear();
//                            }
//                            rightIterator = tableLeft.getIterator();
//                            leftIterator = tableLeft.getIterator();
                            result.settupleList(SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer.getExpression()));
                            //result.setIndexFileName(tableLeft.getIndexFileName());
                        } else {
                            result.settupleList(new ArrayList<>());
                        }
                        leftIterator = result.getIterator();
                        rightIterator = null;
                        tableRight = null;
                        tableLeft = result;
                        tableLeft.setOriginal(false);
                    }
                }
            } else if (operation.equals("SELECTION") && pointer.getExpression() != null) {
                List<Tuple> queryResult = SelectAndJoin(leftIterator, rightIterator, tableLeft, tableRight, pointer.getExpression());
                if (queryResult != null) {
                    result.settupleList(queryResult);
                }
//                if (tableLeft.getIndexFileName().size()!=0){
//                    tableLeft.getIndexTuple();
//                    tableLeft.getIndexFileName().clear();
//                }
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


    private static List<Tuple> hashJoin(Iterator leftIterator, Iterator rightIterator, TableObject tableLeft, TableObject tableRight, Expression exp) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
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
        Comparator c = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                // TODO Auto-generated method stub
                if (o1 < o2)
                    return -1;
                    //注意！！返回值必须是一对相反数，否则无效。jdk1.7以后就是这样。
                else return 1;
            }
        };

        if (tableLeft.isOriginal() && tableRight.isOriginal()) {
            // 左右都是原始表
//            HashMap<String, List<Integer>> leftCol = tableLeft.getIndex(colLeft.getColumnName());
//            HashMap<String, List<Integer>> rightCol = tableRight.getIndex(colRight.getColumnName());
//            for (String p : leftCol.keySet()) {
//                List<Integer> LeftList = leftCol.get(p);
//                List<Integer> rightList = rightCol.get(p);
//
//                if (tableLeft.getCurrentTuple() != null && tableLeft.getCurrentTuple().size() != 0) {
//                    //LeftList.retainAll(tableLeft.getCurrentTuple());
//                    Set leftSet = new HashSet();
//                    leftSet.addAll(LeftList);
//                    Set leftset = new HashSet();
//                    leftset.addAll(tableLeft.getCurrentTuple());
//                    leftSet.retainAll(leftset);
//                    LeftList.clear();
//                    LeftList.addAll(leftSet);
//                    LeftList.sort(c);
//                }
//                if (tableRight.getCurrentTuple() != null && tableRight.getCurrentTuple().size() != 0) {
//                    //rightList.retainAll(tableRight.getCurrentTuple());
//                    Set leftSet = new HashSet();
//                    leftSet.addAll(rightList);
//                    Set leftset = new HashSet();
//                    leftset.addAll(tableRight.getCurrentTuple());
//                    leftSet.retainAll(leftset);
//                    rightList.clear();
//                    rightList.addAll(leftSet);
//                    rightList.sort(c);
//                }
//
//                if (rightList != null && rightList.size() != 0 && LeftList != null && LeftList.size() != 0) {
//                    queryResult.addAll(indexHashJoin(null, LeftList, rightList, leftIterator, rightIterator, tableLeft, tableRight));
//                    CSVParser parserLeft = new CSVParser(new FileReader(tableLeft.getFileDir()), formator);
//                    CSVParser parserRight = new CSVParser(new FileReader(tableRight.getFileDir()), formator);
//                    leftIterator = parserLeft.iterator();
//                    rightIterator = parserRight.iterator();
//                }
//            }
//            tableLeft.setOriginal(false);
//            tableRight.setOriginal(false);
        } else if (!tableLeft.isOriginal() && tableRight.isOriginal()) {
            //左边是查询结果，右边是原始表
            HashMap<String, List<Integer>> leftCol = tableLeft.getIndex(colLeft.getColumnName());
            HashMap<String, List<Integer>> rightCol = tableRight.getIndex(colRight.getColumnName());
            ArrayList<String> valInleft = new ArrayList<>();
            for (Tuple t : tableLeft.getTupleList()) {
                valInleft.add(t.getAttributes().get(colLeft.getColumnName()).toRawString());
            }
            Set set = new HashSet();
            set.addAll(valInleft);
            valInleft.clear();
            valInleft.addAll(set);
            ArrayList<Integer> indexInright = new ArrayList<>();

            for (String val : valInleft) {
                List a = rightCol.get(val);
                if (a != null)
                    indexInright.addAll(a);
            }
            indexInright.sort(c);

            int counter = 1;
            int counterIndex = 0;
            while (rightIterator.hasNext()) {
                if (counterIndex < indexInright.size() && counter == indexInright.get(counterIndex)) {
                    Tuple t = getTuple(rightIterator, tableRight);
                    PrimitiveValue p = t.getAttributes().get(colRight.getColumnName());
                    if (leftCol.get(p.toRawString()) != null) {
                        for (Integer indexInfile : leftCol.get(p.toRawString())) {
                            if (tableLeft.getFile2Current().get(indexInfile) != null) {
                                int index = tableLeft.getFile2Current().get(indexInfile);
                                queryResult.add(t.joinTuple(tableLeft.getTupleList().get(index)));

                            }
                        }
                    }
//                    if (queryResult.size()>FLUSHSIZE){
//                        flush2disk(queryResult,tableLeft);
//                    }
                    counterIndex++;
                    counter++;
                } else if (counterIndex == indexInright.size()) {
                    break;
                } else {
                    rightIterator.next();
                    counter++;
                }
            }
//            flush2disk(queryResult,tableLeft);
            tableLeft.setFile2Current(null);
            tableRight.setOriginal(false);
        } else if (tableLeft.isOriginal() && !tableRight.isOriginal()) {
            //左边是原始表，右边是查询结果
            HashMap<String, List<Integer>> leftCol = tableLeft.getIndex(colLeft.getColumnName());
            HashMap<String, List<Integer>> rightCol = tableRight.getIndex(colRight.getColumnName());
            ArrayList<String> valInRight = new ArrayList<>();
            for (Tuple t : tableRight.getTupleList()) {
                String val = t.getAttributes().get(colRight.getColumnName()).toRawString();
                valInRight.add(val);
            }
            Set set = new HashSet();
            set.addAll(valInRight);
            valInRight.clear();
            valInRight.addAll(set);
            ArrayList<Integer> indexInLeft = new ArrayList<>();
            for (String val : valInRight) {
                indexInLeft.addAll(leftCol.get(val));
            }
            indexInLeft.sort(c);

            int counter = 1;
            int counterIndex = 0;
            while (leftIterator.hasNext()) {
                if (counterIndex < indexInLeft.size() && counter == indexInLeft.get(counterIndex)) {
                    Tuple t = getTuple(leftIterator, tableLeft);
                    PrimitiveValue p = t.getAttributes().get(colLeft.getColumnName());
                    if (rightCol.get(p.toRawString()) != null) {
                        for (Integer indexInfile : rightCol.get(p.toRawString())) {
                            if (tableRight.getFile2Current().get(indexInfile) != null) {
                                int index = tableRight.getFile2Current().get(indexInfile);
                                queryResult.add(t.joinTuple(tableRight.getTupleList().get(index)));
                            }
                        }
                    }
//                    if (queryResult.size()>FLUSHSIZE){
//                        flush2disk(queryResult,tableLeft);
//                    }
                    counterIndex++;
                    counter++;
                } else if (counterIndex == indexInLeft.size()) {
                    break;
                } else {
                    leftIterator.next();
                    counter++;
                }
            }
            //flush2disk(queryResult,tableLeft);
            tableRight.setFile2Current(null);
            tableRight.setOriginal(false);
        } else {
            // 左右都是查询结果
            //if the table  is not parsed
//            if (tableLeft.getTupleList() == null) {
//                List<Tuple> list = new ArrayList<>();
//                while (leftIterator.hasNext()) {
//                    list.add(new Tuple(tableLeft, (CSVRecord) leftIterator.next()));
//                }
//                tableLeft.settupleList(list);
//            }
//            if (tableRight.getTupleList() == null) {
//                List<Tuple> list = new ArrayList<>();
//                while (rightIterator.hasNext()) {
//                    list.add(new Tuple(tableRight, (CSVRecord) rightIterator.next()));
//                }
//                tableRight.settupleList(list);
//            }
            //将exp的左右列与join的左右表匹配

            HashMap<Integer, ArrayList<Integer>> rightjoinHash = new HashMap<>();
            for (int i = 0; i < tableRight.getTupleList().size(); i++) {
                String val = tableRight.getTupleList().get(i).getAttributes().get(colRight.getColumnName()).toRawString();
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
                int key = tleft.getAttributes().get(colLeft.getColumnName()).toRawString().hashCode();
                List<Integer> rightCols = rightjoinHash.get(key);
                if (rightCols != null && rightCols.size() > 0) {
                    for (int j = 0; j < rightCols.size(); j++) {
                        evaluate eva = new evaluate(tleft, tableRight.getTupleList().get(rightCols.get(j)), exp);
                        queryResult = eva.Eval(queryResult);
                    }
                }
//                if (queryResult.size()>FLUSHSIZE)
//                    flush2disk(queryResult,tableLeft);
            }
//            flush2disk(queryResult,tableLeft);
        }
        return queryResult;
    }

    private static List<Tuple> indexHashJoin(Tuple tuple, List<Integer> leftCol, List<Integer> rightCol,
                                             Iterator leftIterator, Iterator rightIterator,
                                             TableObject tableLeft, TableObject tableRight) {
        List<Tuple> result = new ArrayList<>();
        if (tuple != null) {
            if (leftCol == null && rightCol == null) {
                return result;
            }
            List<Integer> colList = leftCol != null ? leftCol : rightCol;
            if (colList.size() == 0)
                return result;
            Iterator iterator = leftIterator != null ? leftIterator : rightIterator;
            TableObject tableObject = tableLeft != null ? tableLeft : tableRight;
            Iterator<Integer> iteratorList = colList.iterator();
            int counter = 1;
            int index =  iteratorList.next();
            while (iterator.hasNext()) {
                if (counter == index) {
                    result.add(tuple.joinTuple(new Tuple(tableObject, (CSVRecord) iterator.next())));
                    if (iteratorList.hasNext())
                        index =  iteratorList.next();
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
//            if (leftCol.size() == 0 || rightCol.size() == 0) {
//                return result;
//            }
            Iterator<Integer> leftListIter = leftCol.iterator();
            Iterator<Integer> rightListIter = rightCol.iterator();
            int leftIndex =  leftListIter.next();
            int rightIndex =  rightListIter.next();
            List<Tuple> leftList = new ArrayList<>();
            List<Tuple> rightList = new ArrayList<>();
            while (leftIterator.hasNext()) {
                if (leftCounter == leftIndex) {
                    leftList.add(new Tuple(tableLeft, (CSVRecord) leftIterator.next()));
                    if (leftListIter.hasNext())
                        leftIndex =  leftListIter.next();
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
                        rightIndex =  rightListIter.next();
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
                                             TableObject tableLeft, TableObject tableRight, Expression exp) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        List<Tuple> leftBlock, rightBlock;
        if (tableRight == null) {
            //if no right table ,just evaluate left tuple 右表为空
            if (tableLeft.isOriginal() || (exp instanceof EqualsTo || exp instanceof MinorThan || exp instanceof GreaterThan)) {
                List<Integer> tupleIndex = getIndexList(tableLeft, exp, true);
                tableLeft.setCurrentTuple(tupleIndex);
                queryResult = getTupleByIndex(tableLeft, tupleIndex, leftIterator);
                //queryResult = getTupleByIndexBuffer(tableLeft, tupleIndex, leftIterator);
                //tableLeft.setOriginal(false);
            }

        } else if (exp instanceof BinaryExpression &&
                ((BinaryExpression) exp).getLeftExpression() instanceof Column &&
                ((BinaryExpression) exp).getRightExpression() instanceof Column) {
            //右表存在，且左右join
            // A.C=B.C
            queryResult = hashJoin(leftIterator, rightIterator, tableLeft, tableRight, exp);
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
            t = new Tuple(tableObject, (CSVRecord) iterator.next());

        } else {
            t = (Tuple) iterator.next();
        }
        return t;
    }

    private static List<Tuple> getTupleByIndex1(TableObject tableObject, List<Integer> tupleIndex, Iterator CSViterator) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        if (tupleIndex.size() != 0) {
            Iterator<Integer> iterator = tupleIndex.iterator();
            int counter = 1;
            int index =  iterator.next();
            while (CSViterator.hasNext()) {
                if (counter == index) {
                    queryResult.add(new Tuple(tableObject, (CSVRecord) CSViterator.next()));
                    if (iterator.hasNext())
                        index =  iterator.next();
                    else
                        break;
                } else {
                    CSViterator.next();
                }
                counter++;
            }

        }
        return queryResult;
    }
    private static List<Tuple> getTupleByIndex(TableObject tableObject, List<Integer> tupleIndex, Iterator CSViterator) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        if (tupleIndex.size() != 0) {
            int counter = 1;
            int counterIndex = 0;
            int size = tupleIndex.size();
            int index =  tupleIndex.get(0);
            while (CSViterator.hasNext()) {
                if (counter != index) {
                    CSViterator.next();
                } else {
                    queryResult.add(new Tuple(tableObject, (CSVRecord) CSViterator.next()));
                    counterIndex = counterIndex + 1;
                    if (counterIndex<size) {
                        index =  tupleIndex.get(counterIndex);
                    }else
                        break;
                }
                counter=counter+1;
            }

        }
        return queryResult;
    }

    private static List<Tuple> getTupleByIndexBuffer(TableObject tableObject, List<Integer> tupleIndex, Iterator CSViterator) throws Exception {
        List<Tuple> queryResult = new ArrayList<>();
        if (tupleIndex.size() != 0) {
            Iterator<Integer> iterator = tupleIndex.iterator();
            int counter = 1;
            int index =  iterator.next();
            FileInputStream inputStream = new FileInputStream(new File("indexes/" + tableObject.getTableName() + ".txt"));
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            List<ColumnDefinition> col = tableObject.getColumnDefinitions();
            while (inputStream.available() > 0) {
                if (counter == index) {
                    queryResult.add(((Tuple) objectInputStream.readObject()).Map(col));
                    //Tuple t = ((Tuple) objectInputStream.readObject()).Map(col);
                    if (iterator.hasNext())
                        index =  iterator.next();
                    else
                        break;
                }else
                    objectInputStream.readObject();
                counter++;
            }
        }
        return queryResult;

    }

    private static List<Integer> getIndexList(TableObject tableObject, Expression exp, boolean flag) throws Exception {
        List<Integer> tupleIndex = new ArrayList<>();
        boolean isSorted = true;
        Comparator c = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                // TODO Auto-generated method stub
                if (o1 < o2)
                    return -1;
                    //注意！！返回值必须是一对相反数，否则无效。jdk1.7以后就是这样。
                else return 1;
            }
        };
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
            //tupleIndex = tableObject.getIndex().get(colName).get(colVal.toRawString());
            tupleIndex = tableObject.getIndex(colName).get(colVal.toRawString());
        } else if (exp instanceof MinorThan || exp instanceof GreaterThan || exp instanceof GreaterThanEquals || exp instanceof MinorThanEquals) {
            String operator = "";
            String colName = "";
            PrimitiveValue colVal = null;
            if (exp instanceof MinorThan) {
                if (((MinorThan) exp).getLeftExpression() instanceof Column) {
                    //  colName<colVal
                    colName = ((Column) ((MinorThan) exp).getLeftExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((MinorThan) exp).getRightExpression());
                    operator = "colName<colVal";
                } else {
                    //  colVal<colName
                    colName = ((Column) ((MinorThan) exp).getRightExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((MinorThan) exp).getLeftExpression());
                    operator = "colVal<colName";
                }
            } else if (exp instanceof GreaterThan) {
                if (((GreaterThan) exp).getLeftExpression() instanceof Column) {
                    //  colVal<colName
                    colName = ((Column) ((GreaterThan) exp).getLeftExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((GreaterThan) exp).getRightExpression());
                    operator = "colVal<colName";
                } else {
                    //
                    colName = ((Column) ((GreaterThan) exp).getRightExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((GreaterThan) exp).getLeftExpression());
                    operator = "colName<colVal";
                }
            } else if (exp instanceof GreaterThanEquals) {
                if (((GreaterThanEquals) exp).getLeftExpression() instanceof Column) {
                    //  colVal>=colName
                    colName = ((Column) ((GreaterThanEquals) exp).getLeftExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((GreaterThanEquals) exp).getRightExpression());
                    operator = "colName>=colVal";
                } else {
                    //
                    colName = ((Column) ((GreaterThanEquals) exp).getRightExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((GreaterThanEquals) exp).getLeftExpression());
                    operator = "colVal>=colName";
                }
            } else {
                if (((MinorThanEquals) exp).getLeftExpression() instanceof Column) {
                    //  colVal>=colName
                    colName = ((Column) ((MinorThanEquals) exp).getLeftExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((MinorThanEquals) exp).getRightExpression());
                    operator = "colVal>=colName";
                } else {
                    //
                    colName = ((Column) ((MinorThanEquals) exp).getRightExpression()).getColumnName();
                    colVal = ((PrimitiveValue) ((MinorThanEquals) exp).getLeftExpression());
                    operator = "colName>=colVal";
                }
            }
            //HashMap<String, ArrayList<String>> map = tableObject.getIndex().get(colName);
            HashMap<String, List<Integer>> map = tableObject.getIndex(colName);

            switch (operator) {
                case "colName<colVal":
                    for (String key : map.keySet()) {
                        if (Double.valueOf(key) < colVal.toDouble())
                            tupleIndex.addAll(map.get(key));
                    }
                    break;
                case "colVal<colName":
                    for (String key : map.keySet()) {
                        if (colVal.toDouble() < Double.valueOf(key))
                            tupleIndex.addAll(map.get(key));
                    }
                    break;
                case "colName>=colVal":
                    for (String key : map.keySet()) {
                        if (Double.valueOf(key) >= colVal.toDouble())
                            tupleIndex.addAll(map.get(key));
                    }
                    break;
                case "colVal>=colName":
                    for (String key : map.keySet()) {
                        if (colVal.toDouble() >= Double.valueOf(key))
                            tupleIndex.addAll(map.get(key));
                    }
                    break;
            }
            isSorted = false;
        } else {
            //size == 0 : the tableobject is the results of a subselect
            if (tableObject.isOriginal()) {
                if (exp instanceof AndExpression) {
                    Expression leftExp = ((AndExpression) exp).getLeftExpression();
                    Expression rightExp = ((AndExpression) exp).getRightExpression();
                    List<Integer> leftIndex = getIndexList(tableObject, leftExp, false);
                    List<Integer> rightIndex = getIndexList(tableObject, rightExp, false);

                    Set right = new HashSet();
                    right.addAll(rightIndex);
                    Set left = new HashSet();
                    left.addAll(leftIndex);
                    right.retainAll(left);
                    tupleIndex.addAll(right);
                    isSorted = false;
                } else if (exp instanceof OrExpression) {
                    Expression leftExp = ((OrExpression) exp).getLeftExpression();
                    Expression rightExp = ((OrExpression) exp).getRightExpression();
                    List<Integer> leftIndex = getIndexList(tableObject, leftExp, false);
                    List<Integer> rightIndex = getIndexList(tableObject, rightExp, false);
                    leftIndex.addAll(rightIndex);
                    Set set = new HashSet();
                    set.addAll(leftIndex);
                    tupleIndex.addAll(set);
                    isSorted = false;
                }
            }
        }
        if (flag) {
            HashMap<Integer, Integer> file2current = new HashMap<>();
            for (int i = 0; i < tupleIndex.size(); i++) {
                file2current.put(tupleIndex.get(i), i);
            }
            tableObject.setFile2Current(file2current);
            if (!isSorted)
                tupleIndex.sort(c);
        }
        return tupleIndex;
    }

    private static void flush2disk(List<Tuple> queryResult, TableObject tableLeft) throws Exception {
        String fileName = "indexes/" + System.currentTimeMillis() + ".txt";
        File file = new File(fileName);
        tableLeft.setIndexFileName(fileName);
        file.createNewFile();

        FileOutputStream outputStream = new FileOutputStream(file, false);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(queryResult);
        objectOutputStream.close();
        queryResult.clear();

    }
}
