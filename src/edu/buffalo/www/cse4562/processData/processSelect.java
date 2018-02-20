package edu.buffalo.www.cse4562.processData;

import com.sun.tools.javac.util.Name;
import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.TempTable;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
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

    public static TempTable SelectData(RANode raTree, HashMap<String, TableObject> tableMap) throws Exception {
        //TODO
        List<Tuple> queryResult = new ArrayList<>();
        TempTable result = new TempTable();
        RANode pointer = raTree;

        TableObject tableLeft = new TableObject();
        TableObject tableRight = new TableObject();

        TempTable leftResult;
        TempTable rightResult;

        FileReader fileReaderLeft;
        CSVFormat formator = CSVFormat.DEFAULT.withDelimiter('|');
        CSVParser parserLeft;
        Iterator<CSVRecord> CSVInteratorLeft = new Iterator<CSVRecord>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public CSVRecord next() {
                return null;
            }
        };


        FileReader fileReaderRight;
        CSVParser parserRight;
        Iterator<CSVRecord> CSVInteratorRight = new Iterator<CSVRecord>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public CSVRecord next() {
                return null;
            }
        };

        List<SelectExpressionItem> selectItems = new ArrayList();
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();

        while (pointer.hasNext()) {
            //find the first join
            pointer = pointer.getLeftNode();
            if (pointer.getOperation().equals("JOIN")) {
                RANode left = pointer.getLeftNode();
                if (left.getOperation().equals("TABLE")) {
                    tableLeft = tableMap.get(((RATable) left).getTable().getName().toUpperCase());
                    fileReaderLeft = new FileReader(tableLeft.getFileDir());
                    parserLeft = new CSVParser(fileReaderLeft, formator);
                    CSVInteratorLeft = parserLeft.iterator();
                } else {
                    leftResult = SelectData(left, tableMap);
                    CSVInteratorLeft = leftResult.getIterator();
                }
                if (pointer.getRightNode() != null) {
                    RANode right = pointer.getRightNode();
                    if (right.getOperation().equals("TABLE")) {
                        fileReaderRight = new FileReader(tableRight.getFileDir());
                        parserRight = new CSVParser(fileReaderRight, formator);
                        CSVInteratorRight = parserRight.iterator();
                    } else {
                        rightResult = SelectData(right, tableMap);
                        CSVInteratorRight = rightResult.getIterator();
                    }
                }
            }

        }

        pointer = pointer.getParentNode();

        while (pointer != null) {
            String operation = pointer.getOperation();
            if (operation.equals("SELECTION")) {
                //todo if no selection no record will be picked!!
                while (CSVInteratorLeft.hasNext()){
                    Tuple tupleLeft = new Tuple(tableLeft, CSVInteratorLeft.next());
                    Tuple tupleRight = null;
                    evaluate eva = new evaluate(tupleLeft, tupleRight, ((RASelection) pointer).getWhere());
                    if (eva.whereEval()){
                        queryResult.add(tupleLeft);
                    }
                }
            } else if (operation.equals("PROJECTION")) {
                selectItems = ((RAProjection)pointer).getSelectItem();
                columnDefinitions = tempColDef(selectItems,tableLeft,tableRight);
                queryResult = projection(queryResult,selectItems,columnDefinitions);

            } else if (operation.equals("ORDERBY")) {
                //TODO
            } else if (operation.equals("DISTINCT")) {
                //todo
            } else if (operation.equals("LIMIT")) {
                //todo
            }
            pointer = pointer.getParentNode();
        }

        result.setTempTable(queryResult);
        result.setColumnDefinitions(columnDefinitions);
        return result;
    }

    public static List<Tuple> projection(List<Tuple> queryResult,List<SelectExpressionItem> selectItems,
                                         List<ColumnDefinition> list)throws Exception{
        List<Tuple> result = new ArrayList<>();

        for(int i = 0;i<queryResult.size();i++){
            evaluate eva = new evaluate(queryResult.get(i),selectItems);
            result.add(eva.selectEval(list));
        }
        return result;
    }
    public static List<ColumnDefinition> tempColDef(List<SelectExpressionItem> selectItems,TableObject tableLeft,
                                                    TableObject tableRight){
        //todo create the temptable coldef
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        if (selectItems.get(0).toString().equals("*")){
            columnDefinitions = tableLeft.getColumnDefinitions();
        }else{
            for (SelectExpressionItem s:selectItems){
                Expression expression = s.getExpression();

                if (s.toString().contains("*")){
                    //todo
//                    if (column.getTable().getName().equals(tableLeft.getTable().getName())){
//                        columnDefinitions.addAll(tableLeft.getColumnDefinitions());
//                    }else if (column.getTable().getName().equals(tableRight.getTable().getName())){
//                        columnDefinitions.addAll(tableRight.getColumnDefinitions());
//                    }
                }else {
                    ColumnDefinition colDef = new ColumnDefinition();
                    if (expression instanceof Column){
                        Column column = (Column)expression;
                        String colName = column.getColumnName();
                        Table table = column.getTable();
                        if (column.getTable().getName()!=null){
                                colDef.setColumnName(colName);
                                for (int i = 0;i<columnDefinitions.size();i++){
                                    if (columnDefinitions.get(i).getColumnName().equals(colName)){
                                        colDef.setColDataType(columnDefinitions.get(i).getColDataType());
                                    }
                                }
                        }else {
                            for (ColumnDefinition c:tableLeft.getColumnDefinitions()){
                                if (colName.equals(c.getColumnName())){
                                    colDef.setColumnName(colName);
                                    colDef.setColDataType(c.getColDataType());
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

                    }else {
                        colDef.setColumnName(s.getAlias());
                        //colDef.setColDataType();
                    }

                    columnDefinitions.add(colDef);
                }
            }
        }
        return columnDefinitions;
    }


}
