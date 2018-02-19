package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.TempTable;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
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
                while (CSVInteratorLeft.hasNext()){
                    Tuple tupleLeft = new Tuple(tableLeft, CSVInteratorLeft.next());
                    //todo
                    Tuple tupleRight = null;

                    evaluate eva = new evaluate(tupleLeft, tupleRight, ((RASelection) pointer).getWhere());
                    if (eva.processEval()){
                        queryResult.add(tupleLeft);
                    }
                }
            } else if (operation.equals("PROJECTION")) {

            } else if (operation.equals("ORDERBY")) {
                //TODO
            } else if (operation.equals("DISTINCT")) {
                //todo
            } else if (operation.equals("LIMIT")) {
                //todo
            }
            pointer = pointer.getParentNode();
        }


        return result;
    }


}
