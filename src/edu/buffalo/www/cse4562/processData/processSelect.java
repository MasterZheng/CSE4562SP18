package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import sun.tools.jconsole.Tab;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class processSelect {

    static Logger logger = Logger.getLogger(processSelect.class.getName());

    public static List<String> SelectData(RANode raTree, HashMap<String, TableObject> tableMap) throws Exception {
        //TODO
        String[] headers = {};
        List<String> result = new ArrayList<>();
        RANode pointer = raTree;


        List<Join> joins = null;
        List<SelectItem> selectItem = null;
        Expression where = null;
        List orderby = null;
        Distinct dist = null;
        Limit lim = null;
        FromItem fromItem = null;
        TableObject tableLeft;
        TableObject tableRight = null;

        while (pointer.hasNext()) {
            String operation = pointer.getOperation();
            if (operation.equals("PROJECTION")) {
                selectItem = ((RAProjection) pointer).getSelectItem();
                pointer = (RANode) pointer.next();
            } else if (operation.equals("LIMIT")) {
                lim = ((RALimit) pointer).getLimit();
                pointer = (RANode) pointer.next();
            } else if (operation.equals("DISTINCT")) {
                dist = ((RADistinct) pointer).getDistinct();
                pointer = (RANode) pointer.next();
            } else if (operation.equals("SELECTION")) {
                where = ((RASelection) pointer).getWhere();
                pointer = (RANode) pointer.next();
            } else if (operation.equals("JOIN")) {
                //todo
                fromItem = ((RAJoin) pointer).getFromItem();
                joins = ((RAJoin) pointer).getJoin();
                // left is a table, right is null or a table
                if (pointer.getLeftNode() instanceof RATable
                        && (pointer.getRightNode() == null || pointer.getRightNode() instanceof RATable)) {
                    tableLeft = tableMap.get(((RATable) pointer.getLeftNode()).getTable().getName().toUpperCase());
                    if (pointer.getRightNode() != null) {
                        tableRight = tableMap.get(((RATable) pointer.getRightNode()).getTable().getName().toUpperCase());
                    }
                    queryData(result, tableLeft, tableRight, where);
                } else {
                    // left is a subSelect

                }
                pointer = (RANode) pointer.next();

            }

        }


        return result;
    }

    public static List<String> queryData(List<String> result, TableObject tableLeft,
                                         TableObject tableRight, Expression where) throws Exception {
        FileReader fileReaderLeft = new FileReader(tableLeft.getFileDir());
        if (tableRight!=null){
            FileReader fileReaderRight = new FileReader(tableLeft.getFileDir());

        }
        CSVFormat formator = CSVFormat.DEFAULT.withHeader();
        CSVParser parser = new CSVParser(fileReaderLeft, formator);
        Iterator<CSVRecord> CSVInterator = parser.iterator();
        List<CSVRecord> records = parser.getRecords();
        System.out.println(CSVInterator.next());

        parser.close();
        fileReaderLeft.close();
        return result;
    }


}
