package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
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
                //todo find the first join, should find the lowest join and finish the subselect.
//                fromItem = ((RAJoin) pointer).getFromItem();
//                joins = ((RAJoin) pointer).getJoin();
                // left is a table, right is null or a table
                if (pointer.getLeftNode() instanceof RATable
                        && (pointer.getRightNode() == null || pointer.getRightNode() instanceof RATable)) {
                    tableLeft = tableMap.get(((RATable) pointer.getLeftNode()).getTable().getName().toUpperCase());
                    if (pointer.getRightNode() != null) {
                        tableRight = tableMap.get(((RATable) pointer.getRightNode()).getTable().getName().toUpperCase());
                    }
                    queryData(tableLeft);
                } else {
                    // left is a subSelect

                }
                pointer = (RANode) pointer.next();

            }

        }


        return result;
    }

    public static Tuple queryData(TableObject table) throws Exception {
        // todo merge the fuction into the upper one
        FileReader fileReaderLeft = new FileReader(table.getFileDir());
        CSVFormat formator = CSVFormat.DEFAULT.withDelimiter('|');
        CSVParser parser = new CSVParser(fileReaderLeft, formator);
        Iterator<CSVRecord> CSVInterator = parser.iterator();
        Tuple tuple = new Tuple();
        if (CSVInterator.hasNext()){
            tuple = new Tuple(table,CSVInterator.next());
        }
        parser.close();
        fileReaderLeft.close();
        return null;
    }


}
