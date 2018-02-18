package edu.buffalo.www.cse4562.processData;

import edu.buffalo.www.cse4562.RA.*;
import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class processSelect {

    static Logger logger = Logger.getLogger(processSelect.class.getName());

    public static void SelectData(RANode raTree, HashMap<String,TableObject> tableMap) throws IOException{
        //TODO
        String[] headers = {};

        RANode pointer = raTree;


        List<Join> joins;
        List<SelectItem> selectItem;
        Expression where;
        List orderby;
        Distinct dist;
        Limit lim;
        FromItem fromItem;

        while (pointer.hasNext()){
            String operation = pointer.getOperation();
            if (operation.equals("LIMIT")){
                lim = ((RALimit) pointer).getLimit();
                pointer.next();
            }
            if (operation.equals("DISTINCT")){
                dist = ((RADistinct) pointer).getDistinct();
                pointer.next();
            }
            if (operation.equals("SELECT")){
                where = ((RASelection)pointer).getWhere();
                pointer.next();
            }
            if (operation.equals("PROJECTION")){
                selectItem = ((RAProjection)pointer).getSelectItem();
                pointer.next();
            }

        }

        CSVFormat formator = CSVFormat.DEFAULT.withHeader(headers);
        FileReader fileReader=new FileReader(filePath);
        CSVParser parser=new CSVParser(fileReader,formator);
        Iterator<CSVRecord> CSVInterator= parser.iterator();
//      List<CSVRecord> records=parser.getRecords();
        System.out.println(CSVInterator.next());

        parser.close();
        fileReader.close();
    }


}
