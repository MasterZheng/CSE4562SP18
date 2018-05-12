package edu.buffalo.www.cse4562.Table;

import edu.buffalo.www.cse4562.RA.RANode;
import edu.buffalo.www.cse4562.RA.RATable;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

public class TableObject {
    private Table table;
    private String tableName;
    private String alisa;
    private String fileDir;
    private boolean original = true;
    private List<ColumnDefinition> columnDefinitions;// record the column type String,Long,Double...
    //when the table is a query result, it is necessary to record the table info about the column
    private List<Column> columnInfo = new ArrayList<>();//record the columns and their table information.

    //when the table is a query result, it is necessary to record the table info about the column


    private List<Tuple> tupleList;
    private HashMap<Integer, ArrayList<Tuple>> groupMap = new HashMap<>();
    private HashMap<Integer, ArrayList<Integer>> joinHash = null;
    private List<Integer> mapRelations = new ArrayList<>();
    private List<Column> primaryKey = new ArrayList<>();
    private List<Column> references = new ArrayList<>();
    //    private HashMap<Column, HashMap<PrimitiveValue, ArrayList<Integer>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
//    private HashMap<Column, HashMap<PrimitiveValue, Integer>> statistics = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,count>
//    private int size = 0;
    static Logger logger = Logger.getLogger(TableObject.class.getName());


    public TableObject(TableObject tableObject, RANode raTable) {

        this.table = ((RATable) raTable).getTable();
        this.tableName = ((RATable) raTable).getTable().getName();
        this.alisa = this.table.getAlias();
        this.columnDefinitions = tableObject.getColumnDefinitions();
        this.columnInfo = tableObject.getColumnInfo();
        this.fileDir = tableObject.getFileDir();
//        this.index = tableObject.getIndex();
//        this.statistics = tableObject.getStatistics();
    }

    public TableObject(TableObject tableObject, RANode raTable, String alisa) {
        this.table = ((RATable) raTable).getTable();
        this.tableName = ((RATable) raTable).getTable().getName();
        this.alisa = alisa;
        this.columnDefinitions = tableObject.getColumnDefinitions();
        this.columnInfo = tableObject.getColumnInfo();
    }

    public TableObject() {

    }

    public TableObject(CreateTable createTable, Table table, String tableName) {
        this.table = table;
        this.tableName = tableName;
        this.columnDefinitions = createTable.getColumnDefinitions();
        for (ColumnDefinition c : this.columnDefinitions) {
            Column col = new Column(table, c.getColumnName());
            columnInfo.add(col);
            if (c.getColumnSpecStrings() != null && c.getColumnSpecStrings().contains("PRIMARY")) {
                this.primaryKey.add(col);
            }
            if (c.getColumnSpecStrings() != null && c.getColumnSpecStrings().contains("REFERENCES")) {
                this.references.add(col);
            }
        }
        if (this.fileDir == null) {
            fileDir = "data/" + createTable.getTable().getName() + ".dat";
        }
    }


    public String getAlisa() {
        return alisa;
    }

    public void setAlisa(String alisa) {
        this.alisa = alisa;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFileDir() {
        return fileDir;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public boolean isOriginal() {
        return original;
    }

    public void setOriginal(boolean original) {
        this.original = original;
    }

    public List<Column> getColumnInfo() {
        return columnInfo;
    }

    public void setColumnInfo(List<Column> columnInfo) {
        this.columnInfo = columnInfo;
    }

    public void MapRelation(List<Column> usedColInfo) {
        //get the map relations between columninfo and usedColumnInfo
        for (int i = 0; i < usedColInfo.size(); i++)
            usedColInfo.get(i).setTable(this.table);
        List<ColumnDefinition> newColDef = new ArrayList<>();
        List<Column> newColInfo = new ArrayList<>();
        for (int i = 0; i < columnInfo.size(); i++) {
            if (usedColInfo.contains(columnInfo.get(i))) {
                newColDef.add(columnDefinitions.get(i));
                newColInfo.add(columnInfo.get(i));
                mapRelations.add(i);
            }
        }
        this.columnInfo = newColInfo;
        this.columnDefinitions = newColDef;
    }

    public List<Integer> getMapRelations() {
        return mapRelations;
    }

    public List<Tuple> getTupleList() {
        return tupleList;
    }

    public void settupleList(List<Tuple> tupleList) {
        this.tupleList = tupleList;
    }

    public void addTupleList(List<Tuple> tupleList) {
        this.tupleList.addAll(tupleList);
    }

    public Iterator getIterator() {
        return this.tupleList.iterator();
    }

    public HashMap<Integer, ArrayList<Tuple>> getgroupMap() {
        return this.groupMap;
    }

    public void setHashMap(HashMap<Integer, ArrayList<Tuple>> hashMap) {
        this.groupMap = hashMap;
    }

    public HashMap<Integer, ArrayList<Integer>> getJoinHash() {
        return joinHash;
    }

    public void setJoinHash(HashMap<Integer, ArrayList<Integer>> joinHash) {
        this.joinHash = joinHash;
    }

    public void indexAndStatistic() throws Exception {
        HashMap<String, HashMap<String, String>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>

        for (int i = 0; i < columnInfo.size(); i++) {
            index.put(columnInfo.get(i).getColumnName(), new HashMap());
        }
        CSVParser parser = new CSVParser(new FileReader(fileDir), CSVFormat.DEFAULT.withDelimiter('|'));
        Iterator<CSVRecord> Iterator = parser.iterator();
        int i = 1;
        if (index.size() != 0) {
            while (Iterator.hasNext()) {
                CSVRecord tuple = Iterator.next();
                for (int j = 0;j<columnInfo.size();j++){
                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
                    HashMap<String,String> colMap = index.get(columnInfo.get(j).getColumnName());
                    String attr = tuple.get(j);
                    if (colMap.containsKey(attr)) {
                        String list = colMap.get(attr)+ "," + Integer.toString(i);
                        colMap.put(attr, list);
                    } else {
                        String list = Integer.toString(i);
                        colMap.put(attr, list);
                    }
                }
//                Tuple t = new Tuple(this, Iterator.next());
//                HashMap<Column, PrimitiveValue> attrs = t.getAttributes();
//                for (String c : index.keySet()) {
//                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
//                    Column col = new Column(null, c);
//                    if (index.get(c).containsKey(attrs.get(col).toRawString())) {
//                        String list = index.get(c).get(attrs.get(col).toRawString()) + "," + Integer.toString(i);
//                        index.get(c).put(attrs.get(col).toRawString(), list);
//                    } else {
//                        String list = Integer.toString(i);
//                        index.get(c).put(attrs.get(col).toRawString(), list);
//                    }
//                }
                i++;
            }
        }

        final String[] FILE_HEADER = {"Column", "Value", "Index"};
        final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + ".csv";
        CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER).withSkipHeaderRecord();
        try (Writer out = new FileWriter(FILE_NAME);
             CSVPrinter printer = new CSVPrinter(out, format)) {
            for (String column : index.keySet()) {
                for (String value : index.get(column).keySet()) {
                    List<String> records = new ArrayList<>();
                    records.add(column);
                    records.add(value);
                    records.add(index.get(column).get(value));
                    printer.printRecord(records);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public HashMap<String, HashMap<String, ArrayList<String>>> getIndex() {
        final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + ".csv";
        final String[] FILE_HEADER = {"Column", "Value", "Index"};
        CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER).withSkipHeaderRecord();
        HashMap<String, HashMap<String, ArrayList<String>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>

        try (Reader in = new FileReader(FILE_NAME)) {
            Iterable<CSVRecord> records = format.parse(in);
            for (CSVRecord record : records) {
                String col = record.get("Column").toUpperCase();
                //Column c = new Column(new Table(),col);
                String p = record.get("Value");
                String[] indseq = record.get("Index").replace(" ", "").replace("[", "").replace("]", "").split(",");
                ArrayList<String> list = new ArrayList<>(Arrays.asList(indseq));
                if (index.containsKey(col)) {
                    index.get(col).put(p, list);
                } else {
                    HashMap<String, ArrayList<String>> hashMap = new HashMap<>();
                    hashMap.put(p, list);
                    index.put(col, hashMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return index;
    }


    public void setIndexTxt() throws Exception {

        HashMap<String, HashMap<String, String>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>

        for (int i = 0; i < columnInfo.size(); i++) {
            index.put(columnInfo.get(i).getColumnName(), new HashMap());
        }
        CSVParser parser = new CSVParser(new FileReader(fileDir), CSVFormat.DEFAULT.withDelimiter('|'));
        Iterator<CSVRecord> Iterator = parser.iterator();
        int i = 1;
        if (index.size() != 0) {
            while (Iterator.hasNext()) {
                Tuple t = new Tuple(this, Iterator.next());
                HashMap<Column, PrimitiveValue> attrs = t.getAttributes();
                for (String c : index.keySet()) {
                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
                    Column col = new Column(null, c);
                    if (index.get(c).containsKey(attrs.get(col).toRawString())) {
                        String list = index.get(c).get(attrs.get(col).toRawString()) + "," + Integer.toString(i);
                        index.get(c).put(attrs.get(col).toRawString(), list);
                    } else {
                        String list = Integer.toString(i);
                        index.get(c).put(attrs.get(col).toRawString(), list);
                    }
                }
                i++;
            }
        }

        File file = new File("indexes/" + this.getTableName().toUpperCase() + ".txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file, false);
        BufferedWriter bw = new BufferedWriter(fw);
        for (String column : index.keySet()) {
            for (String value : index.get(column).keySet()) {
                String record = column + "@" + value + "@" + index.get(column).get(value)+"\n";
                bw.write(record);
            }
        }
        bw.close();
        fw.close();
    }

    public HashMap<String, HashMap<String, ArrayList<String>>> getIndex1() {
        HashMap<String, HashMap<String, ArrayList<String>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("indexes/" + this.getTableName().toUpperCase() + ".txt")),
                    "UTF-8"));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                String[] record = lineTxt.split("@");
                String col = record[0].toUpperCase();
                String p = record[1];
                String[] indseq = record[2].replace(" ", "").replace("[", "").replace("]", "").split(",");
                ArrayList<String> list = new ArrayList<>(Arrays.asList(indseq));
                if (index.containsKey(col)) {
                    index.get(col).put(p, list);
                } else {
                    HashMap<String, ArrayList<String>> hashMap = new HashMap<>();
                    hashMap.put(p, list);
                    index.put(col, hashMap);
                }
            }
            br.close();
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }
        return index;
    }

    public void setIndex() throws Exception {
        HashMap<String, HashMap<String, ArrayList<Integer>>> index1 = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
        HashMap<String, HashMap<String, ArrayList<Integer>>> index2 = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>

        for (int i = 0; i < columnInfo.size() / 2 + 1; i++) {
            index1.put(columnInfo.get(i).getColumnName(), new HashMap());
        }
        CSVParser parser = new CSVParser(new FileReader(fileDir), CSVFormat.DEFAULT.withDelimiter('|'));
        Iterator<CSVRecord> Iterator = parser.iterator();
        int i = 1;
        if (index1.size() != 0) {
            while (Iterator.hasNext()) {
                Tuple t = new Tuple(this, Iterator.next());
                HashMap<Column, PrimitiveValue> attrs = t.getAttributes();
                for (String c : index1.keySet()) {
                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
                    Column col = new Column(null, c);
                    if (index1.get(c).containsKey(attrs.get(col).toRawString())) {
                        index1.get(c).get(attrs.get(col).toRawString()).add(i);
                    } else {
                        ArrayList<Integer> list = new ArrayList<>();
                        list.add(i);
                        index1.get(c).put(attrs.get(col).toRawString(), list);
                    }
                }
                i++;
            }
        }
        final String[] FILE_HEADER = {"Value", "Index"};
        for (String colName : index1.keySet()) {
            final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + "_" + colName + ".csv";
            CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER).withSkipHeaderRecord();
            try (Writer out = new FileWriter(FILE_NAME);
                 CSVPrinter printer = new CSVPrinter(out, format)) {
                for (String value : index1.get(colName).keySet()) {
                    List<String> records = new ArrayList<>();
                    records.add(value);
                    records.add(index1.get(colName).get(value).toString());
                    printer.printRecord(records);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        index1.clear();

        for (int j = columnInfo.size() / 2 + 1; j < columnInfo.size(); j++) {
            index2.put(columnInfo.get(j).getColumnName(), new HashMap());
        }
        parser = new CSVParser(new FileReader(fileDir), CSVFormat.DEFAULT.withDelimiter('|'));
        Iterator = parser.iterator();
        i = 1;
        if (index2.size() != 0) {
            while (Iterator.hasNext()) {
                Tuple t = new Tuple(this, Iterator.next());
                HashMap<Column, PrimitiveValue> attrs = t.getAttributes();
                for (String c : index2.keySet()) {
                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
                    Column col = new Column(null, c);
                    if (index2.get(c).containsKey(attrs.get(col).toRawString())) {
                        index2.get(c).get(attrs.get(col).toRawString()).add(i);
                    } else {
                        ArrayList<Integer> list = new ArrayList<>();
                        list.add(i);
                        index2.get(c).put(attrs.get(col).toRawString(), list);
                    }
                }
                i++;
            }
        }
        for (String colName : index2.keySet()) {
            final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + "_" + colName + ".csv";
            CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER).withSkipHeaderRecord();
            try (Writer out = new FileWriter(FILE_NAME);
                 CSVPrinter printer = new CSVPrinter(out, format)) {
                for (String value : index2.get(colName).keySet()) {
                    List<String> records = new ArrayList<>();
                    records.add(value);
                    records.add(index2.get(colName).get(value).toString());
                    printer.printRecord(records);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        index2.clear();

    }
    public HashMap<String, ArrayList<String>> getIndex(String ColName) {
        final String FILE_NAME = "indexes/" + this.getTableName().toUpperCase() + "_" + ColName + ".csv";
        final String[] FILE_HEADER = {"Value", "Index"};
        CSVFormat format = CSVFormat.DEFAULT.withHeader(FILE_HEADER);

        HashMap<String, ArrayList<String>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>

        try (Reader in = new FileReader(FILE_NAME)) {
            Iterable<CSVRecord> records = format.parse(in);
            for (CSVRecord record : records) {
                //Column c = new Column(new Table(),col);
                String p = record.get("Value");
                String[] indseq = record.get("Index").replace(" ", "").replace("[", "").replace("]", "").split(",");
                ArrayList<String> list = new ArrayList<>(Arrays.asList(indseq));
                index.put(p, list);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return index;
    }

    public void print() {
        Iterator<Tuple> iterator = this.getIterator();
        while (iterator.hasNext()) {
            iterator.next().printTuple(this.columnDefinitions, this.columnInfo);
        }
    }




}
