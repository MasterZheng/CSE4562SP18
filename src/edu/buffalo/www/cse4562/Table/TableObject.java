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
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.util.*;
import java.util.logging.Logger;

public class TableObject {
    private Table table;
    private String tableName;
    private String alisa;
    private String fileDir;

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
    private HashMap<Column, HashMap<PrimitiveValue, ArrayList<Integer>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
    private HashMap<Column, HashMap<PrimitiveValue, Integer>> statistics = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,count>
    private int size = 0;
    static Logger logger = Logger.getLogger(TableObject.class.getName());


    public TableObject(TableObject tableObject, RANode raTable) {

        this.table = ((RATable) raTable).getTable();
        this.tableName = ((RATable) raTable).getTable().getName();
        this.alisa = this.table.getAlias();
        this.columnDefinitions = tableObject.getColumnDefinitions();
        this.columnInfo = tableObject.getColumnInfo();
        this.fileDir = tableObject.getFileDir();
        this.index = tableObject.getIndex();
        this.statistics = tableObject.getStatistics();
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


    public void optimize() {

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

    public void setIndex(HashMap<Column, HashMap<PrimitiveValue, ArrayList<Integer>>> index) {
        this.index = index;
    }

    public HashMap<Column, HashMap<PrimitiveValue, ArrayList<Integer>>> getIndex() {
        return index;
    }

    public HashMap<Column, HashMap<PrimitiveValue, Integer>> getStatistics() {
        return statistics;
    }

    public int getSize() {
        return size;
    }

    public void indexAndStatistic() throws Exception {
        HashMap<Column, HashMap<PrimitiveValue, ArrayList<Integer>>> index = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,arraylist>
        HashMap<Column, HashMap<PrimitiveValue, Integer>> statistics = new HashMap<>();//Key 是列名，value是hashmap<primitiveValue,count>
        int size = 0;
        for (int i = 0; i < primaryKey.size(); i++) {
            index.put(primaryKey.get(i),new HashMap());
        }
        for (int i = 0; i < references.size(); i++) {
            index.put(references.get(i), new HashMap());
        }
        for (int i = 0; i < columnInfo.size(); i++) {
            statistics.put(columnInfo.get(i), new HashMap());
        }
//        for (int i = 0; i < primaryKey.size(); i++) {
//            index.put(columnInfo.get(i), new HashMap());
//        }
        CSVParser parser = new CSVParser(new FileReader(fileDir), CSVFormat.DEFAULT.withDelimiter('|'));
        Iterator<CSVRecord> Iterator = parser.iterator();
        int i = 1;
        if (index.size() != 0) {
            while (Iterator.hasNext()) {
                Tuple t = new Tuple(this, Iterator.next());
                HashMap<Column, PrimitiveValue> attrs = t.getAttributes();
                for (Column c : index.keySet()) {
                    //判断当前index表中某列的index是否存在这个值，如果存在，将下标加入list
                    if (index.get(c).containsKey(attrs.get(c))) {
                        index.get(c).get(attrs.get(c)).add(i);
                    } else {
                        ArrayList<Integer> list = new ArrayList<>();
                        list.add(i);
                        index.get(c).put(attrs.get(c), list);
                    }
                }

                for (Column c : statistics.keySet()) {
                    //判断当前statistics表中某列的是否存在这个值，如果存在，+1,
                    if (statistics.get(c).containsKey(attrs.get(c))) {
                        statistics.get(c).put(attrs.get(c), statistics.get(c).get(attrs.get(c)) + 1);
                    } else {
                        statistics.get(c).put(attrs.get(c), 1);
                    }
                }
                i++;
                size++;
            }
        }

        this.index = index;
        this.statistics = statistics;
        this.size = size;
    }


    public void print() {
        Iterator<Tuple> iterator = this.getIterator();
        while (iterator.hasNext()) {
            iterator.next().printTuple(this.columnDefinitions, this.columnInfo);
        }
    }

}
