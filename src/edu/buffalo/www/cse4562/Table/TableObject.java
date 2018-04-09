package edu.buffalo.www.cse4562.Table;

import edu.buffalo.www.cse4562.RA.RANode;
import edu.buffalo.www.cse4562.RA.RATable;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

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


    private List<Tuple> tupleList = new ArrayList<>();
    private HashMap<Integer, ArrayList<Tuple>> hashMap = new HashMap<>();
    private List<Integer> mapRelations = new ArrayList<>();
    static Logger logger = Logger.getLogger(TableObject.class.getName());

    public TableObject(TableObject tableObject, RANode raTable) {

        this.table = ((RATable) raTable).getTable();
        this.tableName = ((RATable) raTable).getTable().getName();
        this.alisa = this.table.getAlias();
        this.columnDefinitions = tableObject.getColumnDefinitions();
        this.columnInfo = tableObject.getColumnInfo();
        this.fileDir = tableObject.getFileDir();
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
        for (ColumnDefinition c : this.columnDefinitions)
            columnInfo.add(new Column(table, c.getColumnName()));
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

    public HashMap<Integer, ArrayList<Tuple>> getHashMap() {
        return this.hashMap;
    }

    public void setHashMap(HashMap<Integer, ArrayList<Tuple>> hashMap) {
        this.hashMap = hashMap;
    }

    public void print(int c) {
        Iterator<Tuple> iterator = this.getIterator();
        while (iterator.hasNext()) {
            iterator.next().printTuple(this.columnDefinitions, this.columnInfo, c);
        }
    }
}
