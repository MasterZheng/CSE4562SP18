package edu.buffalo.www.cse4562.Table;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class TableObject {
    private CreateTable createTable;
    private Table table;
    private String tableName;
    private String fileDir;
    private File DBFile;//not be used
    private List<ColumnDefinition> columnDefinitions;
    private boolean empty;
    private List<Tuple> tupleList = new ArrayList<>();

    static Logger logger = Logger.getLogger(TableObject.class.getName());
    public TableObject(){
        this.empty = true;
    }

    public TableObject(List<ColumnDefinition> columnDefinitions, List<Tuple> tupleList) {
        this.tupleList = tupleList;
        this.columnDefinitions = columnDefinitions;
        this.fileDir = null;
        this.DBFile = null;
        this.empty = false;
        this.table = null;
    }

    public TableObject(CreateTable createTable, Table table, String tableName) {
        this.createTable = createTable;
        this.table = table;
        this.tableName = tableName;
        columnDefinitions = createTable.getColumnDefinitions();

        if (this.fileDir==null){
            //fileDir = createTable.getTable().getName()+".csv";
            fileDir = "data/"+createTable.getTable().getName()+".dat";

            DBFile = new File(fileDir);
        }
        this.empty = false;
//        this.fileDir = fileDir;
//        this.DBFile = DBFile;
    }

    public CreateTable getCreateTable() {
        return createTable;
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

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
    }

    public File getDBFile() {
        return DBFile;
    }

    public void setDBFile(File DBFile) {
        this.DBFile = DBFile;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public boolean isEmpty() {
        return empty;
    }

    public void settupleList(List<Tuple> tupleList) {
        this.tupleList = tupleList;
    }
    public Iterator getIterator() {
        return this.tupleList.iterator();
    }

}
