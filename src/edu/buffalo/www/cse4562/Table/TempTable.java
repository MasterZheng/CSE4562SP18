package edu.buffalo.www.cse4562.Table;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TempTable {
    List<Tuple> tempTable = new ArrayList<>();
    private List<ColumnDefinition> columnDefinitions;

    public TempTable(List<ColumnDefinition> columnDefinitions, List<Tuple> tempTable) {
        this.tempTable = tempTable;
        this.columnDefinitions = columnDefinitions;
        //this.tableObject = new TableObject(columnDefinitions);
    }

    public TempTable() {
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) { this.columnDefinitions = columnDefinitions; }

    public Iterator getIterator() {
        return this.tempTable.iterator();
    }

    public List<Tuple> getTempTable() { return tempTable; }

    public void setTempTable(List<Tuple> tempTable) {
        this.tempTable = tempTable;
    }


}
