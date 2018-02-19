package edu.buffalo.www.cse4562.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TempTable implements Iterator{
    List<Tuple> tempTable = new ArrayList<>();
    private Iterator iterator;

    public TempTable(List<Tuple> tempTable) {
        this.tempTable = tempTable;
        this.iterator = tempTable.iterator();
    }

    public TempTable() {
    }

    public List<Tuple> getTempTable() {
        return tempTable;
    }

    public void setTempTable(List<Tuple> tempTable) {
        this.tempTable = tempTable;
    }

    public Iterator getIterator() {
        return iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Object next() {
        return iterator.next();
    }
}
