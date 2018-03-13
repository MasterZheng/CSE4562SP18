package edu.buffalo.www.cse4562.RA;

import net.sf.jsqlparser.schema.Table;

public class RATable extends RANode{
    //No useful actions, just make a table be a node in the RAtree
    private String operation = "TABLE";
    private Table table;


    public RATable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }


    public void setTable(Table table) {
        this.table = table;
    }

    public String getOperation() {
        return operation;
    }

    @Override
    public boolean hasNext() {
        if (this.leftNode!=null||this.rightNode!=null){
            return true;
        }else {
            return false;
        }
    }

    @Override
    public Object next() {
        return null;
    }
}
