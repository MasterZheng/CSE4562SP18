package edu.buffalo.www.cse4562.Table;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Table;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;

import java.io.Serializable;

public class Column extends net.sf.jsqlparser.schema.Column implements Expression,Serializable{
    private String columnName = "";
    private Table table;

    public Column() {
    }

    public Column(Table table, String columnName) {
        this.table = table;
        this.columnName = columnName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public Table getTable() {
        return this.table;
    }

    public void setColumnName(String string) {
        this.columnName = string;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getWholeColumnName() {
        String columnWholeName = null;
        String tableWholeName = this.table.getWholeTableName();
        if (tableWholeName != null && tableWholeName.length() != 0) {
            columnWholeName = tableWholeName + "." + this.columnName;
        } else {
            columnWholeName = this.columnName;
        }

        return columnWholeName;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String toString() {
        return this.getWholeColumnName();
    }

    public int hashCode() {
        return this.columnName.hashCode();
    }

}

