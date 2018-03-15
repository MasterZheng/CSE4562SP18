package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Evaluate.evaluate;
import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RAProjection extends RANode {

    private String operation = "PROJECTION";
    private List selectItem;
    private boolean flag = false;//record select *
    private List<Expression> columnList = new ArrayList<>();
    private List<Column> columnInfo = new ArrayList<>();
    public RAProjection(List<SelectItem> selectItem) {
        this.selectItem = selectItem;
        if (selectItem!=null&&selectItem.get(0) instanceof AllColumns){
            this.flag = true;
        }
    }

    public List getSelectItem() {
        return selectItem;
    }

    public TableObject Eval(TableObject OutputTable,String tableName)throws Exception{
        if (!flag){
            projectionParser(OutputTable.getColumnInfo(),new Table(tableName));
            evaluate eva = new evaluate(this.selectItem);
            OutputTable.settupleList(eva.project(OutputTable.getTupleList(),this.columnList,this.columnInfo));
        }else {
            if (tableName!=null){
                for (int i = 0;i<OutputTable.getColumnInfo().size();i++){
                    OutputTable.getColumnInfo().get(i).setTable(new Table(tableName));
                }
            }
        }
        return OutputTable;
    }
    public void setSelectItem(List selectItem) {
        this.selectItem = selectItem;
    }


    public String getOperation() {
        return this.operation;
    }

    @Override
    public boolean hasNext() {
        if (this.leftNode != null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public RANode next() {
        return this.leftNode;
    }

    public void projectionParser(List<Column> columns, Table table) throws Exception {
        //todo 查表后进行 projection，优化，利用新列定义，不解析 selectItem
        for (int i = 0; i < selectItem.size(); i++) {
            Object s = selectItem.get(i);
            if (s instanceof AllTableColumns) {
                String tableName = ((AllTableColumns) s).getTable().getName();
                for (int j = 0; j < columns.size(); j++) {
                    if (columns.get(j).getTable().getName().equals(tableName)) {
                        Column column = new Column(columns.get(j).getTable(), columns.get(j).getColumnName());
                        columnList.add(columns.get(j));
                        columnInfo.add(column);
                        if (!table.toString().equals("null")) {
                            column.setTable(table);
                        }
                    }
                }
            } else if (((SelectExpressionItem) s).getExpression() instanceof Column) {
                String alias = ((SelectExpressionItem) s).getAlias();
                Column column = new Column(((Column) ((SelectExpressionItem) s).getExpression()).getTable(), ((Column) ((SelectExpressionItem) s).getExpression()).getColumnName());
                if (alias == null) {
                    columnList.add(((SelectExpressionItem) s).getExpression());
                    columnInfo.add(column);
                } else {
                    column.setColumnName(alias);
                    columnList.add(((SelectExpressionItem) s).getExpression());
                    columnInfo.add(column);
                }
                if (!table.toString().equals("null")) {
                    column.setTable(table);
                }
            } else {
                String name = ((SelectExpressionItem) s).getAlias();
                columnList.add(null);
                columnInfo.add(new Column(table, name));
            }
        }
    }

}
