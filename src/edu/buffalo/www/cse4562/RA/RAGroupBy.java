package edu.buffalo.www.cse4562.RA;

import edu.buffalo.www.cse4562.Table.TableObject;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class RAGroupBy extends RANode  {
    private List<Column> groupByReferences;
    private String operation = "GROUPBY";
    private List<Function> aggSelect = new ArrayList<>();
    public RAGroupBy(List<Column> groupByReferences, List<SelectItem> selectItems) {
        for(SelectItem item:selectItems){
            Expression exp = ((SelectExpressionItem)item).getExpression();
            if (exp instanceof Function){
                aggSelect.add((Function) exp);
            }
        }

        this.groupByReferences = groupByReferences;
    }


    public List<Column> getGroupByReferences() {
        return groupByReferences;
    }

    public TableObject Eval(TableObject OutputTable, List<Column> groupByReferences)throws Exception{

        return OutputTable;
    }

    @Override
    public String getOperation() {
        return operation;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Object next() {
        return null;
    }
}
