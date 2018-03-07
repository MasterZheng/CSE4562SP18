package edu.buffalo.www.cse4562.Evaluate;

import edu.buffalo.www.cse4562.Table.TableObject;
import edu.buffalo.www.cse4562.Table.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class evaluate extends Eval {
    static Logger logger = Logger.getLogger(evaluate.class.getName());
    private Tuple tupleLeft;
    private Tuple tupleRight;
    private Expression expression;
    private Map<String, TableObject> tableMap;
    private List<Object> selectList;

    public evaluate(Tuple tupleLeft, Tuple tupleRight, Expression expression, Map<String, TableObject> tableMap) {
        this.tupleLeft = tupleLeft;
        this.tupleRight = tupleRight;
        this.expression = expression;
        this.tableMap = tableMap;
    }

    public evaluate(Tuple tupleLeft, List<Object> list, HashMap<String, TableObject> tableMap) {
        this.tupleLeft = tupleLeft;
        this.selectList = list;
        this.tableMap = tableMap;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException {
        HashMap<String, PrimitiveValue> tupleMap = tupleLeft.getAttributes();
        String colName = column.getColumnName().toUpperCase();
        return tupleMap.get(colName);
    }

    public boolean selectEval() throws Exception {
        PrimitiveValue result = eval(expression);
        return result.toBool();
    }

    public Tuple projectEval(List<ColumnDefinition> list) throws Exception {
        Tuple newTuple = new Tuple();
        HashMap<String, PrimitiveValue> attributes = new HashMap<>();
        for (int i = 0; i < selectList.size(); i++) {
            Object s = selectList.get(i);
            if (s instanceof AllColumns) {
                newTuple = tupleLeft;
                break;
            } else if (s instanceof AllTableColumns) {
                //todo
            } else {
                //  todo 优化
                if (((SelectExpressionItem) s).getExpression() instanceof Column) {
                    String name = ((Column) ((SelectExpressionItem) s).getExpression()).getColumnName();
                    String alias = ((SelectExpressionItem) s).getAlias();

                    if (alias == null) {
                        attributes.put(name, tupleLeft.getAttributes().get(name));
                    } else {
                        attributes.put(alias, tupleLeft.getAttributes().get(name));
                    }

                } else {
                    PrimitiveValue result = eval(((SelectExpressionItem) s).getExpression());
                    String name = ((SelectExpressionItem) s).getAlias();
                    attributes.put(name, result);

                }
            }
            newTuple.setAttributes(attributes);
        }

        newTuple.setTableName(tupleLeft.getTableName());
        return newTuple;
    }

}
