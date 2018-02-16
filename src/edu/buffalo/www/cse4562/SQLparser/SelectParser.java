package edu.buffalo.www.cse4562.SQLparser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;

import java.util.HashMap;
import java.util.List;

public class SelectParser {
    public static HashMap<String, Object> SelectFunction(SelectBody body, int tableCounter) {
        HashMap<String, Object> query = new HashMap<>();
        HashMap<String, Object> operations = new HashMap<>();

        if (!(body instanceof Union)) {
            // get required elements
            List<Join> joins = ((PlainSelect) body).getJoins();
            List<SelectItem> selectItem = ((PlainSelect) body).getSelectItems();
            Expression where = ((PlainSelect) body).getWhere();
            List orderby = ((PlainSelect) body).getOrderByElements();
            Distinct dist = ((PlainSelect) body).getDistinct();
            Limit lim = ((PlainSelect) body).getLimit();
            FromItem fromItem = ((PlainSelect) body).getFromItem();
            String tableAlias = fromItem.getAlias();


            tableCounter = solveFromList(query, fromItem, tableCounter, tableAlias);

            if (joins != null) {
                for (int i = 0; i < joins.size(); i++) {
                    tableCounter = solveFromList(query, joins.get(i).getRightItem(), tableCounter, joins.get(i).getRightItem().getAlias());
                }
            }

            if (where != null) {
                operations.put("WHERE",where);
            }

            //Select Item
            if (selectItem!=null){
                //The select list must be traversed during the data process
                //Do not need to parse the select list here,
                operations.put("SELECTITEM",selectItem);
            }
//            for (int i = 0; i < selectItem.size(); i++) {
//                //todo
//                if (selectItem.get(i) instanceof SelectExpressionItem) {
//                    selectList.add(selectItem.get(i));
//                } else if (selectItem.get(i) instanceof AllTableColumns) {
//                    String schema = ((AllTableColumns) selectItem).getTable().getSchemaName();
//                    selectList.add(selectItem.get(i));
//                } else if (selectItem.get(i) instanceof AllColumns) {
//                    selectList.add(selectItem.get(i));
//                }
//            }

            if (orderby != null) {
                operations.put("ORDERBY", orderby);
            }
            if (dist != null) {
                operations.put("DISTINCT", dist);
            }
            if (lim != null) {
                operations.put("LIMIT", lim);
            }

            query.put("OPERATIONS", operations);
        } else {
            //todo UNION
            int unionSize = ((Union) body).getPlainSelects().size();
            for (int i=0;i<unionSize;i++){
                PlainSelect sub = ((Union) body).getPlainSelects().get(i);
                //query.put("Union",SelectFunction(sub,tableCounter));
            }
        }
        return query;
    }

    public static int solveFromList(HashMap<String, Object> query, FromItem fromItem, int tableCounter, String tableAlias) {
        if (fromItem instanceof SubSelect) {
            //1 subselect
            SubSelect sub = ((SubSelect) fromItem);
            String subAlisa = sub.getAlias();
            if (subAlisa == null) {
                query.put("FromsubSelect" + tableCounter, SelectFunction(sub.getSelectBody(), tableCounter));
            } else {
                query.put(subAlisa, SelectFunction(sub.getSelectBody(), tableCounter));
            }
        } else {
            //0 subselect
            if (tableAlias == null) {
                query.put("fromSelect" + tableCounter++, fromItem);
            } else {
                query.put(tableAlias, fromItem);
                tableCounter++;
            }
        }
        return tableCounter;
    }

}
