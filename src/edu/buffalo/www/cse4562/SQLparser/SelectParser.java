package edu.buffalo.www.cse4562.SQLparser;

import edu.buffalo.www.cse4562.RA.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;


import java.util.HashMap;
import java.util.List;

public class SelectParser {
    /*
    implement 2 ways to parse SQL
     */
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
                operations.put("WHERE", where);
            }

            //Select Item
            if (selectItem != null) {
                //The select list must be traversed during the data process
                //Do not need to parse the select list here,
                operations.put("SELECTITEM", selectItem);
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
            for (int i = 0; i < unionSize; i++) {
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

    public static RANode SelectFunction(SelectBody body) {
        if (!(body instanceof Union)) {
            List<Join> joins = ((PlainSelect) body).getJoins();
            List<SelectItem> selectItem = ((PlainSelect) body).getSelectItems();
            Expression where = ((PlainSelect) body).getWhere();
            List orderby = ((PlainSelect) body).getOrderByElements();
            Distinct dist = ((PlainSelect) body).getDistinct();
            Limit lim = ((PlainSelect) body).getLimit();
            FromItem fromItem = ((PlainSelect) body).getFromItem();

            //parse the SQL and build the tree down to up
            //process fromItem joins
            RANode joinNode = new RAJoin(fromItem, joins);
            RANode point = joinNode;
            if (joins == null) {
                //only 1 table involved
                if (fromItem instanceof SubSelect) {
                    // subSelect
                    joinNode.setLeftNode(SelectFunction(((SubSelect) fromItem).getSelectBody()));
                } else {
                    //table
                    joinNode.setLeftNode(new RATable((Table) fromItem));
                }
            } else {
                //more than one tables involved
                if (fromItem instanceof SubSelect) {
                    // subSelect
                    joinNode.setLeftNode(SelectFunction(((SubSelect) fromItem).getSelectBody()));
                } else {
                    //table
                    joinNode.setLeftNode(new RATable((Table) fromItem));
                }
                for (int i = 0; i < joins.size(); i++) {
                    FromItem join = joins.get(i).getRightItem();
                    if (joinNode.getLeftNode() == null) {
                        //if the leftchild is empty, insert the node into leftchild position.
                        if (join instanceof SubSelect) {
                            joinNode.setLeftNode(SelectFunction(((SubSelect) join).getSelectBody()));
                        } else {
                            joinNode.setLeftNode(new RATable((Table) join));
                        }
                    } else if (joinNode.getRightNode() == null) {
                        //if the rightchild is empty, insert the node into rightchild position.
                        if (join instanceof SubSelect) {
                            joinNode.setRightNode(SelectFunction(((SubSelect) join).getSelectBody()));
                        } else {
                            joinNode.setRightNode(new RATable((Table) join));
                        }
                    } else {
                        //if both children are not empty, new a new RAjoin node , insert the node into leftchild position
                        RANode joinNew = new RAJoin(fromItem, joins);
                        while (joinNode.getLeftNode() != null) {
                            joinNode = joinNode.getLeftNode();
                        }
                        joinNode.setLeftNode(joinNew);
                        joinNode = joinNew;
                        if (join instanceof SubSelect) {
                            joinNode.setLeftNode(SelectFunction(((SubSelect) join).getSelectBody()));
                        } else {
                            joinNode.setLeftNode(new RATable((Table) join));
                        }
                    }
                }
            }


            //process where
            if (where != null) {
                RANode whereNode = new RASelection(where);
                whereNode.setLeftNode(point);
                point = whereNode;
            }

            //process projection
            RANode projNode = new RAProjection(selectItem);
            projNode.setLeftNode(point);
            point = projNode;

            //process orderby
            if (orderby != null) {
                RANode orderbyNode = new RAOrderby(orderby);
                orderbyNode.setLeftNode(point);
                point = orderbyNode;
            }

            //process distinct
            if (dist != null) {
                RANode distNode = new RADistinct(dist);
                distNode.setLeftNode(point);
                point = distNode;
            }

            //process limit
            if (lim != null) {
                RANode limNode = new RALimit(lim);
                limNode.setLeftNode(point);
                point = limNode;
            }
            return point;

        } else {
            //todo UNION
            return null;

        }
    }
}
