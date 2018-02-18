package edu.buffalo.www.cse4562.SQLparser;

import edu.buffalo.www.cse4562.RA.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;


import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class SelectParser {
    /*
    implement 2 ways to parse SQL
     */
    static Logger logger = Logger.getLogger(SelectItem.class.getName());

    public static HashMap<String, Object> SelectFunction(SelectBody body, int tableCounter) {
        logger.info("Use hashMap to parse SQL");

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
        logger.info("Use RATree to parse SQL");

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
            RANode pointer = joinNode;

            if (fromItem instanceof SubSelect) {
                // subSelect
                RANode subSelect = SelectFunction(((SubSelect) fromItem).getSelectBody());
                joinNode.setLeftNode(subSelect);
                subSelect.setParentNode(pointer);
            } else {
                //table
                RATable table = new RATable((Table) fromItem);
                joinNode.setLeftNode(table);
                table.setParentNode(pointer);
            }
            if (joins != null) {
                for (int i = 0; i < joins.size(); i++) {
                    FromItem join = joins.get(i).getRightItem();

                    //if the leftchild is empty, insert the node into leftchild position.
                    if (join instanceof SubSelect) {
                        if (joinNode.getLeftNode() == null) {
                            //todo
                            RANode subSelect = SelectFunction(((SubSelect) join).getSelectBody());
                            joinNode.setLeftNode(subSelect);
                            subSelect.setParentNode(pointer);
                        } else if (joinNode.getRightNode() == null) {
                            RANode subSelect = SelectFunction(((SubSelect) join).getSelectBody());
                            joinNode.setRightNode(subSelect);
                            subSelect.setParentNode(pointer);
                        } else {
                            //if both children are not empty, new a new RAjoin node , insert the node into leftchild position
                            RANode joinNew = new RAJoin(fromItem, joins);
                            while (joinNode.getLeftNode() != null) {
                                joinNode = joinNode.getLeftNode();
                            }
                            joinNode.setLeftNode(joinNew);
                            joinNew.setParentNode(joinNode);
                            joinNode = joinNew;
                            RANode subSelect = SelectFunction(((SubSelect) join).getSelectBody());
                            joinNode.setLeftNode(subSelect);
                            subSelect.setParentNode(joinNode);
                        }
                    } else {
                        if (joinNode.getLeftNode() == null) {
                            RATable table = new RATable((Table) join);
                            joinNode.setLeftNode(table);
                            table.setParentNode(pointer);
                        } else if (joinNode.getRightNode() == null) {
                            RATable table = new RATable((Table) join);
                            joinNode.setRightNode(table);
                            table.setParentNode(pointer);
                        } else {
                            //if both children are not empty, new a new RAjoin node , insert the node into leftchild position
                            RANode joinNew = new RAJoin(fromItem, joins);
                            while (joinNode.getLeftNode() != null) {
                                joinNode = joinNode.getLeftNode();
                            }
                            joinNode.setLeftNode(joinNew);
                            joinNew.setParentNode(joinNode);
                            joinNode = joinNew;
                            RATable table = new RATable((Table) join);
                            joinNode.setLeftNode(table);
                            table.setParentNode(joinNode);
                        }

                    }
                }
            }


            //process where
            if (where != null) {
                RANode whereNode = new RASelection(where);
                whereNode.setLeftNode(pointer);
                pointer.setParentNode(whereNode);
                pointer = whereNode;
            }

            //process projection
            RANode projNode = new RAProjection(selectItem);
            projNode.setLeftNode(pointer);
            pointer.setParentNode(projNode);
            pointer = projNode;

            //process orderby
            if (orderby != null) {
                RANode orderbyNode = new RAOrderby(orderby);
                orderbyNode.setLeftNode(pointer);
                pointer.setParentNode(orderbyNode);
                pointer = orderbyNode;
            }

            //process distinct
            if (dist != null) {
                RANode distNode = new RADistinct(dist);
                distNode.setLeftNode(pointer);
                pointer.setParentNode(distNode);
                pointer = distNode;
            }

            //process limit
            if (lim != null) {
                RANode limNode = new RALimit(lim);
                limNode.setLeftNode(pointer);
                pointer.setParentNode(limNode);
                pointer = limNode;
            }
            return pointer;
        } else {
            //todo UNION
            return null;
        }

    }
}
