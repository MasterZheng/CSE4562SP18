package edu.buffalo.www.cse4562.RA;


import java.util.Iterator;

public abstract class RANode implements Iterator{
    public RANode leftNode;
    public RANode rightNode;
    public String operation;

    public RANode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(RANode leftNode) {
        this.leftNode = leftNode;
    }

    public RANode getRightNode() {
        return rightNode;
    }

    public void setRightNode(RANode rightNode) {
        this.rightNode = rightNode;
    }
    public abstract String getOperation();

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract Object next();
}
