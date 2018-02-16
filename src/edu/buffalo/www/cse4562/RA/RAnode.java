package edu.buffalo.www.cse4562.RA;

public class RAnode {
    private String nodeName;
    private RAnode leftChild;
    private RAnode rightChild;
    private int finish;

    public RAnode(String nodeName, RAnode leftChild, RAnode rightChild) {
        this.nodeName = nodeName;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.finish = 0;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public RAnode getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(RAnode leftChild) {
        this.leftChild = leftChild;
    }

    public RAnode getRightChild() {
        return rightChild;
    }

    public void setRightChild(RAnode rightChild) {
        this.rightChild = rightChild;
    }

    public int getFinish() {
        return finish;
    }

    public void setFinish(int finish) {
        this.finish = finish;
    }
}
