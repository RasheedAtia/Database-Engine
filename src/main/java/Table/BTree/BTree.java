package Table.BTree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

import Engine.Utils;

/**
 * A B+ tree Since the structures and behaviors between internal node and
 * external node are different, so there are two different classes for each kind
 * of node.
 *
 * @param < TKey > the data type of the key
 * @param < TValue > the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable {
    /**
     * @uml.property name="root"
     * @uml.associationEnd multiplicity="(1 1)"
     */
    private BTreeNode<TKey> root;
    /**
     * @uml.property name="tableName"
     */
    private String tableName;

    public BTree() {
        this.root = new BTreeLeafNode<TKey, TValue>();
    }

    /**
     * Insert a new key and its associated value into the B+ tree.
     */
    public void insert(TKey key, TValue value) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        leaf.insertKey(key, value);

        if (leaf.isOverflow()) {
            BTreeNode<TKey> n = leaf.dealOverflow();
            if (n != null)
                this.root = n;
        }
    }

    /**
     * Search a key value on the tree and return its associated value.
     */
    public TValue search(TKey key) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        int index = leaf.search(key);
        return (index == -1) ? null : leaf.getValue(index);
    }

    public Vector<TValue> searchGreater(TKey key, String type) {
        Vector<TValue> res = new Vector<>();
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        System.out.println(leaf.getKey(0));

        BTreeNode<TKey> smallest = leaf;

        while (smallest != null) {

            System.out.println(smallest.getKey(0));
            for (int i = 0; i < smallest.keyCount; i++) {
                System.out.println("hiiiii");
                if (Utils.compareKeys(smallest.getKey(i).toString(), key.toString(), type) > 0) {
                    System.out.println("noooooo");
                    res.add(((BTreeLeafNode<TKey, TValue>) smallest).getValue(i));
                }
            }
            smallest = smallest.getRightSibling();
        }

        return res;
    }

    public Vector<TValue> searchLower(TKey key, String type) {
        Vector<TValue> res = new Vector<>();
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        if (leaf == null) {
            return res;
        }
        BTreeNode<TKey> smallest = leaf;
        while (smallest != null) {
            for (int i = 0; i < smallest.keyCount; i++) {
                System.out.println("hiiiii");
                if (Utils.compareKeys(smallest.getKey(i).toString(), key.toString(), type) < 0) {
                    res.add(((BTreeLeafNode<TKey, TValue>) smallest).getValue(i));
                }
            }
            smallest = smallest.getLeftSibling();
        }

        return res;
    }

    /**
     * Delete a key and its associated value from the tree.
     */
    public void delete(TKey key) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        if (leaf.delete(key) && leaf.isUnderflow()) {
            BTreeNode<TKey> n = leaf.dealUnderflow();
            if (n != null)
                this.root = n;
        }
    }

    /**
     * Search the leaf node which should contain the specified key
     */
    @SuppressWarnings("unchecked")
    private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
        BTreeNode<TKey> node = this.root;
        while (node.getNodeType() == TreeNodeType.InnerNode) {
            node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
        }

        return (BTreeLeafNode<TKey, TValue>) node;
    }

    public void print() {
        ArrayList<BTreeNode> upper = new ArrayList<>();
        ArrayList<BTreeNode> lower = new ArrayList<>();

        upper.add(root);
        while (!upper.isEmpty()) {
            BTreeNode cur = upper.get(0);
            if (cur instanceof BTreeInnerNode) {
                ArrayList<BTreeNode> children = ((BTreeInnerNode) cur).getChildren();
                for (int i = 0; i < children.size(); i++) {
                    BTreeNode child = children.get(i);
                    if (child != null)
                        lower.add(child);
                }
            }
            System.out.println(cur.toString() + " ");
            upper.remove(0);
            if (upper.isEmpty()) {
                System.out.println("\n");
                upper = lower;
                lower = new ArrayList<>();
            }
        }
    }

    public BTreeLeafNode getSmallest() {
        return this.root.getSmallest();
    }

    public String commit() {
        return this.root.commit();
    }
}