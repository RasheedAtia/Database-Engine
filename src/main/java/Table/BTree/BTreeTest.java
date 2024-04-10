package Table.BTree;

public class BTreeTest {
	public static void main(String[] args) {
		IntegerBTree tree = new IntegerBTree();

		tree.insert(10);
		tree.insert(48);
		tree.insert(23);
		tree.insert(33);
		tree.insert(12);

		tree.insert(50);

		tree.insert(15);
		tree.insert(18);
		tree.insert(20);
		tree.insert(21);
		tree.insert(31);
		tree.insert(45);
		tree.insert(47);
		tree.insert(52);

		tree.insert(30);

		tree.insert(19);
		tree.insert(22);

		tree.insert(11);
		tree.insert(13);
		tree.insert(16);
		tree.insert(17);

		tree.insert(1);
		tree.insert(2);
		tree.insert(3);
		tree.insert(4);
		tree.insert(5);
		tree.insert(6);
		tree.insert(7);
		tree.insert(8);
		tree.insert(9);

		tree.print();
		// DBBTreeIterator iterator = new DBBTreeIterator(tree);
		// iterator.print();
	}
}

class IntegerBTree extends BTree<Integer, Integer> {
	public void insert(int key) {
		this.insert(key, key);
	}

	public void remove(int key) {
		this.delete(key);
	}
}