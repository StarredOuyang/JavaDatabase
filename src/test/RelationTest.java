package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.Relation;
import hw1.RelationalOperator;
import hw1.TupleDesc;

public class RelationTest {

	private HeapFile testhf;
	private TupleDesc testtd;

	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	private TupleDesc newDesc;
	private HeapFile hFILE;

	@Before
	public void setup() {

		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(),
					StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}

		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");

		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);

		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");

		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);

	}

	@Test
	public void testSelect() throws IOException {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.select(0, RelationalOperator.EQ, new IntField(530));
		System.out.println(ar.toString());
		assert (ar.getTuples().size() == 5);
		assert (ar.getDesc().equals(atd));
	}

	@Test
	public void testProject() throws IOException {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();

		c.add(1);

		ar = ar.project(c);

		assert (ar.getDesc().getSize() == 4);
		assert (ar.getTuples().size() == 8);
		assert (ar.getDesc().getFieldName(0).equals("a2"));
	}

	@Test
	public void testJoin() throws IOException {
		Relation tr = new Relation(testhf.getAllTuples(), testtd);
		Relation ar = new Relation(ahf.getAllTuples(), atd);

		tr = tr.join(ar, 0, 0);

		System.out.println("Tuples: " + tr.getTuples().size());
		assert (tr.getTuples().size() == 5);
		System.out.println("desc size: " + tr.getDesc().getSize());
		assert (tr.getDesc().getSize() == 141);
	}

	@Test
	public void testRename() throws IOException {
		Relation ar = new Relation(ahf.getAllTuples(), atd);

		ArrayList<Integer> f = new ArrayList<Integer>();
		ArrayList<String> n = new ArrayList<String>();

		f.add(0);
		n.add("b1");

		ar = ar.rename(f, n);
		// works, but something wront with tuple string... td in relation is being
		// changed but td in tuple is not?
		assertTrue(ar.getTuples().size() == 8);
		assertTrue(ar.getDesc().getFieldName(0).equals("b1"));
		assertTrue(ar.getDesc().getFieldName(1).equals("a2"));
		assertTrue(ar.getDesc().getSize() == 8);

	}

	@Test
	public void testAggregate() throws IOException {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);

		ar = ar.aggregate(AggregateOperator.SUM, false);

		assertTrue(ar.getTuples().size() == 1);

		IntField agg = new IntField(ar.getTuples().get(0).getField(0));

		assertTrue(agg.getValue() == 36);

		// Test for max;
		ar = new Relation(ahf.getAllTuples(), atd);
		c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MAX, false);
		assertTrue(ar.getTuples().size() == 1);

		agg = new IntField(ar.getTuples().get(0).getField(0));

		System.out.println("MAX: " + agg.getValue());
		assertEquals(8, agg.getValue());

		// Test for min
		ar = new Relation(ahf.getAllTuples(), atd);
		c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MIN, false);
		assertTrue(ar.getTuples().size() == 1);

		agg = new IntField(ar.getTuples().get(0).getField(0));

		System.out.println("Min: " + agg.getValue());
		assertEquals(1, agg.getValue());

		// AVG test
		ar = new Relation(ahf.getAllTuples(), atd);
		c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.AVG, false);
		assertTrue(ar.getTuples().size() == 1);

		agg = new IntField(ar.getTuples().get(0).getField(0));

		System.out.println("AVG: " + agg.getValue());
		System.out.println(agg.getValue());
		assertEquals(4, agg.getValue()); // ??

		// Test for min
		ar = new Relation(ahf.getAllTuples(), atd);
		c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MIN, false);
		assertTrue(ar.getTuples().size() == 1);

		agg = new IntField(ar.getTuples().get(0).getField(0));

		System.out.println("Min: " + agg.getValue());
		assertEquals(1, agg.getValue());

		// Test for count
		ar = new Relation(ahf.getAllTuples(), atd);
		c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.COUNT, false);
		assertTrue(ar.getTuples().size() == 1);

		agg = new IntField(ar.getTuples().get(0).getField(0));

		System.out.println("Count: " + agg.getValue());
		assertEquals(8, agg.getValue());

	}

	@Test
	public void testGroupBy() throws IOException {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.SUM, true);

		System.out.println(ar.toString());
		assertTrue(ar.getTuples().size() == 4);

	}

	@Test
	public void testGroupByAvg() throws IOException {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		System.out.println(ar.toString());
		ar = ar.aggregate(AggregateOperator.AVG, true);
		// System.out.println(ar.toString());
		System.out.println(ar.getTuples().get(1)); // should print out 530, its AVG is 4
		assertTrue(ar.getTuples().size() == 4);

	}

	@Test
	public void testGroupByCount() throws IOException {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		System.out.println(ar.toString());
		ar = ar.aggregate(AggregateOperator.COUNT, true);
		// System.out.println(ar.toString());
		System.out.println(ar.getTuples()); // should print out each group and their number of values
		assertTrue(ar.getTuples().size() == 4);

	}

	@Test
	public void testGroupByMax() throws IOException {// missing string test*********
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		System.out.println(ar.toString());
		ar = ar.aggregate(AggregateOperator.MAX, true);
		// System.out.println(ar.toString());
		System.out.println(ar.getTuples()); // should print out each group and their max value
		assertTrue(ar.getTuples().size() == 4);

	}

	@Test
	public void testGroupByMin() throws IOException {// missing string test*********
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		System.out.println(ar.toString());
		ar = ar.aggregate(AggregateOperator.MIN, true);
		// System.out.println(ar.toString());
		System.out.println(ar.getTuples()); // should print out each group and their main value
		assertTrue(ar.getTuples().size() == 4);

	}

}