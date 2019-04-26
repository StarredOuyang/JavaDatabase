package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.Tuple;
import hw1.TupleDesc;

public class HeapFileTest {

	private HeapFile hf;
	private TupleDesc td;
	private Catalog c;

	@Before
	public void setup() {

		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}

		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");

		int tableId = c.getTableId("test");
		td = c.getTupleDesc(tableId);

		hf = c.getDbFile(tableId);
	}

	@Test
	public void testGetters() throws IOException {
		assertTrue(hf.getTupleDesc().equals(td));

		assertTrue(hf.getNumPages() == 1);
		assertTrue(hf.readPage(0) != null);
	}

	@Test
	public void testWrite() throws IOException {
		Tuple t = new Tuple(td);
		t.setField(0, new byte[] { 0, 0, 0, (byte) 131 });
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 98;
		s[2] = 121;
		t.setField(1, s);
		System.out.println("tuple overhere" + t.toString());
		System.out.println("SIZE: original" + hf.getAllTuples().size());
		try {
			hf.addTuple(t);

		} catch (Exception e) {
			e.printStackTrace();
			fail("unable to add valid tuple");
		}
		System.out.println("SIZE: after" + hf.getAllTuples().size());
		assertTrue(hf.getAllTuples().size() == 2);
	}

	@Test
	public void testWriteMultiplePages() throws Exception {

		for (int i = 0; i < 95; i++) {

			Tuple t = new Tuple(td);
			t.setField(0, new byte[] { 0, 0, 0, (byte) 131 });
			byte[] s = new byte[129];
			s[0] = 2;
			s[1] = 98;
			s[2] = 121;
			t.setField(1, s);

			// added a page counter rather than the file stuff.

			try {
				hf.addTuple(t);

				System.out.println(i + 1 + "th Tuple added");
				System.out.println("Page Num: " + hf.getNumPages());
				System.out.println("SIZE: " + hf.getAllTuples().size());
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Failure** " + hf.getNumPages());

				fail("unable to add valid tuple");
			}

		}

		System.out.println("SIZE***: " + hf.getAllTuples().size());
		assertTrue(hf.getAllTuples().size() == 96);
	}

	@Test
	public void testRemove() throws Exception {
		Tuple t = new Tuple(td);
		// t.setField(0, new byte[] {0, 0, 0, (byte)530});
		t.setField(0, new byte[] { 0, 0, 0x02, 0x12 });
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 0x68;
		s[2] = 0x69;
		t.setField(1, s);

		System.out.println("size " + hf.getAllTuples().size());
		try {
			hf.deleteTuple(t);
		} catch (Exception e) {
			e.printStackTrace();
			fail("unable to delete tuple");
		}

		assertTrue(hf.getAllTuples().size() == 0);
	}

}
