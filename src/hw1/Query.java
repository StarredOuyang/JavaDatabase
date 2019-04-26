package hw1;

import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;

	public Query(String q) {
		this.q = q;
	}

	public Relation execute() throws IOException {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect) selectStatement.getSelectBody();

		Table x = (Table) sb.getFromItem();
		String name = x.getName();

		// setup just to get table from catalog
		int tableid = Database.getCatalog().getTableId(name);
		HeapFile table = Database.getCatalog().getDbFile(tableid);
		TupleDesc td = Database.getCatalog().getTupleDesc(tableid);

		ArrayList<Tuple> tuples = table.getAllTuples();

		Relation relation = new Relation(tuples, td);
		// System.out.println(relation.toString());

		// Join
		WhereExpressionVisitor ev = new WhereExpressionVisitor();
		ArrayList<Join> joins = (ArrayList<Join>) sb.getJoins();
		if (joins != null) {
			for (Join item : sb.getJoins()) {

				item.getOnExpression().accept(ev);

				// get 'other' table for Relation.join(...)
				int tableidother = Database.getCatalog().getTableId(item.getRightItem().toString());
				HeapFile tableother = Database.getCatalog().getDbFile(tableidother);
				TupleDesc tdother = Database.getCatalog().getTupleDesc(tableidother);
				ArrayList<Tuple> tuplesother = tableother.getAllTuples();
				// for(Tuple t : tuples)
				Relation other = new Relation(tuplesother, tdother);
				// get field1 and field 2 params for join

				String[] otherstr = ev.getRight().toString().split("\\.");
				ArrayList<String> fieldList = new ArrayList<String>();
				int field1 = 0;// could be field 2
				int field2 = 1;
				for (int i = 0; i < td.numFields(); i++) { // join condition flip works
					fieldList.add(td.getFieldName(i));
				}

				if (fieldList.contains(otherstr[1])) {
					field1 = td.nameToId(otherstr[1]);
					field2 = tdother.nameToId(ev.getLeft());
				} else {
					field1 = td.nameToId(ev.getLeft());
					field2 = tdother.nameToId(otherstr[1]);
				}

				relation = relation.join(other, field1, field2);
			} // how to parse join
		}

		// Where
		if (sb.getWhere() != null) {
			WhereExpressionVisitor wv = new WhereExpressionVisitor();

			sb.getWhere().accept(wv);

			int field = td.nameToId(wv.getLeft()); // int id for field
			Field constfield; // have to account for string or int.

			relation = relation.select(field, wv.getOp(), wv.getRight());

		}
		// aggregate info
		ColumnVisitor cv = new ColumnVisitor();
		ArrayList<Integer> rename_fields = new ArrayList<>();
		ArrayList<String> rename_names = new ArrayList<>();

		if (sb.getSelectItems() != null) {

			if (sb.getSelectItems().get(0).toString() == "*") {// select all
				int tableidother = Database.getCatalog().getTableId(sb.getFromItem().toString());
				HeapFile tableother = Database.getCatalog().getDbFile(tableidother);
				TupleDesc allDesc = Database.getCatalog().getTupleDesc(tableidother);
				ArrayList<Tuple> allTuple = tableother.getAllTuples();
				relation = new Relation(allTuple, allDesc);

			} else {
				for (SelectItem agg : sb.getSelectItems()) {
					agg.accept(cv);

					SelectExpressionItem exitem = (SelectExpressionItem) agg;
					exitem.accept(cv);

					// alias
					if (exitem.getAlias() != null) {
						System.out.println("Alias: " + exitem.getAlias().getName());
						rename_names.add(exitem.getAlias().getName());
						rename_fields.add(td.nameToId(cv.getColumn()));

					}
				}

			}
			// project
			if (joins == null && sb.getGroupByColumnReferences() == null && sb.getWhere() == null
					&& cv.isAggregate() == false && sb.getSelectItems().get(0).toString() != "*") {
				ArrayList<Integer> c = new ArrayList<>();
				for (SelectItem agg : sb.getSelectItems()) {
					agg.accept(cv);
					c.add(td.nameToId(cv.getColumn()));
					System.out.println(cv.getColumn());

				}
				relation = relation.project(c);
			}

			if (cv.isAggregate())

			{
				if (sb.getGroupByColumnReferences() != null) { // then grouping exists
					for (Expression ex : sb.getGroupByColumnReferences()) {
						Column col = new Column(x, ex.toString());

						// this is the 'grouping' column. look at the grouping test in
						// relation.
						relation = relation.aggregate(cv.getOp(), true);

					}
				} else {// here, if no grouping, continue aggregate and pass just
						// operator and false.
						// will project like done in Aggregate test in relation

					ArrayList<Integer> c = new ArrayList<Integer>();
					c.add(td.nameToId(cv.getColumn()));
					relation = relation.project(c);
					relation = relation.aggregate(cv.getOp(), false);
				}
			}

		}
		if (!rename_names.isEmpty())

		{
			System.out.println("alias added");
			relation = relation.rename(rename_fields, rename_names);
			System.out.println(relation.toString());
		}

		return relation;

	}
}