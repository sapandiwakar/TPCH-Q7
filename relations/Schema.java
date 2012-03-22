package relations;

import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;

/**
 * Class for handling relation schemas.
 * 
 * Doesn't store the actual relation relations, but knows how to manipulate
 * rows.
 */
public class Schema {
	// Constants

	public static List<String> NATIONS_FIELDS = Arrays.asList(
            "NATIONKEY", "NAME", "REGIONKEY", "COMMENT" );

	public static List<String> SUPPLIER_FIELDS = Arrays.asList(
            "SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE",
			"ACCTBAL", "COMMENT" );

	public static List<String> CUSTOMER_FIELDS = Arrays.asList(
                "CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE",
			"ACCTBAL", "MKTSEGMENT", "COMMENT" );

	public static List<String> LINEITEM_FIELDS = Arrays.asList(
                        "ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER",
			"QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG",
                        "LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE",
                        "SHIPINSTRUCT", "SHIPMODE", "COMMENT");
        
        public static List<String> ORDERS_FIELDS = Arrays.asList( "ORDERKEY",
            "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE", "ORDERPRIORITY",
            "CLERK", "SHIPPRIORITY", "COMMENT");

	/**
	 * Create a schema from a list of column names.
	 * 
	 * Our database doesn't care what _type_ of relations is stored in columns;
	 * it's all stored as strings.
	 */
	public Schema(List<String> schema) {
		_schema = schema;
	}

	/** Create a new schema as a projection of the current schema. */
	public Schema projection(List<Integer> columns) {
		ArrayList<String> proj = new ArrayList<String>(columns.size());
		for (Integer col : columns)
			proj.add(_schema.get(col));
		return new Schema(proj);
	}

	/**
	 * Create a new schema as a join of two existing schemas. Keep all columns,
	 * don't check for duplicates.
	 * 
	 * TODO: how do we distinguish which of relations comes first in the actual
	 * ReduceSideJoin operators
	 * 
	 * 
	 * TODO: how do we handle schemas after joining where doublicate fields
	 * exist (e.g. NAME from nation and supplier):
	 * 
	 * [NATIONKEY, NAME, REGIONKEY, COMMENT, SUPPKEY , NAME, ADDRESS, NATIONKEY,
	 * PHONE, ACCTBAL, COMMENT]
	 * 
	 * probably we wont need that soon, but just have in mind.
	 */
	public Schema join(Schema rhs) {
		ArrayList<String> j = new ArrayList<String>(_schema.size() + rhs._schema.size());
		j.addAll(_schema);
		j.addAll(rhs._schema);
		return new Schema(j);
	}

	/** Get the index of a column by column name. */
	public int getColumnIndex(String col) {
		return _schema.indexOf(col);
	}

	/** Convenience function: get a sorted list of column indices. */
	public List<Integer> getColumnIndices(List<String> colNames) {
		ArrayList<Integer> result = new ArrayList<Integer>(colNames.size());
		for (String col : colNames)
			result.add(getColumnIndex(col));
		Collections.sort(result);
		return result;
	}

	/** Extract a value from column col (index starting at 0) of a row. */
	public Text getValue(Text row, int index) {
		int startIndex = 0, endIndex = -1;
		for (int count = index; count >= 0; --count) {
			startIndex = endIndex + 1;

			// find the next | character
			endIndex = row.find("|", startIndex);
			if (endIndex < 0) {
				if (count == 0)
					// OK, we return the last item
					endIndex = row.getLength();
				else
					// error: our tuple doesn't have enough columns
					// TODO: how, if at all, should we report?
					return new Text();
			}
		}

		Text result = new Text();
		result.set(row.getBytes(), startIndex, endIndex - startIndex);
		return result;
	}

	/**
	 * Projection: return a Text item of the specified set of columns.
	 * 
	 * Indices in columns must be sorted.
	 */
	public Text rowProjection(Text row, List<Integer> columns) {
		Text result = new Text();

		boolean first = true;
		int lastCol = 0;
		int endIndex = -1;
		for (Integer column : columns) {
			int startIndex = 0;
			for (; lastCol <= column; ++lastCol) {
				startIndex = endIndex + 1;

				// find the next | character
				endIndex = row.find("|", startIndex);
				if (endIndex < 0) {
					if (lastCol == column)
						// OK, we return the last item
						endIndex = row.getLength();
					else {
						// error: our tuple doesn't have enough columns
						// TODO: how, if at all, should we report?
						endIndex = startIndex;
						break;
					}
				}
			}

			if (!first) {
				// some other entry came before, which means both that we want
				// to add a divider and that our input starts with a divider
				startIndex -= 1;
				if (startIndex < 0)
					// with the exception of some very weird bugs
					startIndex = 0;
			}
			result.append(row.getBytes(), startIndex, endIndex - startIndex);
			first = false;
		}

		return result;
	}

	/**
	 * Return the join on a row. Simple concatenation with separator; it is
	 * assumed that columns to be joined have been removed from at least one
	 * tuple.
	 */
	public Text rowJoin(Text row1, Text row2) {
		Text result = new Text(row1);
		byte[] sep = {'|'};
		result.append(sep, 0, 1);
		result.append(row2.getBytes(), 0, row2.getLength());
		return result;
	}

	private List<String> _schema;

	public String toString() {
		return "Schema: " + _schema.toString();
	}

	// unit test
	public static void main(String[] args) throws Exception {
		List<String> names123 = Arrays.asList( "col1", "col2", "col3" );
		List<String> names2 = Arrays.asList( "col2" );
		List<String> names13 = Arrays.asList( "col1", "col3" );
		List<String> names12313 = new ArrayList<String>(names123);
		names12313.addAll(names13);

		Schema schema = new Schema(names123);
		Schema schema13 = new Schema(names13);

		Text row = new Text("data1|data2|data3");
		assert (schema.getValue(row, schema.getColumnIndex("col1")) == new Text("data1"));
		assert (schema.getValue(row, schema.getColumnIndex("col3")) == new Text("data3"));

		List<Integer> indices = schema.getColumnIndices(names2);
		assert (schema.rowProjection(row, indices) == new Text("data2"));

		List<String> names31 = names13;
		Collections.reverse(names31);
		indices = schema.getColumnIndices(names31);
		assert (schema.rowProjection(row, indices) == new Text("data1|data3"));

		assert (schema.projection(indices) == schema13);

		assert (schema.join(schema13) == new Schema(names12313));

		assert (schema.rowJoin(row, new Text("d1|d3")) == new Text("data1|data2|data3|d1|d3"));
		System.out.println("All Tests OK");
	}
}
