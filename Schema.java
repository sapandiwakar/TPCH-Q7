import java.util.List;
import java.util.ArrayList;	// only for unittest
import org.apache.hadoop.io.Text;

/** Class for handling relation schemas.
 *
 * Doesn't store the actual relation data, but knows how to manipulate rows.
 */
class Schema {
    /** Create a schema from a list of column names.
     *
     * Our database doesn't care what _type_ of data is stored in columns; it's
     * all stored as strings. */
    public Schema (List<String> schema){
	_schema = schema;
    }
    
    /** Get the index of a column by column name. */
    public int columnIndex (String col){
	return _schema.indexOf(col);
    }
    
    /** Extract a value from column col (index starting at 0) of a row. */
    public Text getValue (Text row, int index){
	int startIndex = 0, endIndex = -1;
	for( int count = index; count >= 0; --count ){
	    startIndex = endIndex + 1;
	    
	    // find the next | character
	    endIndex = row.find("|", startIndex);
	    if( endIndex < 0 ){
		if( count == 0 )
		    // OK, we return the last item
		    endIndex = row.getLength();
		else
		    // error: our tuple doesn't have enough columns
		    // TODO: how, if at all, should we report?
		    return new Text();
	    }
	}
	
	Text result = new Text();
	result.set( row.getBytes(), startIndex, endIndex-startIndex );
	return result;
    }
    
    
    private List<String> _schema;
    
    
    // unit test
    public static void main(String[] args) throws Exception {
	Schema schema = new Schema( new ArrayList<String>(){{ add("col1"); add("col2"); }} );
	
	Text row = new Text( "data1|data2" );
	assert( schema.getValue( row, schema.columnIndex( "col1" ) ) == new Text("data1") );
	assert( schema.getValue( row, schema.columnIndex( "col2" ) ) == new Text("data2") );
    }
}
