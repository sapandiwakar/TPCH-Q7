package relations;

public class Relation {
	public Schema schema;
	public String storageFileName;
	public String name = "";

	public Relation(Schema schema, String fileName) {
		super();
		this.schema = schema;
		this.storageFileName = fileName;
	}
	
	public Relation(Schema schema, String fileName, String name) {
		super();
		this.schema = schema;
		this.storageFileName = fileName;
		this.name = name;
	}

}
