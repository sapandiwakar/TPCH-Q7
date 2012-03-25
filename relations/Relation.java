package relations;

public class Relation {

	/** default paths */
	static String inputPath = "/team8/input001/";
	static String tmpPath = "/team8/temp001/";
	static String outputPath = "/team8/output001/";

	/** relation schema */
	public Schema schema = null;
	
	/** data path */
	public String storageFileName;
	
	/** relation name */
	public String name = "";

	/** constructor for input relations only -- schema is provided explicitly */
	public Relation(Schema schema, String fileName) {
		super();
		this.schema = schema;
		this.storageFileName = inputPath + fileName;
		this.name = fileName;
	}
	
	/**
	 * constructor for intermediate relations only -- the schema will be specified
	 * inside join
	 */
	public Relation(String fileName) {
		super();
		this.storageFileName = tmpPath + fileName;
		this.name = fileName;
	}
	
	/** constructor for output relations */
	public Relation() {
		super();
		this.storageFileName = outputPath;
	}

	public static String getInputPath() {
		return inputPath;
	}

	public static void setInputPath(String inputFilesPrefix) {
		Relation.inputPath = inputFilesPrefix;
	}

	public static String getTmpPath() {
		return tmpPath;
	}

	public static void setTmpPath(String tmpFilesPrefix) {
		Relation.tmpPath = tmpFilesPrefix;
	}

	public static String getOutputPath() {
		return outputPath;
	}

	public static void setOutputPath(String outputPath) {
		Relation.outputPath = outputPath;
	}
}
