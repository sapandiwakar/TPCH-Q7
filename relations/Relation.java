package relations;

public class Relation {

  public static String inputFilesPrefix = "/team8/input/";
  public static String tmpFilesPrefix = "/team8/temp/";

  public Schema schema = null;
  public String storageFileName;
  public String name = "";

  /**
   * constructor for input relations only -- schema is provided explicitly
   */
  public Relation(Schema schema, String fileName) {
    super();
    this.schema = schema;
    this.storageFileName = inputFilesPrefix + fileName;
    this.name = fileName;
  }

  /**
   * constructor for intermediate relations only -- the schema will be specified
   * inside join
   */
  public Relation(String fileName) {
    super();
    this.storageFileName = tmpFilesPrefix + fileName;
    this.name = fileName;
  }

}
