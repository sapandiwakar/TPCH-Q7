import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import operators.join.SimpleJoin.ReduceSideJoin;
import operators.selection.DateSelectionFilter;
import operators.selection.SelectionEntry;
import operators.selection.SelectionFilter;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import relations.Relation;
import relations.Schema;

public class dbq7 {

  // TODO: More robust way of initializing schemas
  // TODO: fill in the fields
  static Schema schemaSupplier = new Schema(Schema.SUPPLIER_FIELDS);
  static Schema schemaNations = new Schema(Schema.NATIONS_FIELDS);
  static Schema schemaLineItem = new Schema(Schema.LINEITEM_FIELDS);
  static Schema schemaCustomer = new Schema(Schema.CUSTOMER_FIELDS);
  static Schema schemaOrder = new Schema(Schema.ORDERS_FIELDS);

  // TODO: this is a bit messy. TODO: join itself to return new schema

  // TODO: nice way of handling schema changes, including projections
  // TODO: smaller is first, the param is first in schema.join
  static Schema schemaN1Supplier = schemaSupplier.join(schemaNations);
  static Schema schemaImdN1Supplier_LineItem = schemaLineItem.join(schemaN1Supplier);
  static Schema schemaImdN2Customer = schemaCustomer.join(schemaNations);
  static Schema schemaImdN2CustomerOrders = schemaImdN2Customer.join(schemaOrder);
  static Schema schemaImdJoinResult = schemaImdN1Supplier_LineItem.join(schemaImdN2CustomerOrders);

  static String inputFilesPrefix = "/team8/input/";
  static String tmpFilesPrefix = "/team8/temp/";

  static String inSupplier = inputFilesPrefix + "supplier.tbl";
  static String inNations = inputFilesPrefix + "nation.tbl";
  static String inLineItem = inputFilesPrefix + "lineitem.tbl";
  static String inCustomer = inputFilesPrefix + "customer.tbl";
  static String inOrder = inputFilesPrefix + "orders.tbl";

  static Relation relSupplier = new Relation(schemaSupplier, inSupplier, "Supplier");
  static Relation relNations = new Relation(schemaNations, inNations, "Nation");
  static Relation relLineItem = new Relation(schemaLineItem, inLineItem, "LineItem");
  static Relation relCustomer = new Relation(schemaCustomer, inCustomer, "Customer");

  // Intermediate result files
  static Relation relImdN1Supplier = new Relation(schemaN1Supplier, tmpFilesPrefix + "N1Supplier");

  static Relation relImdN1SupplierLineItem = new Relation(schemaImdN1Supplier_LineItem,
      tmpFilesPrefix + "N1SupplierLineItem");

  static Relation relImdN2Customer = new Relation(schemaImdN2Customer, tmpFilesPrefix
      + "N2Customer");

  static Relation relImdN2CustomerOrders = new Relation(schemaImdN2Customer, tmpFilesPrefix
      + "N2CustomerOrders");

  static Relation relImdJoinResult = new Relation(schemaImdJoinResult, tmpFilesPrefix
      + "JoinResult");

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {

    JobControl jobs = buildJobs();
    jobs.run();
    System.exit(0);
  }

  public static JobControl buildJobs() throws IOException {
    // ======
    // Params
    String nation1 = "FRANCE";
    String nation2 = "GERMANY";
    // ======

    JobControl jbcntrl = new JobControl("jbcntrl");

    // ======
    // map-side join o(n1) |><| supplier
    // =========
    // TODO: this still will have to know about selections and projections

    JobConf job_n1_suppliers_conf = ReduceSideJoin.getConf(relSupplier, relNations, "NATIONKEY",
        relImdN1Supplier);
    // add selection: TODO: for now default selection type is OR
    @SuppressWarnings("unchecked")
    List<SelectionEntry<String>> nationFilters = Arrays.asList(new SelectionEntry<String>("NAME",
        nation1), new SelectionEntry<String>("NAME", nation2));

    SelectionFilter.addSelectionsToJob(job_n1_suppliers_conf, ReduceSideJoin.PREFIX_JOIN_SMALLER,
        nationFilters, relNations.schema);

    ControlledJob job_n1_suppliers = new ControlledJob(job_n1_suppliers_conf);
    job_n1_suppliers.setJobName("job_n1_suppliers");
    jbcntrl.addJob(job_n1_suppliers);

    System.out.println("Join schema:" + schemaN1Supplier.toString() + " ind of SUPPKEY="
        + schemaN1Supplier.getColumnIndex("SUPPKEY"));

    // ===============================================
    // map-side join LineItem with [o(n1) |><| supplier]
    // ==================================================
    JobConf job_n1_suppliers_lineitem_conf = ReduceSideJoin.getConf(relLineItem, relImdN1Supplier,
        "SUPPKEY", relImdN1SupplierLineItem);

    // enable the date filter by providing column index. TODO: clean up
    job_n1_suppliers_lineitem_conf.set(DateSelectionFilter.PARAM_DATEFILTER_PREFIX
        + relLineItem.name, relLineItem.schema.getColumnIndex("SHIPDATE") + "");

    ControlledJob job_n1_suppliers_lineitem = new ControlledJob(job_n1_suppliers_lineitem_conf);

    // set job dependencies
    job_n1_suppliers_lineitem.addDependingJob(job_n1_suppliers);
    jbcntrl.addJob(job_n1_suppliers_lineitem);

    // RIGHT - BRANCH

    // ======
    // map-side join o(n2) |><| Customer
    // =========
    // TODO: this still will have to know about selections and projections

    JobConf job_n2_customers_conf = ReduceSideJoin.getConf(relCustomer, relNations, "NATIONKEY",
        relImdN2Customer);
    SelectionFilter.addSelectionsToJob(job_n2_customers_conf, ReduceSideJoin.PREFIX_JOIN_SMALLER,
        nationFilters, relNations.schema);

    ControlledJob job_n2_customer = new ControlledJob(job_n2_customers_conf);
    jbcntrl.addJob(job_n2_customer);

    // ======
    // map-side join Order X [o(n2) |><| Customer]
    // =========
    // TODO: this still will have to know about selections and projections

    JobConf job_n2_customer_orders_conf = ReduceSideJoin.getConf(relCustomer, relNations,
        "NATIONKEY", relImdN2CustomerOrders);

    ControlledJob job_n2_customer_orders = new ControlledJob(job_n2_customer_orders_conf);
    job_n2_customer_orders.addDependingJob(job_n2_customer);
    jbcntrl.addJob(job_n2_customer_orders);

    // =============================
    // finally join the two huge branches
    // ========================

    JobConf final_join_conf = ReduceSideJoin.getConf(relImdN1SupplierLineItem, relImdN2Customer,
        "ORDERKEY", relImdJoinResult);

    // TODO: Add Filter
    // TODO: refactor selections
    ControlledJob job_final_join = new ControlledJob(final_join_conf);
    job_final_join.addDependingJob(job_n2_customer_orders);
    job_final_join.addDependingJob(job_n1_suppliers_lineitem);

    jbcntrl.addJob(job_final_join);

    // TODO: add group_by and aggregation
    // TODO: add final filtering
    // TODO: add projections and formulas

    /**
     * TODO: This filter we will have to enforce once more again after n2 and n1
     * was joined together!!! ( (n1.n_name = '[NATION1]' and n2.n_name =
     * '[NATION2]') or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]') )
     */

    return jbcntrl;

  }

}
