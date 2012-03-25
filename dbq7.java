import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import operators.group.Group.GroupBy;
import operators.join.SimpleJoin.ReduceSideJoin;
import operators.selection.DateSelectionFilter;
import operators.selection.SelectionEntry;
import operators.selection.SelectionFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.jobcontrol.*;

import relations.Relation;
import relations.Schema;

public class dbq7 {

  // input Relations
  static Relation relSupplier, relNations, relLineItem, relCustomer, relOrder;

  // Intermediate result files
  static Relation relImdN1Supplier, relImdN1SupplierLineItem, relImdN2Customer,
      relImdN2CustomerOrders, relImdJoinResult;

  private static Relation relImdGroupByResult;

  static final boolean DEBUG = false;

  private static void initRelations() {
    // input Relations
    relSupplier = new Relation(new Schema(Schema.SUPPLIER_FIELDS), "supplier.tbl");
    relNations = new Relation(new Schema(Schema.NATIONS_FIELDS), "nation.tbl");
    relLineItem = new Relation(new Schema(Schema.LINEITEM_FIELDS), "lineitem.tbl");
    relCustomer = new Relation(new Schema(Schema.CUSTOMER_FIELDS), "customer.tbl");
    relOrder = new Relation(new Schema(Schema.ORDERS_FIELDS), "orders.tbl");

    // Intermediate result files
    relImdN1Supplier = new Relation("ImdN1Supplier");
    relImdN1SupplierLineItem = new Relation("ImdN1SupplierLineItem");
    relImdN2Customer = new Relation("ImdN2Customer");
    relImdN2CustomerOrders = new Relation("N2CustomerOrders");
    relImdJoinResult = new Relation("ImdJoinResult");

    relImdGroupByResult = new Relation("ImdResultGroupBy");

  }

  /**
   * @param args
   *          (input dir, nation1, nation2)
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    // ======
    // Params

    System.out.println("args:" + Arrays.asList(args));

    if (args.length > 0) {
      Relation.inputFilesPrefix = args[0];
      System.out.println("Using input path:" + Relation.inputFilesPrefix);
    }
    initRelations();

    String nation1 = "FRANCE";
    String nation2 = "GERMANY";

    if (args.length > 2) {
      nation1 = args[1];
      nation2 = args[2];
    }
    System.out.println("Nation1=" + nation1 + " NATION2=" + nation2);

    // ======

    long start = System.currentTimeMillis();

    JobControl jc = buildJobs(nation1, nation2);

    // based on:
    // http://blog.pfa-labs.com/2010/01/running-jobs-with-depenencies-in.html

    // start the controller in a different thread, no worries as it does that
    // anyway
    Thread theController = new Thread(jc);
    theController.start();

    // poll until everything is done,
    // in the meantime justs output some status message
    while (!jc.allFinished()) {
      System.out.println("Jobs in waiting state: " + jc.getWaitingJobs().size());
      System.out.println("Jobs in ready state: " + jc.getReadyJobs().size());
      System.out.println("Jobs in running state: " + jc.getRunningJobs().size());
      System.out.println("Jobs in success state: " + jc.getSuccessfulJobs().size());
      System.out.println("Jobs in failed state: " + jc.getFailedJobs().size());
      System.out.println("\n");

      printTimeElasped(start);

      // sleep 5 seconds
      try {
        Thread.sleep(5000);
      } catch (Exception e) {
      }
    }

    if (jc.allFinished())
      theController.stop();

    System.out.println("Done. ");
    printTimeElasped(start);

  }

  /**
   * @param start
   * @param jc
   */
  private static void printTimeElasped(long start) {
    // Get elapsed time in milliseconds
    long elapsedTimeMillis = System.currentTimeMillis() - start;
    // Get elapsed time in seconds
    float elapsedTimeMin = elapsedTimeMillis / (60 * 1000F);
    System.out.println("Time elapsed: " + elapsedTimeMin + " min.");
  }

  @SuppressWarnings("unchecked")
  private static List<SelectionEntry<String>> getWeakNationFilters(String nation1, String nation2) {
    return Arrays.asList(new SelectionEntry<String>("NAME", nation1), new SelectionEntry<String>(
        "NAME", nation2));
  }

  public static JobControl buildJobs(String nation1, String nation2) throws IOException {

    // TODO: maybe try compressing intermediate results? but here I guess
    // network is quote fast and disk too?.

    JobControl jbcntrl = new JobControl("jbcntrl");

    Job job_n1_suppliers_lineitem = get_jobs_N1_Supplier_Lineitem(nation1, nation2, jbcntrl);
    Job job_n2_customer_orders = get_jobs_N2_Customer_Order(jbcntrl, nation1, nation2);

    // =============================
    // finally join the two huge branches
    // ========================

    // TODO: Add Filter
    // TODO: refactor selections
    Job job_final_join = new Job(ReduceSideJoin.createJob(new Configuration(),
        relImdN1SupplierLineItem, relImdN2CustomerOrders, "ORDERKEY", relImdJoinResult),
        new ArrayList<Job>(Arrays.asList(job_n1_suppliers_lineitem, job_n2_customer_orders)));

    // TODO: add final filtering by both N1 and N2! : This could be inReduce
    // filter
    // TODO: add projections and simple column transformation formulas to make it work!!!

    if (DEBUG) {
      System.out.println("relImdN1SupplierLineItem schema: "
          + relImdN1SupplierLineItem.schema.toString());
      System.out.println("relImdN2CustomerOrders schema: "
          + relImdN2CustomerOrders.schema.toString());
      System.out.println("Final schema: " + relImdJoinResult.schema.toString());
    }
    jbcntrl.addJob(job_final_join);

    // TODO: add group_by and aggregation
    // TODO: project sum(volume) as revenue
    
    Job group_by = new Job(GroupBy.getGroupByConf(new Configuration(),
        relImdJoinResult.storageFileName, relImdJoinResult.schema.getColumnIndex("supp_nation"),
        relImdJoinResult.schema.getColumnIndex("cust_nation"),
        relImdJoinResult.schema.getColumnIndex("l_year"),
        relImdJoinResult.schema.getColumnIndex("volume"), relImdGroupByResult.storageFileName));
    group_by.addDependingJob(job_final_join);
    jbcntrl.addJob(group_by);

    /**
     * TODO: This filter we will have to enforce once more again after n2 and n1
     * was joined together!!! ( (n1.n_name = '[NATION1]' and n2.n_name =
     * '[NATION2]') or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]') )
     */

    return jbcntrl;

  }

  /**
   * @param nation1
   * @param nation2
   * @param jbcntrl
   * @return
   * @throws IOException
   */
  private static Job get_jobs_N1_Supplier_Lineitem(String nation1, String nation2,
      JobControl jbcntrl) throws IOException {

    // ======
    // map-side join o(n1) |><| supplier
    // =========
    // TODO: this still will have to know about selections and projections

    // add selection: TODO: for now default selection type is OR

    List<SelectionEntry<String>> nationFilters = getWeakNationFilters(nation1, nation2);
    Configuration job_n1_suppliers_conf = SelectionFilter.addSelectionsToJob(new Configuration(),
        ReduceSideJoin.PREFIX_JOIN_SMALLER, nationFilters, relNations.schema);

    Job job_n1_suppliers = new Job(ReduceSideJoin.createJob(job_n1_suppliers_conf, relSupplier,
        relNations, "NATIONKEY", relImdN1Supplier), new ArrayList<Job>());
    jbcntrl.addJob(job_n1_suppliers);

    // ===============================================
    // map-side join LineItem with [o(n1) |><| supplier]
    // ==================================================
    Configuration job_n1_suppliers_lineitem_conf = DateSelectionFilter.addSelection(
        new Configuration(), relLineItem, "SHIPDATE");

    Job job_n1_suppliers_lineitem = new Job(ReduceSideJoin.createJob(
        job_n1_suppliers_lineitem_conf, relLineItem, relImdN1Supplier, "SUPPKEY",
        relImdN1SupplierLineItem), new ArrayList<Job>(Arrays.asList(job_n1_suppliers)));
    // TODO: it is failing here!!!
    jbcntrl.addJob(job_n1_suppliers_lineitem);
    return job_n1_suppliers_lineitem;
  }

  /**
   * @param jbcntrl
   * @param nationFilters
   * @return
   * @throws IOException
   */
  private static Job get_jobs_N2_Customer_Order(JobControl jbcntrl, String nation1, String nation2)
      throws IOException {

    // ======
    // map-side join o(n2) |><| Customer
    // =========
    // TODO: this still will have to know about selections and projections

    List<SelectionEntry<String>> nationFilters = getWeakNationFilters(nation1, nation2);
    Configuration job_n2_customers_conf = SelectionFilter.addSelectionsToJob(new Configuration(),
        ReduceSideJoin.PREFIX_JOIN_SMALLER, nationFilters, relNations.schema);

    Job job_n2_customer = new Job(ReduceSideJoin.createJob(job_n2_customers_conf, relCustomer,
        relNations, "NATIONKEY", relImdN2Customer), new ArrayList<Job>());
    jbcntrl.addJob(job_n2_customer);

    // ======
    // map-side join Order X [o(n2) |><| Customer]
    // =========
    // TODO: this still will have to know about selections and projections

    Configuration job_n2_customer_orders_conf = new Configuration();

    Job job_n2_customer_orders = new Job(ReduceSideJoin.createJob(job_n2_customer_orders_conf,
        relOrder, relImdN2Customer, "CUSTKEY", relImdN2CustomerOrders), new ArrayList<Job>(
        Arrays.asList(job_n2_customer)));

    jbcntrl.addJob(job_n2_customer_orders);
    return job_n2_customer_orders;
  }

}
