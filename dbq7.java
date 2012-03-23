import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  static Relation relSupplier = new Relation(new Schema(Schema.SUPPLIER_FIELDS), "supplier.tbl");
  static Relation relNations = new Relation(new Schema(Schema.NATIONS_FIELDS), "nation.tbl");
  static Relation relLineItem = new Relation(new Schema(Schema.LINEITEM_FIELDS), "lineitem.tbl");
  static Relation relCustomer = new Relation(new Schema(Schema.CUSTOMER_FIELDS), "customer.tbl");
  static Relation relOrder = new Relation(new Schema(Schema.ORDERS_FIELDS), "orders.tbl");

  // Intermediate result files
  static Relation relImdN1Supplier = new Relation("ImdN1Supplier");
  static Relation relImdN1SupplierLineItem = new Relation("ImdN1SupplierLineItem");
  static Relation relImdN2Customer = new Relation("ImdN2Customer");
  static Relation relImdN2CustomerOrders = new Relation("N2CustomerOrders");
  static Relation relImdJoinResult = new Relation("ImdJoinResult");

  /**
   * @param args
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException {
    // ======
    // Params
    String nation1 = "FRANCE";
    String nation2 = "GERMANY";

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

  public static JobControl buildJobs(String nation1, String nation2) throws IOException {

    JobControl jbcntrl = new JobControl("jbcntrl");

    // ======
    // map-side join o(n1) |><| supplier
    // =========
    // TODO: this still will have to know about selections and projections

    // add selection: TODO: for now default selection type is OR
    @SuppressWarnings("unchecked")
    List<SelectionEntry<String>> nationFilters = Arrays.asList(new SelectionEntry<String>("NAME",
        nation1), new SelectionEntry<String>("NAME", nation2));

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

    // RIGHT - BRANCH

    Job job_n2_customer_orders = get_jobs_N2_Customer_Order(jbcntrl, nationFilters);

    // =============================
    // finally join the two huge branches
    // ========================

    // TODO: Add Filter
    // TODO: refactor selections
    Job job_final_join = new Job(ReduceSideJoin.createJob(new Configuration(),
        relImdN1SupplierLineItem, relImdN2CustomerOrders, "ORDERKEY", relImdJoinResult),
        new ArrayList<Job>(Arrays.asList(job_n1_suppliers_lineitem, job_n2_customer_orders)));
    System.out.println("relImdN1SupplierLineItem schema: "
        + relImdN1SupplierLineItem.schema.toString());
    System.out
        .println("relImdN2CustomerOrders schema: " + relImdN2CustomerOrders.schema.toString());

    System.out.println("Final schema: " + relImdJoinResult.schema.toString());

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

  /**
   * @param jbcntrl
   * @param nationFilters
   * @return
   * @throws IOException
   */
  private static Job get_jobs_N2_Customer_Order(JobControl jbcntrl,
      List<SelectionEntry<String>> nationFilters) throws IOException {
    // ======
    // map-side join o(n2) |><| Customer
    // =========
    // TODO: this still will have to know about selections and projections

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
