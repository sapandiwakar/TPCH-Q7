import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import operators.groupby.GroupBy;
import operators.join.HadoopJoin;
import operators.selection.SelectionEntry;
import operators.selection.SelectionFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import relations.Relation;
import relations.Schema;

public class dbq7 extends Configured implements Tool {

  /** input relations */
  static Relation relSupplier, relNations, relLineItem, relCustomer, relOrder;

  /**
   * intermediate result relations: must be global because their schema is
   * initiated then join is specified
   */
  static Relation relImdN1Supplier, relImdN1SupplierLineItem, relImdN2Customer,
      relImdN2CustomerOrders, relImdJoinResult;

  /** result relation */
  static Relation relQueryResult;

  /** monitor interval */
  static int monitorInterval = 5000;

  /** force reduce side join */
  static boolean forceReduceSideJoin = false;

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new dbq7(), args);
    System.exit(res);
  }

  private int printUsage() {
    System.out.println("dbq7 -i <input> -o <output> <nation1> <nation2> "
        + "[-r] [-m <memoryjoin_threshold>]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {
    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-i".equals(args[i])) {
          i++;
          if (!args[i].endsWith("/")) {
            args[i] = args[i] + "/";
          }
          Relation.setInputPath(args[i]);
        } else if ("-o".equals(args[i])) {
          i++;
          if (!args[i].endsWith("/")) {
            args[i] = args[i] + "/";
          }
          Relation.setTmpPath(args[i] + "temp/");
          Relation.setOutputPath(args[i] + "out/");
        } else if ("-f".equals(args[i])) {
          forceReduceSideJoin = true;
        } else if ("-o".equals(args[i])) {
          i++;
          HadoopJoin.setMemoryBackedThreshold(Integer.parseInt(args[i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
        return printUsage();
      }
    }

    // schema config
    schemaConfig();

    // mMake sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " + other_args.size()
          + " instead of 2.");
      return printUsage();
    }

    // nation parammeters
    String nation1 = other_args.get(0);
    String nation2 = other_args.get(1);

    // Phase 1: Nation1 |><| Supplier and Nation2 |><| Customer
    Job n1_supp = new Job(nation1_Supplier(nation1, nation2));
    Job n2_cust = new Job(nation2_Customer(nation1, nation2));

    JobControl jobs = new JobControl("jbcntrl");
    jobs.addJob(n1_supp);
    jobs.addJob(n2_cust);

    Thread jobClient = new Thread(jobs);
    jobClient.start();

    while (!jobs.allFinished()) {
      jobStatus(jobs);
    }

    // Phase 2: N1Supp |><| LineItem and N2Cust |><| Orders
    Job n1_supp_lineitem = new Job(n1Supp_LineItem());
    Job n2_cust_orders = new Job(n2Cust_Orders());

    jobs.addJob(n1_supp_lineitem);
    jobs.addJob(n2_cust_orders);

    while (!jobs.allFinished()) {
      jobStatus(jobs);
    }

    // Phase 3: Final join and Group by
    Job finalJoin = new Job(finalJoin());
    Job groupOrder = new Job(groupBy(nation1, nation2),
        new ArrayList<Job>(Arrays.asList(finalJoin)));

    jobs.addJob(finalJoin);
    jobs.addJob(groupOrder);

    while (!jobs.allFinished()) {
      jobStatus(jobs);
    }

    jobs.stop();

    return 0;
  }

  public static void schemaConfig() {
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
    relImdN2CustomerOrders = new Relation("ImdN2CustomerOrders");
    relImdJoinResult = new Relation("ImdJoinResult");

    // Result file
    relQueryResult = new Relation("Result");
  }

  /** Nation1 |><| Supplier job configuration */
  public static JobConf nation1_Supplier(String nation1, String nation2) throws IOException {
    @SuppressWarnings("unchecked")
    List<SelectionEntry<String>> nationFilters = Arrays.asList(new SelectionEntry<String>("NAME",
        nation1, SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER + Schema.STRING
            + SelectionEntry.CTRL_DELIMITER + "="), new SelectionEntry<String>("NAME", nation2,
        SelectionFilter.OR + SelectionEntry.CTRL_DELIMITER + Schema.STRING
            + SelectionEntry.CTRL_DELIMITER + "="));

    String nation1Alias = "n1_";
    List<String> nation1Projection = Arrays.asList("NAME", nation1Alias);
    List<String> supplierProjection = Arrays.asList("SUPPKEY", "");

    return HadoopJoin.join(relNations, relSupplier, relImdN1Supplier, nationFilters, null,
        nation1Projection, supplierProjection, "NATIONKEY", forceReduceSideJoin);
  }

  /** N1Supp |><| LineItem job configuration */
  public static JobConf n1Supp_LineItem() throws IOException {
    @SuppressWarnings("unchecked")
    List<SelectionEntry<String>> shipdateFilters = Arrays.asList(new SelectionEntry<String>(
        "SHIPDATE", "1995-01-01", SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER + Schema.DATE
            + SelectionEntry.CTRL_DELIMITER + ">="), new SelectionEntry<String>("SHIPDATE",
        "1996-12-31", SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER + Schema.DATE
            + SelectionEntry.CTRL_DELIMITER + "<="));

    String nation1Alias = "n1_";
    List<String> imdNation1Projection = Arrays.asList(nation1Alias + "NAME", "");
    List<String> lineItemProjection = Arrays.asList("ORDERKEY", "EXTENDEDPRICE", "DISCOUNT",
        "SHIPDATE", "");

    return HadoopJoin.join(relImdN1Supplier, relLineItem, relImdN1SupplierLineItem, null,
        shipdateFilters, imdNation1Projection, lineItemProjection, "SUPPKEY", forceReduceSideJoin);
  }

  /** Nation2 |><| Customer job configuration */
  public static JobConf nation2_Customer(String nation1, String nation2) throws IOException {
    @SuppressWarnings("unchecked")
    List<SelectionEntry<String>> nationFilters = Arrays.asList(new SelectionEntry<String>("NAME",
        nation1, SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER + Schema.STRING
            + SelectionEntry.CTRL_DELIMITER + "="), new SelectionEntry<String>("NAME", nation2,
        SelectionFilter.OR + SelectionEntry.CTRL_DELIMITER + Schema.STRING
            + SelectionEntry.CTRL_DELIMITER + "="));

    String nation2Alias = "n2_";
    List<String> nation2Projection = Arrays.asList("NAME", nation2Alias);
    List<String> customerProjection = Arrays.asList("CUSTKEY", "");

    return HadoopJoin.join(relNations, relCustomer, relImdN2Customer, nationFilters, null,
        nation2Projection, customerProjection, "NATIONKEY", forceReduceSideJoin);
  }

  /** N2Cust |><| Orders job configuration */
  public static JobConf n2Cust_Orders() throws IOException {
    String nation2Alias = "n2_";
    List<String> imdNation2Projection = Arrays.asList(nation2Alias + "NAME", "");
    List<String> orderProjection = Arrays.asList("ORDERKEY", "");

    return HadoopJoin.join(relImdN2Customer, relOrder, relImdN2CustomerOrders, null, null,
        imdNation2Projection, orderProjection, "CUSTKEY", forceReduceSideJoin);
  }

  /** FinalJoin job configuration */
  public static JobConf finalJoin() throws IOException {
    String nation1Alias = "n1_";
    String nation2Alias = "n2_";
    List<String> imdN1LineItemProjection = Arrays.asList(nation1Alias + "NAME", "EXTENDEDPRICE",
        "DISCOUNT", "SHIPDATE", "");
    List<String> imdNation2Projection = Arrays.asList(nation2Alias + "NAME", "");

    return HadoopJoin.join(relImdN1SupplierLineItem, relImdN2CustomerOrders, relImdJoinResult,
        null, null, imdN1LineItemProjection, imdNation2Projection, "ORDERKEY", forceReduceSideJoin);
  }

  /** GroupBy job configuration */
  public static JobConf groupBy(String nation1, String nation2) throws IOException {
    String nation1Alias = "n1_";
    String nation2Alias = "n2_";
    List<String> groupByFields = Arrays.asList(nation1Alias + "NAME", nation2Alias + "NAME",
        "SHIPDATE");

    @SuppressWarnings("unchecked")
    List<SelectionEntry<String>> nationFilters = Arrays.asList(new SelectionEntry<String>(
        nation1Alias + "NAME", nation1, SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER
            + Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="), new SelectionEntry<String>(
        nation2Alias + "NAME", nation2, SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER
            + Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="), new SelectionEntry<String>(
        nation1Alias + "NAME", nation2, SelectionFilter.OR + SelectionEntry.CTRL_DELIMITER
            + Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="), new SelectionEntry<String>(
        nation2Alias + "NAME", nation1, SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER
            + Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="));

    return GroupBy.createJob(new Configuration(), relImdJoinResult, nationFilters, null,
        groupByFields, relQueryResult);
  }

  public static void jobStatus(JobControl jobs) {
    System.out.println("Jobs in waiting state: " + jobs.getWaitingJobs().size());
    System.out.println("Jobs in ready state: " + jobs.getReadyJobs().size());
    System.out.println("Jobs in running state: " + jobs.getRunningJobs().size());
    System.out.println("Jobs in success state: " + jobs.getSuccessfulJobs().size());
    System.out.println("Jobs in failed state: " + jobs.getFailedJobs().size());
    System.out.println("\n");

    try {
      Thread.sleep(monitorInterval);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
