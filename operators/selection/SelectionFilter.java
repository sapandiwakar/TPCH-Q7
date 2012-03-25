package operators.selection;

import java.util.ArrayList;
import java.util.List;

import relations.Schema;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

public class SelectionFilter implements SelectionFilterInterface {

  private static final String CONF_KEY_NCOLS = "selection_NCols";
  private static final String CONF_KEY_N_FILTER_GROUPS = "selection_ngroups_";
  private static final String CONF_GROUP_PREFIX = "_group_";

  /* e.g. AND */
  private static final String CONF_GROUP_OPERATOR = "_groupop_";
  /* e.g. OR */
  private static final String CONF_GROUP_INTRA_OPERATOR = "_groupopintra_";

  private static final String CONF_KEY_VALUES = "selection_values";
  private static final String CONF_KEY_COLUMNS = "selection_cols";

  ArrayList<SelectionEntry<Integer>> selectionArguments;
  ArrayList<ArrayList<SelectionEntry<Integer>>> selectionArgumentsGroup;


  String filterType = "OR";
  String innerFilterType = "AND";
  

  // TODO: we need relation name too!!!

  /**
   * very simple selection implementation that checks for equality. TODO: add
   * ORs etc
   * 
   * @param tuple
   * @return
   */

  public boolean checkSelection(String[] tuple) {
    
    if ("OR".equals(filterType)) {
      for (Entry<Integer, String> entry : selectionArguments) {

        // System.out.println(tuple[entry.getKey()] + " vs " + entry.getValue()
        // + " tuple: " + new ArrayList<String>(Arrays.asList(tuple)));

        if (tuple[entry.getKey()].equals(entry.getValue()))
          return true;
      }
    }
    if ("AND".equals(filterType)) {
      for (Entry<Integer, String> entry : selectionArguments) {
        if (!tuple[entry.getKey()].equals(entry.getValue()))
          return false;
      }
      return true;
    }
    return false || selectionArguments.size() == 0;
  }

  /**
   * Constructor given Configuration. as join have multiple relations we add
   * relationPrefix field
   * 
   */
  public SelectionFilter(Configuration conf, String relationPrefix) {
    super();

    int nSelectionCols = conf.getInt(relationPrefix + CONF_KEY_NCOLS, 0);
    String[] selectionValues = conf.getStrings(relationPrefix + CONF_KEY_VALUES, new String[] {});
    String[] sSelectionCols = conf.getStrings(relationPrefix + CONF_KEY_COLUMNS, new String[] {});

    selectionArguments = new ArrayList<SelectionEntry<Integer>>();

    // System.out.println("SelectionFilter n=" + nSelectionCols);

    for (int i = 0; i < nSelectionCols; i++) {
      System.out.println("SelectionFilter key=" + sSelectionCols[i] + "--> " + selectionValues[i]);

      selectionArguments.add(new SelectionEntry<Integer>(Integer.parseInt(sSelectionCols[i]),
          selectionValues[i]));
    }
  }

  /**
   * TODO: Combined filters save these conf params:
   * 
   * n_filter_groups = 2 groupOperator = AND intraGroupOperator = OR
   * 
   * n_ncols relation_prefix (for map - input name); reduce - output name
   * 
   * @param conf
   * @param relationPrefix
   * @param nationFilters
   * @param schema
   * @return
   */

  // TODO: shall use relation name instead
  public static Configuration addMultipleSelectionsToJob(Configuration conf, String relationPrefix,
      List<List<SelectionEntry<String>>> filterGroups, String groupOperator,
      String intraGroupOperator, Schema schema) {

    conf.setInt(relationPrefix + CONF_KEY_N_FILTER_GROUPS, filterGroups.size());
    
    //conf.set(relationPrefix + CONF_GROUP_OPERATOR, groupOperator);
    //conf.set(relationPrefix + CONF_GROUP_INTRA_OPERATOR, intraGroupOperator);

    int i = 0;
    for (List<SelectionEntry<String>> filterGroup : filterGroups) {
      String groupPrefix = CONF_GROUP_PREFIX + i++;

      conf.setInt(relationPrefix + groupPrefix + CONF_KEY_NCOLS, filterGroup.size());

      System.out.println("SelectionFilterCreate [] n=" + filterGroup.size());

      // using schema convert column names into their indexes
      List<String> columnIndexes = new ArrayList<String>();
      List<String> values = new ArrayList<String>();

      for (SelectionEntry<String> entry : filterGroup) {
        columnIndexes.add(schema.getColumnIndex(entry.getKey()) + "");
        values.add(entry.getValue());

      }
      conf.setStrings(relationPrefix + groupPrefix + CONF_KEY_COLUMNS,
          columnIndexes.toArray(new String[0]));
      conf.setStrings(relationPrefix + groupPrefix + CONF_KEY_VALUES, values.toArray(new String[0]));
    }

    return conf;
  }

  // TODO: shall use relation name instead
  public static Configuration addSelectionsToJob(Configuration conf, String relationPrefix,
      List<SelectionEntry<String>> nationFilters, Schema schema) {
    conf.setInt(relationPrefix + CONF_KEY_NCOLS, nationFilters.size());

    System.out.println("SelectionFilterCreate n=" + nationFilters.size());

    // using schema convert column names into their indexes
    List<String> columnIndexes = new ArrayList<String>();
    List<String> values = new ArrayList<String>();

    for (SelectionEntry<String> entry : nationFilters) {
      columnIndexes.add(schema.getColumnIndex(entry.getKey()) + "");
      values.add(entry.getValue());

    }
    conf.setStrings(relationPrefix + CONF_KEY_COLUMNS, columnIndexes.toArray(new String[0]));
    conf.setStrings(relationPrefix + CONF_KEY_VALUES, values.toArray(new String[0]));

    return conf;
  }
}
