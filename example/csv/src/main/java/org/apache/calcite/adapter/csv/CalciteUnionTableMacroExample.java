package org.apache.calcite.adapter.csv;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.util.NlsString;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked deprecated")
public class CalciteUnionTableMacroExample {

  private static SchemaPlus rootSchema;

  public static final Method UNION_MACRO =
      org.apache.calcite.linq4j.tree.Types.lookupMethod(CalciteUnionTableMacroExample.class, "withLimit", Object.class,
          Object.class);

  private static void print(Object s) {
    System.out.println(s.toString());
  }

  public static void main(String[] args) throws Exception {
//    unionMacroExample();
    pivotUsage();
  }

  public static TranslatableTable withLimit(Object table1, Object table2) {
    if (table1 instanceof NlsString && table2 instanceof NlsString) {
      String sql = "select 1 from \"MOCK_WAREHOUSE\".\""+((NlsString) table1).getValue()+"\" union all select 1 from " +
          "\"MOCK_WAREHOUSE\".\""+((NlsString) table2).getValue()+"\"";
      List<String> viewPath = new ArrayList<String>();
      viewPath.add("a");
      return ViewTable.viewMacro(
          rootSchema,
          sql,
          new ArrayList<>(),
          viewPath,
          false)
          .apply(null);
    }
    return null;
  }

  /**
   * This example won't work in Calcite 1.26 due to the bug I helped fix that will ship in 1.27.
   * @throws Exception
   */
  public static void pivotUsage() throws Exception {
    final String url = "jdbc:calcite:model='inline:" + CALCITE_MODEL + "'";
    Connection c = DriverManager.getConnection(url);
    CalciteConnection calciteConnection = c.unwrap(CalciteConnection.class);
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select * from \"example_fact_table_transaction_date\"" +
            "pivot (" +
            " sum(\"amount\") as ss, count(*) as kount" +
            " for (\"transaction_date\")" +
            " in (('2022-12-29') as c10, ('2022-09-13') as m20)" +
            ")"
    );
    List<Map<String, String>> results = new ArrayList<Map<String, String>>();
    while (resultSet.next()) {
      Map<String, String> row = new HashMap<String, String>();
      print("row:");
      for (int count = 1; count <= resultSet.getMetaData().getColumnCount(); count++) {
        print(resultSet.getMetaData().getColumnLabel(count) + ":" + resultSet.getString(count));
        row.put(resultSet.getMetaData().getColumnLabel(count), resultSet.getString(count));
      }
      results.add(row);
    }
    print(results);
    resultSet.close();
    statement.close();
    c.close();
  }

  public static void unionMacroExample() throws Exception {
    /**
     * Be sure to setup the mock warehouse database locally before running this:
     * https://github.com/OpenGov/opengov/tree/master/source/scripts/mock-data-warehouse
     */
    final String url = "jdbc:calcite:model='inline:" + CALCITE_MODEL + "'";
    Connection c = DriverManager.getConnection(url);
    CalciteConnection calciteConnection = c.unwrap(CalciteConnection.class);
    Statement statement = calciteConnection.createStatement();
    rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    // add tables...
    rootSchema.add("union",
        TableMacroImpl.create(CalciteUnionTableMacroExample.UNION_MACRO));
    ResultSet resultSet = statement.executeQuery(
        "select count(*) from table(\"union\"('example_fact_table_transaction_date', " +
            "'example_fact_table_transaction_date'))"
    );

    while (resultSet.next()) {
      print("{");
      for (int count = 1; count <= resultSet.getMetaData().getColumnCount(); count++) {
        print(" \"" + resultSet.getMetaData().getColumnLabel(count) + "\":\"" + resultSet.getString(count) + "\"");
      }
      print("}");
    }
    resultSet.close();
    statement.close();
    c.close();
  }

  private static final String CALCITE_MODEL =  "{\n"
      + "  version: \"1.0\",\n"
      + "  defaultSchema: \"MOCK_WAREHOUSE\",\n"
      + "  schemas: [\n"
      + "     {\n"
      + "       type: \"jdbc\",\n"
      + "       name: \"MOCK_WAREHOUSE\",\n"
      + "       jdbcDriver: \"org.postgresql.Driver\",\n"
      + "       jdbcUrl: \"jdbc:postgresql://localhost:5432/mock_warehouse\",\n"
      + "       jdbcCatalog: null,\n"
      + "       jdbcSchema: null\n"
      + "     }, "
      + "     {\n"
      + "       name: \"adhoc\",\n"
      + "       autoLattice: \"true\"\n"
      + "     } "
      + "  ]\n"
      + "}";

}
