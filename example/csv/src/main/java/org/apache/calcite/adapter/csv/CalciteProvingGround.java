package org.apache.calcite.adapter.csv;

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.*;
import java.util.*;

@SuppressWarnings("unchecked")
public class CalciteProvingGround {

  private static void print(Object s) {
    System.out.println(s.toString());
  }

  public static void main(String[] args) throws Exception {
    basicJdbcUsage();
  }

  public static void basicJdbcUsage() throws Exception {
    final String url = "jdbc:calcite:model='inline:" + CALCITE_MODEL + "'";
    Connection c = DriverManager.getConnection(url);
    CalciteConnection calciteConnection = c.unwrap(CalciteConnection.class);
    Statement statement = calciteConnection.createStatement();
    String inValues = getInValues(calciteConnection, "select\n" +
        "\tdd.\"calendar_first_day_of_month\" as column_axis\n" +
        "from \"example_fact_table_transaction_date\" fact\n" +
        "right outer join \"example_coa_dimension\" coa\n" +
        "  on coa.\"account_number\" = fact.\"account_number\"\n" +
        "join \"date_dimension_one\" dd\n" +
        "  on dd.\"calendar_date_actual\" = fact.\"transaction_date\"\n" +
        "group by dd.\"calendar_first_day_of_month\"");
    ResultSet resultSet = statement.executeQuery(
        "select * from (select\n" +
            "  fact.\"amount\" as measure_amount,\n" +
            "  fact.\"account_number\" as measure_count,\n" +
            "  coa.\"account_type\" as row_axis,\n" +
            "  dd.\"calendar_first_day_of_month\" as column_axis\n" +
            "from \"example_fact_table_transaction_date\" fact\n" +
            "right outer join \"example_coa_dimension\" coa\n" +
            "  on coa.\"account_number\" = fact.\"account_number\"\n" +
            "left join \"date_dimension_one\" dd\n" +
            "  on dd.\"calendar_date_actual\" = fact.\"transaction_date\"" +
            ")" +
            "pivot (" +
            " sum(\"MEASURE_AMOUNT\") as measure_amount," +
            " count(\"MEASURE_COUNT\") as measure_count" +
            " for (\"COLUMN_AXIS\")" +
            " in ("+ inValues +")" +
            ")"
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

  private static String getInValues(CalciteConnection connection, String query) throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    List<String> rows = new ArrayList<>();
    while (resultSet.next()) {
      List<String> row = new ArrayList<>();
      for (int count = 1; count <= resultSet.getMetaData().getColumnCount(); count++) {
        row.add("'" + resultSet.getString(count) + "'");
      }
      rows.add("("+String.join(", ", row)+")");
    }
    return String.join(", ", rows);
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
