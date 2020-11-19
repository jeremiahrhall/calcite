package org.apache.calcite.adapter.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.*;

import java.sql.*;

@SuppressWarnings("unchecked")
public class CalciteInMemoryReflectionExample {
  final static ObjectMapper mapper = new ObjectMapper();

  private static void print(Object s) {
    System.out.println(s.toString());
  }

  public static class Employee {
    public String empid;
    public String deptno;

    public Employee(String empid, String deptno) {
      this.empid = empid;
      this.deptno = deptno;
    }
  }

  public static class Department {
    public String deptno;

    public Department(String deptno) {
      this.deptno = deptno;
    }
  }

  public static class HrSchema {
    public final Employee[] emps = {
        new Employee("Jeremiah", "Engineering"),
        new Employee("Pushkala", "Engineering"),
        new Employee("Deepak", "Engineering"),
        new Employee("Todd", "Engineering"),
        new Employee("Sonali", "Design"),
        new Employee("Richard", "Design"),
        new Employee("Aaron", "Design"),
    };
    public final Department[] depts = {
        new Department("Design"),
        new Department("Engineering"),
        new Department("Sales"),
    };
  }
  public static void main(String[] args) throws Exception {
    reflectionExample();
  }

  /**
   * The pivot bug is specific to JdbcAdapters so this works!
   * @throws Exception
   */
  public static void reflectionExample() throws Exception {
    final String url = "jdbc:calcite:";
    Connection connection = DriverManager.getConnection(url);
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select * from (" +
            "select * from \"hr\".\"emps\")" +
            "pivot (" +
            " count(\"empid\") as countEmployees" +
            " for (\"deptno\")" +
            " in (('Engineering'), ('Design'))" +
            ")");
    while (resultSet.next()) {
      print("{");
      for (int count = 1; count <= resultSet.getMetaData().getColumnCount(); count++) {
        print(" \"" + resultSet.getMetaData().getColumnLabel(count) + "\":\"" + resultSet.getString(count) + "\"");
      }
      print("}");
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

}
