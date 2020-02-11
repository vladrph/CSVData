package edu.fgcu.dataengineering;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
// just to check if these are found

public class BookStoreDB {

  public static void main(String[] args) {

    String dbPath = "src/Data/BookStore.db";
    Connection conn = null;
    try {
      Class.forName("org.sqlite.JDBC");
      conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
      System.out.println(" Successful Connection to Database ");
      Gson gson = new Gson();
      JsonReader jread = new JsonReader(new FileReader("src/Data/authors.json"));
      AuthorParser[] authors = gson.fromJson(jread, AuthorParser[].class);
      System.out.println("Inserting  records into the table...");

      for (var element : authors) {
        System.out.println(element.getName() + " " + element.getEmail() + " " + element.getUrl());
        String name = element.getName();
        String email = element.getEmail();
        String url = element.getUrl();

        if (url.equals("")) {
          int x = 0;
          url = "NO URL FOUND FOR " + name + " Error " + x;
          x++;
        }

        String sql = "INSERT INTO author(author_name,author_email,author_url) VALUES(?,?,?)";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, name);
        pstmt.setString(2, email);
        pstmt.setString(3, url);
        pstmt.executeUpdate();


      }
      System.out.println("Inserted  records into the table...");
    } catch (ClassNotFoundException ex) {
      ex.printStackTrace();
    } catch (SQLException ex) {
      ex.printStackTrace();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } finally {
      try {
        if (conn != null) {
          conn.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
