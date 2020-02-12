package edu.fgcu.dataengineering;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
// just to check if these are found

public class BookStoreDB {

/*
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
        //String sql = "INSERT INTO author(author_name,author_email,author_url)  VALUES(?,?,?) SELECT author.author_name FROM author INNER JOIN author.author_name ON book.author_name" ;
        // SELECT author_name, FROM author, CROSS JOIN author_name
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


  }*/

  public static void authorUpdate() throws IOException {
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

  public static void bookUpdate() throws IOException {

    String dbPath = "src/Data/BookStore.db";
    Connection conn = null;
    try {
      Class.forName("org.sqlite.JDBC");

      conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);

      System.out.println(" Successful Connection to Database ");

      String book = "src/Data/bookstore_report2.csv";

      CSVReader temp = new CSVReader(new FileReader(book));

      String[] SEOExample;  // Creates ann array of values

      System.out.println("Inserting  records into the book table...");

      temp.readNext();
      while ((SEOExample = temp.readNext()) != null) {

        String isbn = SEOExample[0];
        String title = SEOExample[1];
        String author = SEOExample[2];
        String publisher = SEOExample[3];
        String location = SEOExample[4];

        String sql = "INSERT INTO book(isbn,publisher_name,author_name, book_title) VALUES(?,?,?,?)";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, isbn);
        pstmt.setString(2, publisher);
        pstmt.setString(3, author);
        pstmt.setString(4, title);
        // pstmt.setString(5, url);
        pstmt.executeUpdate();

        //} //end brace for for loop
      }  // end brace for while loop

      System.out.println("Inserting  records into the book table...");
    } catch (ClassNotFoundException ex) {
      ex.printStackTrace();
    } catch (SQLException ex) {
      ex.printStackTrace();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (CsvValidationException e) {
      e.printStackTrace();
    } catch (IOException e) {
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
