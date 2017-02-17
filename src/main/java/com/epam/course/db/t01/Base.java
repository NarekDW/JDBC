package com.epam.course.db.t01;

import lombok.SneakyThrows;
import lombok.val;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Date: 16.02.2017
 *
 * @author Karapetyan N.K
 */
public class Base {
    private Connection connection;

    @SuppressWarnings("WeakerAccess")
    @SneakyThrows
    public Base(String dbProperties) {
        Properties properties = getProperties(dbProperties + "db.properties");
        loadDataBase(properties);
        String url = (String) properties.remove("url");
        connection = DriverManager.getConnection(url, properties);
        initDataBase(dbProperties, connection);
    }

    @SuppressWarnings("WeakerAccess")
    public List<Person> getAllFromDb() {
        List<Person> persons = new ArrayList<>();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = connection.createStatement();
            rs = st.executeQuery("SELECT * FROM PERSON");
            while (rs.next()){
                Person person = new Person();
                person.setId(rs.getInt(1))
                        .setFirstName(rs.getString(2))
                        .setLastName(rs.getString(3))
                        .setDateOfBirth(rs.getDate(4));
                persons.add(person);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeStatement(st);
            closeStatement(rs);
        }
        return persons;
    }

    @SuppressWarnings("unused")
    public Person selectByID(int id){
        PreparedStatement ps = null;
        ResultSet rs = null;
        Person person = null;
        try {
            ps = connection.prepareStatement("SELECT * FROM PERSON WHERE ID = ?");
            ps.setInt(1, id);
            rs = ps.executeQuery();
            person = new Person();
            while (rs.next()){
                person.setId(rs.getInt(1))
                        .setFirstName(rs.getString(2))
                        .setLastName(rs.getString(3))
                        .setDateOfBirth(rs.getDate(4));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeStatement(ps);
            closeStatement(rs);
        }
        return person;
    }

    @SuppressWarnings("WeakerAccess")
    public boolean updateByID(int id, String firstName, String lastName, Date date){
        PreparedStatement ps = null;
        try {
            ps  = connection.prepareStatement(
                    "UPDATE PERSON SET first_name=?, last_name=?, date_of_birth=? WHERE id = ?");
            ps.setInt(4, id);
            ps.setString(1, firstName);
            ps.setString(2, lastName);
            ps.setDate(3, date);
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        } finally {
            closeStatement(ps);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public boolean insertPerson(Date date, String... info){
        PreparedStatement ps = null;
        try {
            ps  = connection.prepareStatement(
                    "INSERT INTO PERSON (first_name, last_name, date_of_birth) VALUES (?,?,?)");
            ps.setString(1, info[0]);
            ps.setString(2, info[1]);
            ps.setDate(3, date);
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        } finally {
            closeStatement(ps);
        }
    }

    public boolean dropTable(String table){
        Statement s = null;
        try {
            s = connection.createStatement();
            s.executeUpdate("DROP TABLE "+table);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeStatement(s);
        }
        return true;
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean closeConnection(){
        return closeStatement(connection);
    }

    private boolean closeStatement(AutoCloseable a) {
        try {
            if(a!=null) a.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void initDataBase(String dbProperties, Connection connection) {
        for (int i = 1; new File(dbProperties + i + ".sql").exists(); i++){
            try (val br = new BufferedReader(
                    new FileReader(dbProperties + i + ".sql"))) {
                String str;
                String dbinit = "";
                while ((str = br.readLine()) != null) {
                    dbinit += str;
                }
                try(val stmt = connection.createStatement()){
                    stmt.executeUpdate(dbinit);
                }
            } catch (FileNotFoundException e) {
                System.err.println("File not found " + e);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                System.err.println("Data Base exception HERE" + e);
            }
        }
    }

    private static void loadDataBase(Properties properties) {
        try {
            Class.forName((String) properties.remove("driver"));
        } catch (ClassNotFoundException e) {
            System.err.println("Driver not found " + e);
        }
    }

    private static Properties getProperties(String dbProperties) throws IOException {
        val properties = new Properties();
        try (val inputStream = new FileInputStream(dbProperties)) {
            properties.load(inputStream);
        }
        return properties;
    }

    public static void main(String[] args) {
        Base  base = new Base("src/test/resources/");
    }
}
