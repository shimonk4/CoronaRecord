package corona;


import corona.data.DBConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Solution {

    public static void main() {
        dropTables();
        createTables();
        printTheCountriesName();
    }

    public static void printTheCountriesName() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;

        try {
            pstmt = connection.prepareStatement("SELECT country FROM corona " +
                    "WHERE total_cases < 11000 AND new_cases > 100 " +
                    "GROUP BY country HAVING AVG(new_cases) > 250");
            ResultSet results = pstmt.executeQuery();
            if(!results.next()) {
                results.close();
                return;
            }
            String ret = results.getString("country");
            System.out.println(ret + "\n");
            while(!results.next()) {
                ret = results.getString("country");
                System.out.println(ret + "\n");
            }

            results.close();
        } catch (SQLException e) {
            return;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                return;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                return;
            }
        }
    }

    public static void createTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("CREATE TABLE corona\n" +
                    "(\n" +
                    "    date DATE NOT NULL,\n" +
                    "    country text NOT NULL,\n" +
                    "    new_cases integer NOT NULL,\n" +
                    "    new_deaths integer NOT NULL,\n" +
                    "    total_cases integer NOT NULL,\n" +
                    "    total_deaths integer NOT NULL,\n" +
                    "    PRIMARY KEY (date, country),\n" +
                    "    CHECK (Lab_ID > 0)\n" +
                    ")");
            pstmt.execute();

        } catch (SQLException e) {

        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }

    }

    public static void clearTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {


            pstmt = connection.prepareStatement("DELETE FROM corona ");
            pstmt.execute();

        } catch (SQLException e) {

        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public static void dropTables() {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS corona CASCADE ");
            pstmt.execute();

        } catch (SQLException e) {
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }

    }

}



