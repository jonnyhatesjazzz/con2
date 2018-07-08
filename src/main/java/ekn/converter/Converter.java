package ekn.converter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.StringTokenizer;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class DisctionaryMaker {
    public DisctionaryMaker() {
        initdb();
    }

    String createTableDictionary= "create table  Dictionary (id int primary key, lang varchar2(3) ,phrase varchar2(2048) )";
    String createTabledeutschDictionary= "create table deutschDictionary (id int primary key, phrase varchar2(2048) )";
    String createTablerussianDictionary = "create table russianDictionary (id int primary key, phrase varchar2(2048) )";
    String createTableLinks =  "create table links (id1 int  , id2 int )";
    String createTablerusdeu = "create table rusdeudictionary (id int primary key, deutschphrase varchar2(2048), russianphrase varchar2(2048) )";

    String insertIntoDeutschDictionary = "insert into deutschDictionary values (?,?)";
    String insertIntoRussianDictionary = "insert into russianDictionary values (?,?)";
    String insertIntoDictionary = "insert into  Dictionary values (?,?,?)";

    String insertIntoLinks = "insert into links values (?,?)";

    Connection con = null;

    private void initdb() {
        try {
            Class.forName("org.h2.Driver");
            con = DriverManager.getConnection("jdbc:h2:d:/test");
            con.setAutoCommit(true);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createTables() {
        try {
            con.createStatement().execute(createTableDictionary);
            con.createStatement().execute(createTabledeutschDictionary);
            con.createStatement().execute(createTablerussianDictionary);
            con.createStatement().execute(createTableLinks);
            con.createStatement().execute(createTablerusdeu);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createDeutschDictionaryRecord(int index, String phrase) {
        try {
            PreparedStatement st = con.prepareStatement(insertIntoDeutschDictionary);
            st.setInt(1, index);
            st.setString(2, phrase);
            st.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createRussianDictionaryRecord(int index, String phrase) {
        try {
            PreparedStatement st = con.prepareStatement(insertIntoRussianDictionary);
            st.setInt(1, index);
            st.setString(2, phrase);
            st.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createLinkRecord(int id1, int id2) {
        try {
            PreparedStatement st = con.prepareStatement(insertIntoLinks);
            st.setInt(1, id1);
            st.setInt(2, id2);
            st.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createDictionaryRecord(int id1, String lang, String phrase) {
        try {
            PreparedStatement st = con.prepareStatement(insertIntoDictionary);
            st.setInt(1, id1);
            st.setString(2, lang);
            st.setString(3, phrase);
            st.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void parseSentensesToDictionary() {


        String fileName = "D:/Projects/Tatoeba/sentences/sentences.csv";
        Stream<String> stream = null;
        try {
            stream = Files.lines(Paths.get(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        stream.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                StringTokenizer tk = new StringTokenizer(s);
                int number = Integer.parseInt(tk.nextToken().trim());
                String lang = tk.nextToken().trim();
                String phrase = tk.nextToken("\n");


                if (lang.equals("deu") || lang.equals("rus") ) {

                    createDictionaryRecord(number, lang , phrase);
                    System.out.println(number + " " + lang + " " + phrase);

                }

            }


        });

    }


    public void parseSentenses() {


        String fileName = "D:/Projects/Tatoeba/sentences/sentences.csv";
        Stream<String> stream = null;
        try {
            stream = Files.lines(Paths.get(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        stream.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                StringTokenizer tk = new StringTokenizer(s);
                int number = Integer.parseInt(tk.nextToken().trim());
                String lang = tk.nextToken().trim();
                String phrase = tk.nextToken("\n");


                if (lang.equals("deu")) {

                    createDeutschDictionaryRecord(number, phrase);
                    System.out.println(number + " " + lang + " " + phrase);

                } else if (lang.equals("rus")) {

                    createRussianDictionaryRecord(number, phrase);
                    System.out.println(number + " " + lang + " " + phrase);

                }

            }


        });

    }


    public void parseLinks() {

        String fileName = "D:/Projects/Tatoeba/links/links.csv";
        Stream<String> stream = null;
        try {
            stream = Files.lines(Paths.get(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        stream.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                StringTokenizer tk = new StringTokenizer(s);
                int number1 = Integer.parseInt(tk.nextToken().trim());
                int number2 = Integer.parseInt(tk.nextToken().trim());
                createLinkRecord(number1, number2);
            }
        });

    }


    public void selectRandomPhrase(){
        try {
            String selectDeutschPhrase = "select id , phrase from deutschDictionary  ";
            String selectRussianPhrase = "select id , phrase from russianDictionary where id = ? ";
            String selectLinkedRecords = "select id1 , id2 from links where id1 = ?  ";

            String russianPhrase = null;
            String deutschPhrase = null;

            PreparedStatement st = con.prepareStatement(selectDeutschPhrase);
            ResultSet rs = st.executeQuery();
            while ( rs.next() ){
                int indexDeutsch = rs.getInt(1);
                deutschPhrase = rs.getString(2);
                System.out.println( russianPhrase + "  ==  " + deutschPhrase + " "  + indexDeutsch);

                PreparedStatement st1 = con.prepareStatement(selectLinkedRecords);
                st1.setInt(1, indexDeutsch);
                ResultSet rs1 = st1.executeQuery();
                // с 1 немецким предложением мы нашли много связей
                while ( rs1.next()){
                    int id2 = rs1.getInt("id2");
                    System.out.println("id2 найденное для фразы" + id2 );
                    PreparedStatement st2 = con.prepareStatement(selectRussianPhrase);
                    st2.setInt(1, id2);
                    ResultSet rs2 = st2.executeQuery();

                    if ( rs2.next()  ){
                          russianPhrase = rs2.getString(2);
                          System.out.println( russianPhrase + "  ==  " + deutschPhrase );

                    }else{
                        st.close();
                        break;
                    }

                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}