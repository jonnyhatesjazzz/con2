package ekn.converter;

import ekn.converter.ekn.utils.logging.Logger;

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

    String createTableDictionary = "create table  Dictionary (id int primary key, lang varchar2(3) ,phrase varchar2(2048) )";
    String createTabledeutschDictionary = "create table deutschDictionary (id int primary key, phrase varchar2(2048) )";
    String createTablerussianDictionary = "create table russianDictionary (id int primary key, phrase varchar2(2048) )";
    String createTableLinks = "create table links (id1 int  , id2 int )";
    String createTablerusdeu = "create table rusdeudictionary (id int primary key, deutschphrase varchar2(2048), russianphrase varchar2(2048) )";

    String insertIntoDeutschDictionary = "insert into deutschDictionary values (?,?)";
    String insertIntoRussianDictionary = "insert into russianDictionary values (?,?)";
    String insertIntoDictionary = "insert into  Dictionary values (?,?,?)";

    String insertIntoLinks = "insert into links values (?,?)";

    String  deleteUnneededLinks = "delete from links where id1 = ? or id2 = ?";


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


    void removeLink (int LinkId){
        Logger.debug("IN:removeLink");
        try {
            PreparedStatement st = con.prepareStatement(deleteUnneededLinks);
            st.setInt(1, LinkId);
            st.setInt(2, LinkId);
            st.execute();
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Logger.debug("OUT:removeLink");
    }

    public void createTables() {
            Logger.debug("IN:createTables");
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
        Logger.debug("IN:createLinkRecord");
        try {
            PreparedStatement st = con.prepareStatement(insertIntoLinks);
            st.setInt(1, id1);
            st.setInt(2, id2);
            st.execute();
            st.close();
        } catch (SQLException e) {
            Logger.debug("EXCEPTION:createLinkRecord");
            e.printStackTrace();
        }
        Logger.debug("OUT:createLinkRecord");
    }

    public void createDictionaryRecord(int id1, String lang, String phrase) {
        try {
            PreparedStatement st = con.prepareStatement(insertIntoDictionary);
            st.setInt(1, id1);
            st.setString(2, lang);
            st.setString(3, phrase);
            st.execute();
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void parseSentensesToDictionary() {
        Logger.debug("IN:parseSentensesToDictionary");

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
                int id = Integer.parseInt(tk.nextToken().trim());
                String lang = tk.nextToken().trim();
                String phrase = tk.nextToken("\n");
                Logger.debug( "id: " + id + " lang:" + lang + " phrase:" + phrase);

                if (lang.equals("deu") || lang.equals("rus")) {

                    createDictionaryRecord(id, lang, phrase);
                    System.out.println(id + " " + lang + " " + phrase);

                }else{
                    // это не нужный нам элемент, удалим его и из LINKS
                    removeLink(id);
                }

            }


        });
        Logger.debug("OUT:parseSentensesToDictionary");
    }

/*
 Создает 2 разных словаря
 */
    public void parseSentensesForTwoSeparateDictionaries() {


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


    public void selectRandomPhrase() {
        try {
            String selectDeutschPhrase = "select id , phrase from deutschDictionary  ";
            String selectRussianPhrase = "select id , phrase from russianDictionary where id = ? ";
            String selectLinkedRecords = "select id1 , id2 from links where id1 = ?  ";

            String russianPhrase = null;
            String deutschPhrase = null;

            PreparedStatement st = con.prepareStatement(selectDeutschPhrase);
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                int indexDeutsch = rs.getInt(1);
                deutschPhrase = rs.getString(2);
                System.out.println(russianPhrase + "  ==  " + deutschPhrase + " " + indexDeutsch);

                PreparedStatement st1 = con.prepareStatement(selectLinkedRecords);
                st1.setInt(1, indexDeutsch);
                ResultSet rs1 = st1.executeQuery();
                // с 1 немецким предложением мы нашли много связей
                while (rs1.next()) {
                    int id2 = rs1.getInt("id2");
                    System.out.println("id2 найденное для фразы" + id2);
                    PreparedStatement st2 = con.prepareStatement(selectRussianPhrase);
                    st2.setInt(1, id2);
                    ResultSet rs2 = st2.executeQuery();

                    if (rs2.next()) {
                        russianPhrase = rs2.getString(2);
                        System.out.println(russianPhrase + "  ==  " + deutschPhrase);

                    } else {
                        st.close();
                        break;
                    }

                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void makeOneDictionary() {
        try {


            String selectPhrase = "select id , lang , phrase from Dictionary  ";
            String selectPhraseWhereId = "select id , lang , phrase from Dictionary  where id = ?";
            String selectLinkedRecords = "select id1 , id2 from links where id1 = ?  ";

            String russianPhrase = null;
            String deutschPhrase = null;

            PreparedStatement st1 = con.prepareStatement(selectPhrase);
            ResultSet rs1 = st1.executeQuery();

            while (rs1.next()) {
                // Chose linked IDs
                PreparedStatement st2 = con.prepareStatement(selectLinkedRecords);
                st2.setInt(1, rs1.getInt("id"));
                ResultSet rs2 = st2.executeQuery();

                //Looking for one of them in Dictionary
                while (rs2.next()) {
                    // Ищем если немецкая фраза в нашем словаре
                    PreparedStatement st3 = con.prepareStatement(selectPhraseWhereId);
                    st3.setInt(1, rs2.getInt("id2"));
                    ResultSet rs3 = st3.executeQuery();
                    if (rs3.next()) {
                        //Ура мы нашли подходящую немецкую фразу!
                        System.out.println(rs1.getString(3) + "  " + rs3.getString(3));
                        st3.close();
                        break;
                    } else {
                        //There are no records more than one
                        continue;
                    }
                }
                st2.close();
            }
            st1.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}