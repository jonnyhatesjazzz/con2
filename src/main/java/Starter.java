import ekn.converter.DisctionaryMaker;

public class Starter {
    public static void main(String[] args) {
        DisctionaryMaker c = new DisctionaryMaker() ;
          c.createTables();
          c.parseLinks();
          c.parseSentensesToDictionary();

          //c.selectRandomPhrase();
          //c.makeOneDictionary();
    }



}
