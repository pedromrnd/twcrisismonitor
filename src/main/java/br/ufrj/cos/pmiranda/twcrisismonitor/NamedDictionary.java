package br.ufrj.cos.pmiranda.twcrisismonitor;
import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class NamedDictionary implements Serializable {
        private String cat_name;
        private Set<String> cat_terms;

    public NamedDictionary(String JSONtext) throws IOException, ParseException {
        this.cat_terms = new HashSet<String>();

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(JSONtext);
        JSONObject jsonObject = (JSONObject) obj;
        this.cat_name = (String) jsonObject.get("cat_name");

        // loop array
        JSONArray jsonterms = (JSONArray) jsonObject.get("cat_terms");
        Iterator<String> iterator = jsonterms.iterator();
        while (iterator.hasNext()) {
            this.cat_terms.add(iterator.next().toLowerCase());
        }
    }

    public String getCat_name() {
        return cat_name;
    }

    public Set<String> getCat_terms() {
        return cat_terms;
    }
    public String[] getTermsArray() {
        return cat_terms.toArray(new String[cat_terms.size()]);
    }
}
