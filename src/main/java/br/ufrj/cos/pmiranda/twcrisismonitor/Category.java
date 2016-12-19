package br.ufrj.cos.pmiranda.twcrisismonitor;
import java.io.*;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.Tuple2;

public class Category implements Serializable {
	private String name;
    private Set<String> terms;
    private String email;
    private String language;
    public Double threshold;
    
    public Category(String filepath) throws IOException, ParseException {
        this.terms = new HashSet<String>();

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(filepath));
        JSONObject jsonObject = (JSONObject) obj;
        this.name = (String) jsonObject.get("name");
        this.email = (String) jsonObject.get("email");
        this.language =  (String) jsonObject.get("language");
        this.threshold =  (Double) jsonObject.get("threshold");
        

        // loop array
        JSONArray jsonterms = (JSONArray) jsonObject.get("terms");
        Iterator<String> iterator = jsonterms.iterator();
        while (iterator.hasNext()) {
            this.terms.add(iterator.next());
        }

    }

    public String getEmail() {
		return email;
	}

	public String getName() {
        return name;
    }
    public Double getThreshold() {
        return threshold;
    }


    public String getLanguage() {
        return language;
    }

    public Set<String> getTerms() {
        return terms;
    }
    
    public String[] getTermsArray() {
        return terms.toArray(new String[terms.size()]);
    }
}
