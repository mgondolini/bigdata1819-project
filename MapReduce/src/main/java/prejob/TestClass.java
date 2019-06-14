package prejob;

import com.cloudera.org.codehaus.jackson.JsonFactory;
import com.cloudera.org.codehaus.jackson.JsonParser;
import com.cloudera.org.codehaus.jackson.JsonToken;

import java.io.IOException;

public class TestClass {

    public static void main(String[] args) throws IOException {
        JsonFactory factory = new JsonFactory();

        String test = "{\"id\":\"1\",\"category\":\"Film & Animation\"}";

        JsonParser parser = factory.createJsonParser(test);

        while(!parser.isClosed()){
            JsonToken jsonToken = parser.nextToken();

            if(JsonToken.FIELD_NAME.equals(jsonToken)){
                String fieldName = parser.getCurrentName();
                System.out.println(fieldName);

                jsonToken = parser.nextToken();

                if("id".equals(fieldName)){
                    System.out.println(parser.getText());
                } else if ("category".equals(fieldName)){
                    System.out.println(parser.getText());
                }
            }
        }

    }
}
