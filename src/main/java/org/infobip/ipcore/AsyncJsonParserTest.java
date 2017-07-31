/**
 * 
 */
package org.infobip.ipcore;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mmimica.ajackson.AsyncJsonParser;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author mmimica
 *
 */
public class AsyncJsonParserTest {
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    @Data
    @FieldDefaults(level = AccessLevel.PRIVATE)
    private static class Model {
        String stringValue;
        int intValue;

        Model inner;
        Model[] innerArray;

        int[] intArray;
        Double nullable;
        boolean booleaValue;

    }

    private static final ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final Model model = Model.builder()
            .stringValue("hahaha")
            .intValue(2)
            .inner(Model.builder()
                    .stringValue("inner")
                    .intArray(new int[] { 5, 6, 7 })
                    .build())
            .innerArray(new Model[] {
                    Model.builder()
                    .stringValue("innerArray1")
                    .build(),
                    Model.builder()
                    .stringValue("innerArray2")
                    .build()
            })
            .intArray(new int[] { 2, 3, 4 })
            .nullable(null)
            .booleaValue(true)
            .build();

    @Test
    public void test() throws IOException {
        MutableBoolean parsed = new MutableBoolean(false);
        AsyncJsonParser parser = new AsyncJsonParser(root -> {
            parsed.setValue(true);
            try {
                Assert.assertEquals(mapper.treeToValue(root, Model.class), model);
            } catch (JsonProcessingException e) {
                Assert.fail(e.getMessage());
            }
        });

        for (byte b : new ObjectMapper().writeValueAsBytes(model)) {
            parser.consume(new byte[] { b }, 1);
        }

        Assert.assertTrue(parsed.booleanValue());
    }
    
    @Test
    public void testSequence() throws IOException {
        MutableInt parsed = new MutableInt(0);
        
        AsyncJsonParser parser = new AsyncJsonParser(root -> {
            parsed.increment();
            try {
                Model deserialized = mapper.treeToValue(root, Model.class);
                Assert.assertEquals(deserialized, model);
            } catch (JsonProcessingException e) {
                Assert.fail(e.getMessage());
            }
        });

        for (byte b : new ObjectMapper().writeValueAsBytes(model)) {
            parser.consume(new byte[] { b }, 1);
        }
        for (byte b : new ObjectMapper().writeValueAsBytes(model)) {
            parser.consume(new byte[] { b }, 1);
        }
        for (byte b : new ObjectMapper().writeValueAsBytes(model)) {
            parser.consume(new byte[] { b }, 1);
        }

        Assert.assertEquals(3, parsed.intValue());
    }
    
    @Test
    public void testRootArray() throws IOException {
        MutableInt parsed = new MutableInt(0);
        
        AsyncJsonParser parser = new AsyncJsonParser(root -> {
            parsed.increment();
            try {
                Model[] deserialized = mapper.treeToValue(root, Model[].class);
                System.out.println(Arrays.toString(deserialized));
                Assert.assertEquals(deserialized[0], model);
                Assert.assertEquals(deserialized[1], model);
                Assert.assertEquals(deserialized[2], model);
            } catch (JsonProcessingException e) {
                Assert.fail(e.getMessage());
            }
        });

        parser.consume("[".getBytes());
        for (byte b : new ObjectMapper().writeValueAsBytes(model)) {
            parser.consume(new byte[] { b });
        }
        parser.consume(",".getBytes());
        for (byte b : new ObjectMapper().writeValueAsBytes(model)) {
            parser.consume(new byte[] { b });
        }
        parser.consume(",".getBytes());
        for (byte b : new ObjectMapper().writeValueAsBytes(model)) {
            parser.consume(new byte[] { b });
        }
        parser.consume("]".getBytes());

        Assert.assertEquals(1, parsed.intValue());
    }
}
