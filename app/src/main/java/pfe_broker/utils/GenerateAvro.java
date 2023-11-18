package pfe_broker.utils;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

public class GenerateAvro {
    public static void main(String[] args) {
        String outputDir = "src/main/java";

        // The order of the files is important
        File[] files = new File[] {
                new File("src/main/resources/avro/order-rejected-reason.avsc"),
                new File("src/main/resources/avro/side.avsc"),
                new File("src/main/resources/avro/order.avsc"),
                new File("src/main/resources/avro/trade.avsc"),
                new File("src/main/resources/avro/rejected-order.avsc"),
                new File("src/main/resources/avro/market-data.avsc"),
        };

        try {
            compileSchema(files, new File(outputDir));
        } catch (IOException e) {
            System.err.println("Error generating Avro schema");
            e.printStackTrace();
        }

    }

    public static void compileSchema(File[] srcFiles, File dest) throws IOException {
        Schema.Parser parser = new Schema.Parser();

        for (File src : srcFiles) {
            Schema schema = parser.parse(src);
            SpecificCompiler compiler = new SpecificCompiler(schema);
            compiler.compileToDestination(src, dest);
        }
    }

}
