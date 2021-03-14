package software.amazon.awssdk.demo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.StartJobRunRequest;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        
        try {
            File myObj = new File("/tmp/filename.txt");
            if (myObj.createNewFile()) {
              System.out.println("File created: " + myObj.getName());
            } else {
              System.out.println("File already exists.");
            }
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
        
        try {
            FileWriter myWriter = new FileWriter("/tmp/filename.txt");
            myWriter.write("RANDOM TEXT!!!!!!!!!!!!!!!");
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }      
        
        String bucket_name = "yavula-dev";
        String file_path = "/tmp/filename.txt";
        Path path = FileSystems.getDefault().getPath("/tmp", "filename.txt");
        String key_name = "example.txt";
        
        System.out.format("Uploading %s to S3 bucket %s...\n", file_path, bucket_name);
            
        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucket_name)
                .key(key_name)
                .build();
        s3.putObject(putOb,
                RequestBody.fromFile(path));

        GlueClient glueClient = GlueClient.builder()
                .region(region)
                .build();
        StartJobRunRequest jobRunRequest = StartJobRunRequest.builder()
        		.jobName("kite-pharma-glue-database-cataloging")
                .build();
        glueClient.startJobRun(jobRunRequest);
        
        return "Hello from Lambda!";
    }
}
   