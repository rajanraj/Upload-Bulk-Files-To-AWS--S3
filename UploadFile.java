package com.spring.s3fileuploader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
 
public class UploadFile {
	public static final String delimiter = ",";
	static String[] tempArr;
	//Getting csv file as input
	   public static void read(String csvFile) {
	      try {
	    	  
	         File filecsv = new File(csvFile);
	         //Reading csv file 
	         FileReader fr = new FileReader(filecsv);
	         BufferedReader br = new BufferedReader(fr);
	         String line = "";
	        // read line by line and splitting by comma
	         while((line = br.readLine()) != null) {
	            tempArr = line.split(delimiter);
	            //for(int i=0;i<tempArr.length;i++) {
	               //System.out.println(tempArr[0] );
	               
	               //System.out.println();
	              // System.out.println(tempArr[1] );
	               //System.out.println(tempArr[2] );
	               //System.out.println();
	            //Give your AWS S3 bucket name here
	              String bucketName = "kfcrmsource";
	   	        //File  file = new File("/Users/rajanshankar/Desktop/ReThink-Images/");
	   	        //file.list();
	   	         
	   	       
	   	   //feeding AWS S3 bucket folder structure 
	   	  String folderName = tempArr[1];
	   	  // feeding file name
	   	 String fileName = tempArr[2];
	   	       // for(int i =0;i<2;i++) {
	   	        	//String folderName = tempArr[1];
	   	        String key = folderName +"/" + fileName;
	   	         
	   	        S3Client client = S3Client.builder().build();
	   	         
	   	        PutObjectRequest request = PutObjectRequest.builder()
	   	                        .bucket(bucketName)
	   	                        .key(key)
	   	                        .build();
	   	        // feeding local file path '
	   	     String  filePath = tempArr[0];
	   	        client.putObject(request, RequestBody.fromFile(new File(filePath)));
	   	         
	   	        S3Waiter waiter = client.waiter();
	   	        HeadObjectRequest requestWait = HeadObjectRequest.builder().bucket(bucketName).key(key).build();
	   	         
	   	        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
	   	         
	   	        waiterResponse.matched().response().ifPresent(System.out::println);
	   	         
	   	        System.out.println("File " + fileName + " was uploaded.");    
	   	    }
	           //}
	           // System.out.println();
	        // }
	         br.close();
	         } catch(IOException ioe) {
	            ioe.printStackTrace();
	         }
	      
	     
	   }
    public static void main(String[] args) {
    	
    	// csv file to read
    	//Give your CSV file local path here
        String csvFile = "/Users/rajanshankar/Desktop/Property-rethink_migration-final.csv";
        UploadFile.read(csvFile);
       
    }
}