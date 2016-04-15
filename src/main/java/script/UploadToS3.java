package script;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class UploadToS3 {

    private static final ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);

    private static Instant maxAgeOfFilesToBeUploaded;
    private static File basePath;
    private static File pathToUpload;
    private static TransferManager transferManager;
    private static String awsBucketName;

    public static void main(String... args) throws IOException, InterruptedException {

        validateArgs(args);

        Integer daysAgo = Integer.parseInt(args[0]);
        String awsIdentifier = args[1];
        String awsPassword = args[2];
        awsBucketName = args[3];
        String basePathString = args[4];

        pathToUpload = getPathToUpload(basePathString, args);
        maxAgeOfFilesToBeUploaded = Instant.now().minus(daysAgo, ChronoUnit.DAYS);
        transferManager = new TransferManager(new AmazonS3Client(new BasicAWSCredentials(awsIdentifier, awsPassword)));

        executorService.submit(executeOneDirectory(pathToUpload));

        while (!noWaitingExecution()) {
            Thread.sleep(1000);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    private static File getPathToUpload(String basePathString, String[] args) {
        basePath = new File(basePathString);
        if (args.length == 6) {
            String subfolder = args[5].replaceAll("^/", "").replaceAll("/$", "");
            return new File(basePath, subfolder);
        } else {
            return basePath;
        }
    }

    private static void validateArgs(String[] args) {
        if (args.length != 5 && args.length != 6) {

            log("You must have 5 or 6 params : ");
            log("daysAgo (Integer)");
            log("awsIdentifier");
            log("awsPassword");
            log("awsBucketName");
            log("absolutePathToDownload");
            log("subfolder (optional)");
            log("Example : java -jar executable.jar 5 awsIdentifier awsPassword awsBucketName /home/user/toUpload subFolder/wanted");

            throw new IllegalArgumentException("You must have 5 or 6 params");
        }

        log("daysAgo (Integer) "+args[0]);
        log("awsIdentifier "+args[1]);
        log("awsPassword "+args[2]);
        log("awsBucketName "+args[3]);
        log("absolutePathToDownload "+args[4]);
        log("subfolder (optional) "+args[5]);
    }

    private static Runnable executeOneDirectory(File directory) {
        return () -> {

            Arrays.stream(directory.listFiles())
                    .filter(File::isDirectory)
                    .forEach(UploadToS3::enqueueDirectory);

            Arrays.stream(directory.listFiles())
                    .filter(File::isFile)
                    .forEach(UploadToS3::checkDateAndUploadFile);
        };
    }

    private static void checkDateAndUploadFile(File file) {
        try {

            FileTime lastModifiedTime = Files.getLastModifiedTime(Paths.get(file.getAbsolutePath()));

            if (lastModifiedTime.toInstant().isAfter(maxAgeOfFilesToBeUploaded)) {
                String destination = file.getAbsolutePath().replace(basePath.getAbsolutePath(), "").replaceAll("^/", "");
                uploadImage(file, destination);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void uploadImage(File file, String destination) {

        log(String.format("Upload [%s] to [%s]", file, destination));

        try {
            Upload upload = transferManager.upload(awsBucketName, destination, file);
            upload.waitForUploadResult();
        } catch (InterruptedException | AmazonClientException e) {
            e.printStackTrace();
        }
    }

    private static void enqueueDirectory(File file) {
        executorService.submit(executeOneDirectory(file));
    }

    private static boolean noWaitingExecution() {
        log("Active threads : " + executorService.getActiveCount());
        log("Queue size : " + executorService.getQueue().size());
        return executorService.getActiveCount() == 0 && executorService.getQueue().size() == 0;
    }

    private static void log(String x) {
        System.out.println(Thread.currentThread().getName() + " : " + x);
    }
}

