package com.ahmet;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.eq;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String REPLICA_URL = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=repld";
    private static final String DB = "online-school";
    private static final double MIN_GPA = 90.0;

    public static void main(String[] args) {
        String courseName = args[0];
        String studentName = args[1];
        int age = Integer.parseInt(args[2]);
        double gpa = Double.parseDouble(args[3]);

        MongoDatabase onlineSchoolDB = connectToMongoDB();
        enroll(onlineSchoolDB, courseName, studentName, age, gpa);
    }

    private static void enroll(MongoDatabase database, String course, String studentName, int age, double gpa) {
        if (!isValidCourse(database, course)) {
            LOGGER.error("Invalid course: {}", course);
            return;
        }

        MongoCollection<Document> courseCollection = database.getCollection(course)
                .withWriteConcern(WriteConcern.MAJORITY)
                .withReadPreference(ReadPreference.primaryPreferred());

        if (courseCollection.find(eq("name", studentName)).first() != null) {
            LOGGER.error("Student {} already enrolled in course {}", studentName, course);
            return;
        }

        if (gpa < MIN_GPA) {
            LOGGER.error("Student {} should improve grades to enrol in course {}", studentName, course);
            return;
        }

        courseCollection.insertOne(new Document("name", studentName).append("age", age).append("gpa", gpa));

        LOGGER.warn("Student {} enrolled in course {}", studentName, course);

        LOGGER.warn("List of students enrolled in course {}",course);
        for (Document document : courseCollection.find()) {
            LOGGER.warn(document.toString());
        }
    }

    private static boolean isValidCourse(MongoDatabase database, String course) {
        for (String collectionName : database.listCollectionNames()) {
            if (collectionName.equals(course)) {
                return true;
            }
        }
        return false;
    }

    private static MongoDatabase connectToMongoDB() {
        MongoClient mongoClient = MongoClients.create(REPLICA_URL);
        return mongoClient.getDatabase(DB);
    }
}
