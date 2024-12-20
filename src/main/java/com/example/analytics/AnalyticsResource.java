package com.example.analytics;



import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mysql.cj.jdbc.MysqlDataSource;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import org.bson.Document;

import javax.sql.DataSource;
import java.sql.*;

@Path("/data")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AnalyticsResource {


    @Path("/analyze")
    @POST
    public Response analysis(String s,@HeaderParam("Authorization") boolean authHeader){
        if(!authHeader)
            return Response.status(Response.Status.UNAUTHORIZED).build();
        try (Connection conn = getDataSource().getConnection();
             MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://my-mongodb:27017"))){
            MongoDatabase database = mongoClient.getDatabase("data");
            double avg = getAvg(conn);
            int max = getMax(conn);
            int min = getMin(conn);
            MongoCollection<Document> collection = database.getCollection("analysis");
            Document document = new Document("avg", avg)
                    .append("max", max)
                    .append("min", min);
            collection.insertOne(document);
            return Response.ok().build();

        }
        catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().build();
        }
    }
    private DataSource getDataSource() throws SQLException {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName("mysql");
        ds.setPort(3306);
        ds.setDatabaseName("data");
        ds.setUser("root");
        ds.setPassword("123456");
        ds.setUseSSL(false);
        ds.setAllowPublicKeyRetrieval(true);

        return ds;
    }

    private double getAvg(Connection connection){
        String sql = "SELECT AVG(grade) from data";
        try (Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(sql);
            resultSet.next();
            return resultSet.getDouble(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }
    private int getMax(Connection connection){
        String sql = "SELECT MAX(grade) from data";
        try (Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(sql);
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }
    private int getMin(Connection connection){
        String sql = "SELECT MIN(grade) from data";
        try (Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(sql);
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
