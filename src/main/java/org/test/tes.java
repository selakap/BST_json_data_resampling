package org.test;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.SSLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class tes {
    public static void main(String[] args) throws JSONException {
        //String str = "{\"17\":[[1707779444,11103],[1707779504,13956],[1707779564,13632],[1707789004,10]],\"18\":[[1707779624,13391],[1707779684,12003],[1707779744,15204],[1707779804,13412]]}";
        String str = "{\"17\":[[1707779444,11103],[1707779504,13956],[1707779564,13632]],\"18\":[[1707779624,133911],[1707779684,12003],[1707779744,15204],[1707779804,1312]],\"19\":[[1707779444,11103],[1707779504,13956],[1707779564,13632],[1707789004,1022]],\"29\":[[1707779444,11103],[1707779504,13956],[1707779564,1632],[1707789004,10]],\"30\":[[1707779444,11103],[1707779504,13956],[1707779564,13632],[1707789004,10]],\"31\":[[1707779444,1103],[1707779504,1356],[1707779564,13632],[1707789004,10]]}";
        JSONObject Json_data_records = new JSONObject(str);
        System.out.println(Json_data_records);
        String[] colheads = {"OP1", "OP2", "OP3","IP1", "IP2", "IP3"};//One to one mapping between colheads and attribute_ids
                                                                        // according to the python script
        String[] attribute_ids = {"29", "30", "31", "17", "18", "19"};
        List<DataPoint> originalDataIP1 = new ArrayList<>(); //data sets for each and every attribute_ids
        List<DataPoint> originalDataIP2 = new ArrayList<>();
        List<DataPoint> originalDataIP3 = new ArrayList<>();
        List<DataPoint> originalDataOP1 = new ArrayList<>();
        List<DataPoint> originalDataOP2 = new ArrayList<>();
        List<DataPoint> originalDataOP3 = new ArrayList<>();
        List<LocalDateTime> timestampList = new ArrayList<>();


        for (String key : Json_data_records.keySet()) { // iterate through the attribute_ids
            String value = Json_data_records.get(key).toString();
            JSONArray jsonArray = new JSONArray(value);//value of attribute ids

            //handling data sets for single attribute codes
            for (int i = 0; i < jsonArray.length(); i++) { //iteration of values of attribute codes such as op1,op2 etc
                JSONArray tempJsonArray = new JSONArray(jsonArray.get(i).toString());
                String stringTimestamp = tempJsonArray.get(0).toString();//get the timestamp of the particular attribute id value
                Double valueOfTheTimesatmp = Double.parseDouble(tempJsonArray.get(1).toString()); //get the value of the particular
                                                                                                    // attribute id value
                long longTimestamp = Long.parseLong(stringTimestamp);//convert timestamp to long

                LocalDateTime dateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(longTimestamp),
                        ZoneOffset.UTC
                );// LocalDateTime of the timesatmp

                if (!timestampList.contains(dateTime.withMinute(0).withSecond(0))) {
                    timestampList.add(dateTime.withMinute(0).withSecond(0));
                }

                if (key.contains(attribute_ids[0])) { //OP1 case
                    originalDataOP1.add(new DataPoint(dateTime, valueOfTheTimesatmp));
                } else if (key.contains(attribute_ids[1])){ //OP2 case
                    originalDataOP2.add(new DataPoint(dateTime, valueOfTheTimesatmp));
                }else if (key.contains(attribute_ids[2])){ //OP3 case
                    originalDataOP3.add(new DataPoint(dateTime, valueOfTheTimesatmp));
                }else if (key.contains(attribute_ids[3])){ //IP1 case
                    originalDataIP1.add(new DataPoint(dateTime, valueOfTheTimesatmp));
                }else if (key.contains(attribute_ids[4])){ //IP2 case
                    originalDataIP2.add(new DataPoint(dateTime, valueOfTheTimesatmp));
                }else if (key.contains(attribute_ids[5])){ //IP3 case
                    originalDataIP3.add(new DataPoint(dateTime, valueOfTheTimesatmp));
                } else {
                    System.out.println("Not belongs to a defined attribute ID");
                }
            }
        }
        //call resampling the data
        Map<LocalDateTime, Double> resampledDataOP1 = resampleHourly(originalDataOP1);
        Map<LocalDateTime, Double> resampledDataOP2 = resampleHourly(originalDataOP2);
        Map<LocalDateTime, Double> resampledDataOP3 = resampleHourly(originalDataOP3);
        Map<LocalDateTime, Double> resampledDataIP1 = resampleHourly(originalDataIP1);
        Map<LocalDateTime, Double> resampledDataIP2 = resampleHourly(originalDataIP2);
        Map<LocalDateTime, Double> resampledDataIP3 = resampleHourly(originalDataIP3);
        //print
        System.out.println("Resampled data on an hourly basis OP1 : " + resampledDataOP1);
        System.out.println("Resampled data on an hourly basis OP2 : " + resampledDataOP2);
        System.out.println("Resampled data on an hourly basis OP3 : " + resampledDataOP3);
        System.out.println("Resampled data on an hourly basis IP1 : " + resampledDataIP1);
        System.out.println("Resampled data on an hourly basis IP2 : " + resampledDataIP2);
        System.out.println("Resampled data on an hourly basis IP3 : " + resampledDataIP3);

        Collections.sort(timestampList);
        for (LocalDateTime element : timestampList) {
            List<Double> tempAttributeValueList = new ArrayList<>();
            tempAttributeValueList.add(resampledDataOP1.get(element) == null ? null : resampledDataOP1.get(element));
            tempAttributeValueList.add(resampledDataOP2.get(element) == null ? null : resampledDataOP2.get(element));
            tempAttributeValueList.add(resampledDataOP3.get(element) == null ? null : resampledDataOP3.get(element));
            tempAttributeValueList.add(resampledDataIP1.get(element) == null ? null : resampledDataIP1.get(element));
            tempAttributeValueList.add(resampledDataIP2.get(element) == null ? null : resampledDataIP2.get(element));
            tempAttributeValueList.add(resampledDataIP3.get(element) == null ? null : resampledDataIP3.get(element));

            //Calculate OPT and IPT(sum of all OPs and IPs)
            Double OPT =0.00;
            Double IPT =0.00;

            OPT = (resampledDataOP1.get(element) == null ? 0 : resampledDataOP1.get(element))+
                    (resampledDataOP2.get(element) == null ? 0 : resampledDataOP2.get(element))+
                            (resampledDataOP3.get(element) == null ? 0 : resampledDataOP3.get(element));
            IPT = (resampledDataIP1.get(element) == null ? 0 : resampledDataIP1.get(element))+
                    (resampledDataIP2.get(element) == null ? 0 : resampledDataIP2.get(element))+
                    (resampledDataIP3.get(element) == null ? 0 : resampledDataIP3.get(element));

            tempAttributeValueList.add(OPT == 0.00 ? null : OPT);
            tempAttributeValueList.add( IPT == 0.00 ? null : IPT);
            //System.out.println("OPT: "+OPT);
            //System.out.println("OPT: "+IPT);

            //Insert the values to Database
            insertIntoMysql(element,tempAttributeValueList);
        }


    }
    public static Map<LocalDateTime, Double> resampleHourly(List<DataPoint> data) {
        Map<LocalDateTime, Double> resampledData = new HashMap<>();

        // Group data points by the hour component of their timestamps
        Map<Integer, List<DataPoint>> hourlyData = new HashMap<>();
        for (DataPoint dp : data) {
            int hour = dp.timestamp.getHour();
            hourlyData.computeIfAbsent(hour, k -> new ArrayList<>()).add(dp);
        }

        // Aggregate data points within each hour
        for (int hour : hourlyData.keySet()) {
            List<DataPoint> hourData = hourlyData.get(hour);
            double sum = 0;
            for (DataPoint dp : hourData) {
                sum += dp.value;
            }
            double average = sum/hourData.size();
            //put the resampled data into map
            resampledData.put(hourData.get(0).timestamp.withMinute(0).withSecond(0), average); // put hour and sum to
                                                                                            // the resamples data map
        }

        return resampledData;
    }

    public static void insertIntoMysql(LocalDateTime timestamp, List<Double> attributesValues) {
        // JDBC URL, username, and password of MySQL server
        String url = "jdbc:mysql://localhost:3306/mysqltest?useSSL=false";
        String user = "root";
        String password = "root";

        // SQL query to insert data
        String InsertQuery = "INSERT INTO vrm_hourly (timestamp, op1, op2, op3, ip1, ip2, ip3, opT, ipT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";


        try {

            Connection conn = DriverManager.getConnection(url, user, password);
            PreparedStatement preparedStatement = conn.prepareStatement(InsertQuery);
            conn.setAutoCommit(false);

            // Set parameters
            preparedStatement.setString(1, String.valueOf(timestamp));
            preparedStatement.setString(2, String.valueOf(attributesValues.get(0)));
            preparedStatement.setString(3, String.valueOf(attributesValues.get(1)));
            preparedStatement.setString(4, String.valueOf(attributesValues.get(2)));
            preparedStatement.setString(5, String.valueOf(attributesValues.get(3)));
            preparedStatement.setString(6, String.valueOf(attributesValues.get(4)));
            preparedStatement.setString(7, String.valueOf(attributesValues.get(5)));
            preparedStatement.setString(8, String.valueOf(attributesValues.get(6)));
            preparedStatement.setString(9, String.valueOf(attributesValues.get(7)));

            // Execute the query
            preparedStatement.executeUpdate();
            System.out.println("Data inserted successfully.");
            conn.commit();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    static class DataPoint { //datapoint class
        LocalDateTime timestamp;
        double value;

        public DataPoint(LocalDateTime timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }
}

