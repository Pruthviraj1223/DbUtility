package com.mindarray;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.h2.mvstore.MVStore;
import org.json.simple.parser.JSONParser;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

class H2 {

    public void readMotadataAndWriteToJson() throws IOException {

        String filename = "db.json";

        BufferedWriter bufferedWriter = null;

        MVStore configDB = null;

        try (var resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("db.auth")) {

            var buffer = Buffer.buffer(IOUtils.toString(Objects.requireNonNull(resourceAsStream), StandardCharsets.UTF_8));

            configDB = new MVStore.Builder().
                    fileName(System.getProperty("user.dir") + "/" + "config" + "/" + "motadata")
                    .encryptionKey(new String(Base64.getDecoder().decode(buffer.getBytes())).toCharArray())
                    .backgroundExceptionHandler((thread, exception) -> System.out.println(exception.getMessage()))
                    .autoCommitDisabled()
                    .open();

            bufferedWriter = new BufferedWriter(new FileWriter(filename));

            Object[] collections = configDB.getMapNames().toArray();

            StringBuilder stringBuilder = new StringBuilder();

            int count = 0;

            stringBuilder.append("{");

            for (Object collection : collections) {

                count++;

                var objectMVMap = configDB.openMap(collection.toString());

                var keys = objectMVMap.keyList();

                stringBuilder.append(" \"").append(collection).append("\": [");

                for (Object key : keys) {

                    stringBuilder.append("{ \"").append(key).append("\" : ").append(objectMVMap.get(key)).append("},");

                }

                if (stringBuilder.lastIndexOf(",") > 0) {

                    stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(","));

                }

                stringBuilder.append("],");

                if (count == collections.length) {

                    stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(","));

                }

                bufferedWriter.write(stringBuilder.toString());

                stringBuilder.setLength(0);

            }

            bufferedWriter.write("}");

            System.out.println("Reading from motadata DB and writing to " + filename + " is completed.....");

        } catch (Exception exception) {

            System.out.println(exception.getMessage());

        } finally {

            if (bufferedWriter != null) {

                bufferedWriter.close();

            }

            if (configDB != null) {

                configDB.close();

            }

        }
    }

    public void readJsonAndWriteToMotadata() {

        String filename = "newdb.json";

        MVStore configDB = null;

        try (var resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("db.auth")) {

            var buffer = Buffer.buffer(IOUtils.toString(Objects.requireNonNull(resourceAsStream), StandardCharsets.UTF_8));

            configDB = new MVStore.Builder().
                    fileName(System.getProperty("user.dir") + "/src/main/motadata")
                    .encryptionKey(new String(Base64.getDecoder().decode(buffer.getBytes())).toCharArray())
                    .backgroundExceptionHandler((thread, exception) -> System.out.println(exception.getMessage()))
                    .autoCommitDisabled()
                    .open();

            JsonObject collections = new JsonObject(new JSONParser().parse(new FileReader(filename)).toString());

            for (Map.Entry<String, Object> collection : collections) {

                var objects = collections.getJsonArray(collection.getKey());

                if (objects.size() > 0) {

                    for (int index = 0; index < objects.size(); index++) {

                        var object = objects.getJsonObject(index);

                        for (Map.Entry<String, Object> entry : object) {

                            configDB.openMap(collection.getKey()).put(entry.getKey(), object.getJsonObject(entry.getKey()).encode());

                        }
                    }

                } else {

                    configDB.openMap(collection.getKey());

                }
            }

            System.out.println("Reading from " + filename + " and Writing to motadata DB is completed......");


        } catch (Exception exception) {

            System.out.println(exception.getMessage());

        } finally {

            if (configDB != null) {

                configDB.commit();

                configDB.close();

            }

        }
    }
}

public class H2Store {

    public static void main(String[] args) throws IOException {

        new H2().readMotadataAndWriteToJson();

    }
}
