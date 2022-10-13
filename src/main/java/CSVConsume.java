import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static java.lang.Integer.parseInt;

public class CSVConsume {
    public static void main(String[] args) {
        String jdbcUrl = "jdbc:mysql://localhost:8889/sodch";
        String username = "root";
        String password = "root";
        File dir;
        String dirPath = "/Users/sergejmakarov/Downloads/CsvToDatabaseJava-master/src/main/java";

        // int batchSize = 20;

        Connection connection = null;


        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(false);

            String sql = "insert into sodch(Kusp,kuspdt,ovd, sotr, forma, fioz,addresz, telz,nomuvd,rez, dtsov,fabulap,fabula,typepr,doklad,prov,vrvyezd,izyato, provboss, provdt, rassmotr,ispol,dtporuch,srokdt,provpro,resh,typez,mesto,sopr,dtkor,id,sodchid,adresz2,fab,fioz2,telz2) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            PreparedStatement statement = connection.prepareStatement(sql);

            dir = new File(dirPath);
            String[] fileList = dir.list();
            BufferedReader lineReader;

            if (fileList != null) {
                System.out.println("Read directory");
                for (String fileName : fileList) {

                    if (fileName.startsWith("sodch") && (fileName.endsWith(".csv"))) {
                        lineReader = new BufferedReader(new FileReader(dir.getAbsolutePath() + "/" + fileName));
                        System.out.println(dir.toString() +"/"+ fileName +"-----!!___");
                        String lineText = null;
                        int count = 0;
                        lineReader.readLine();
                        int i = 0;

                        while (true) {
                            lineText = lineReader.readLine();
                            if (lineText == null) {
                                break;
                            }

                            if (lineText.endsWith("|")) {
                                lineText += "\n";
                            }

                            i++;
                            System.out.println("Счетчик =" + i);

                            String[] data = lineText.split("\\|");
                            if (data.length == 36) {
                                System.out.println(data.length);

                                String kusp = data[0];
                                String kuspdt = data[1];
                                String ovd = data[2];
                                String sotr = data[3];
                                String forma = data[4];
                                String fioz = data[5];
                                String addresz = data[6];
                                String telz = data[7];
                                String nomovd = data[8];
                                String rez = data[9];
                                String dtsov = data[10];
                                String fabulap = data[11];
                                String fabula = data[12];
                                String typepr = data[13];
                                String doklad = data[14];
                                String prov = data[15];
                                String vrvyezd = data[16];
                                String izyato = data[17];
                                String provboss = data[18];
                                String provdt = data[19];
                                String rassmot = data[20];
                                String ispol = data[21];
                                String dtporuch = data[22];
                                String srokdt = data[23];
                                String provpro = data[24];
                                String resh = data[25];
                                String typez = data[26];
                                String mesto = data[27];
                                String sopr = data[28];
                                String drkor = data[29];
                                String id = data[30];
                                String sodchid = data[31];
                                String adresz2 = data[32];
                                String fabp = data[33];
                                String fioz2 = data[34];
                                String telz2 = data[35];


                                statement.setString(1, kusp);
                                statement.setString(2, kuspdt);
                                statement.setString(3, ovd);
                                statement.setString(4, sotr);
                                statement.setString(5, forma);
                                statement.setString(6, fioz);
                                statement.setString(7, addresz);
                                statement.setString(8, telz);
                                statement.setString(9, nomovd);
                                statement.setString(10, rez);
                                statement.setString(11, dtsov);
                                statement.setString(12, fabulap);
                                statement.setString(13, fabula);
                                statement.setString(14, typepr);
                                statement.setString(15, doklad);
                                statement.setString(16, prov);
                                statement.setString(17, vrvyezd);
                                statement.setString(18, izyato);
                                statement.setString(19, provboss);
                                statement.setString(20, provdt);
                                statement.setString(21, rassmot);
                                statement.setString(22, ispol);
                                statement.setString(23, dtporuch);
                                statement.setString(24, srokdt);
                                statement.setString(25, provpro);
                                statement.setString(26, resh);
                                statement.setString(27, typez);
                                statement.setString(28, mesto);
                                statement.setString(29, sopr);
                                statement.setString(30, drkor);
                                statement.setString(31, id);
                                statement.setString(32, sodchid);
                                statement.setString(33, adresz2);
                                statement.setString(34, fabp);
                                statement.setString(35, fioz2);
                                statement.setString(36, telz2);

                                statement.addBatch();
                                // if (count % batchSize == 0) {
                                statement.executeBatch();

                                // }
                            }
                        }
                        lineReader.close();
                        System.out.println(dir.toString() +"/"+ fileName);
                        Path temp = Files.move
                                (Paths.get(dir.toString() +"/"+ fileName.toString()),
                                        Paths.get("/Users/sergejmakarov/Downloads/CsvToDatabaseJava-master/src/main/java/out/" + fileName.toString()));

                    }


                }

            }
            //BufferedReader lineReader =new BufferedReader(new FileReader());


            statement.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("Data has been inserted successfully.");

        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}