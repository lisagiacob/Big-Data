package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    String us;
    int user;

    private String[] potentialFriends, friends;

    protected void setup(Context context) throws IOException, InterruptedException{
        String line;
        // Retrive the original paths of the distributed files
        URI[] urisCachedFiles = context.getCacheFiles();
        // Read and process the content of the file - 1st file in this case
        BufferedReader file = new BufferedReader(new FileReader(
                new File(new Path(urisCachedFiles[0].getPath()).getName())));
        // If the file isn't empty, save the line containing the user's friends
        if((line = file.readLine())!=null){
            System.out.println(line);
            user = (int) (line.charAt(4));
            // process the line
            //friends[user] = line.substring(6);
            //System.out.println(friends[user]);
        }
        file.close();
    }
            
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        System.out.println(value);
        String[] users = value.toString().split(",");
        
        // Se nella riga non compare l'utente specificato, allora il pair è formato da amici/amici potenziali/sconosciuti

        if(friends[user].contains(users[0]) && !users[1].equals(user) && !potentialFriends[user].contains(users[1])) potentialFriends[user] = potentialFriends[user] + " " + users[1];
        if(friends[user].contains(users[1]) && !users[0].equals(user) && !potentialFriends[user].contains(users[0])) potentialFriends[user] = potentialFriends[user] + " " + users[0]; 

    }

    protected void cleanup(Context context) throws IOException, InterruptedException{
        for(int i=0; i<7; i++){
            context.write(new Text("user"+ user), new Text(potentialFriends[i]));
        }
    }
}
