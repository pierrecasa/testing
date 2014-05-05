/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package example;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author PierreC
 */
public class WordMapper extends Mapper<Text, Text, Text, Text> {

    private Text word = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException 
    {

        StringTokenizer itr = new StringTokenizer(value.toString(), ",");

        while (itr.hasMoreTokens()) 
        {
            word.set(itr.nextToken());
            context.write(key, word);
        }

    }

}
