package org.fidlr.hadoop;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by omer on 8/25/14.
 */
public class PassThroughReducer<Key,V> extends Reducer<Key,V,Key,V> {

    public void reduce(Key key, Iterable<V> values,
                       Context context) throws IOException, InterruptedException {

        context.write(key, values.iterator().next());
    }
}
