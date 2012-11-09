/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package instamo;

import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.io.FileUtils;

public class AccumuloApp {
  
  public static void run(String instanceName, String zookeepers, String rootPassword, String args[]) throws Exception {
    // edit this method to play with Accumulo

    Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
    
    Connector conn = instance.getConnector("root", rootPassword);
    
    conn.tableOperations().create("foo");
    
    BatchWriter bw = conn.createBatchWriter("foo", 50000000, 60000l, 3);
    Mutation m = new Mutation("r1");
    m.put("cf1", "cq1", "v1");
    m.put("cf1", "cq2", "v3");
    bw.addMutation(m);
    bw.close();
    
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    for (Entry<Key,Value> entry : scanner) {
      System.out.println(entry.getKey() + " " + entry.getValue());
    }
  }
  
  public static void main(String[] args) throws Exception {
    File tmpDir = new File(FileUtils.getTempDirectory(), "macc-" + UUID.randomUUID().toString());
    
    try {
      MiniAccumuloCluster la = new MiniAccumuloCluster(tmpDir, "pass1234", new HashMap<String,String>());
      la.start();
    
      System.out.println("\n   ---- Running Accumulo App against accumulo-" + Constants.VERSION + "\n");

      run(la.getInstanceName(), la.getZookeepers(), "pass1234", args);

      System.out.println("\n   ---- Ran Accumulo App\n");

      la.stop();
    } finally {
      FileUtils.deleteQuietly(tmpDir);
    }
  }
  
}
