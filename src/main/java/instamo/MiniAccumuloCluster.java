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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.logger.LogService;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.tabletserver.TabletServer;
import org.apache.accumulo.server.util.Initialize;
import org.apache.zookeeper.server.ZooKeeperServerMain;

/**
 * 
 */
public class MiniAccumuloCluster {
  
  private static class LogWriter extends Thread {
    private BufferedReader in;
    private BufferedWriter out;
    
    /**
     * @param errorStream
     * @param logDir
     * @throws IOException
     */
    public LogWriter(InputStream stream, File logFile) throws IOException {
      this.setDaemon(true);
      this.in = new BufferedReader(new InputStreamReader(stream));
      out = new BufferedWriter(new FileWriter(logFile));
    }
    
    public synchronized void flush() throws IOException {
      if (out != null)
        out.flush();
    }

    public void run() {
      String line;
      
      try {
        while ((line = in.readLine()) != null) {
          out.append(line);
          out.append("\n");
        }
        
        synchronized (this) {
          out.close();
          out = null;
          in.close();
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private File confDir;
  private File zooKeeperDir;
  private File accumuloDir;
  private File zooCfgFile;
  private File logDir;
  private File walogDir;
  
  private Process zooKeeperProcess;
  private Process masterProcess;
  private Process tabletServerProcess;
  private Process loggerProcess;

  private int zooKeeperPort;
  
  private List<LogWriter> logWriters = new ArrayList<MiniAccumuloCluster.LogWriter>();
  private String rootPassword;



  private int getRandomFreePort() {
    Random r = new Random();
    int count = 0;
    
    while (count < 13) {
      int port = r.nextInt((1 << 16) - 1024) + 1024;
      
      ServerSocket so = null;
      try {
        so = new ServerSocket(port);
        so.setReuseAddress(true);
        return port;
      } catch (IOException ioe) {
        
      } finally {
        if (so != null)
          try {
            so.close();
          } catch (IOException e) {}
      }
      
    }
    
    throw new RuntimeException("Unable to find port");
  }

  private Process exec(Class clazz, String... args) throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    
    classpath = confDir.getAbsolutePath() + File.pathSeparator + classpath;

    String className = clazz.getCanonicalName();
    
    ArrayList<String> argList = new ArrayList<String>();
    
    argList.addAll(Arrays.asList(javaBin, "-cp", classpath, className));
    argList.addAll(Arrays.asList(args));
    
    ProcessBuilder builder = new ProcessBuilder(argList);
    
    builder.environment().put("ACCUMULO_LOG_DIR", logDir.getAbsolutePath());

    Process process = builder.start();

    LogWriter lw;
    lw = new LogWriter(process.getErrorStream(), new File(logDir, clazz.getSimpleName() + "_" + process.hashCode() + ".err"));
    logWriters.add(lw);
    lw.start();
    lw = new LogWriter(process.getInputStream(), new File(logDir, clazz.getSimpleName() + "_" + process.hashCode() + ".out"));
    logWriters.add(lw);
    lw.start();

    return process;
  }

  public MiniAccumuloCluster(File dir, String rootPassword, Map<String,String> siteConfig) throws IOException {

    if (dir.exists() && !dir.isDirectory())
      throw new IllegalArgumentException("Must pass in directory, " + dir + " is a file");
    
    if (dir.exists() && dir.list().length != 0)
      throw new IllegalArgumentException("Directory " + dir + " is not empty");
    
    this.rootPassword = rootPassword;

    confDir = new File(dir, "conf");
    accumuloDir = new File(dir, "accumulo");
    zooKeeperDir = new File(dir, "zookeeper");
    logDir = new File(dir, "logs");
    walogDir = new File(dir, "walogs");
    
    confDir.mkdirs();
    accumuloDir.mkdirs();
    zooKeeperDir.mkdirs();
    logDir.mkdirs();
    walogDir.mkdirs();
    
    zooKeeperPort = getRandomFreePort();

    File siteFile = new File(confDir, "accumulo-site.xml");
    
    FileWriter fileWriter = new FileWriter(siteFile);
    fileWriter.append("<configuration>\n");
    fileWriter.append("<property><name>" + Property.INSTANCE_DFS_URI.getKey() + "</name><value>file:///</value></property>\n");
    fileWriter.append("<property><name>" + Property.INSTANCE_DFS_DIR + "</name><value>" + accumuloDir.getAbsolutePath() + "</value></property>\n");
    fileWriter.append("<property><name>" + Property.INSTANCE_ZK_HOST + "</name><value>localhost:" + zooKeeperPort + "</value></property>\n");
    fileWriter.append("<property><name>" + Property.MASTER_CLIENTPORT + "</name><value>" + getRandomFreePort() + "</value></property>\n");
    fileWriter.append("<property><name>" + Property.TSERV_CLIENTPORT + "</name><value>" + getRandomFreePort() + "</value></property>\n");
    fileWriter.append("<property><name>" + Property.LOGGER_DIR + "</name><value>" + walogDir.getAbsolutePath() + "</value></property>\n");
    for (Entry<String,String> entry : siteConfig.entrySet())
      fileWriter.append("<property><name>" + entry.getKey() + "</name><value>" + entry.getValue() + "</value></property>\n");
    fileWriter.append("</configuration>\n");
    fileWriter.close();
    
    zooCfgFile = new File(confDir, "zoo.cfg");
    fileWriter = new FileWriter(zooCfgFile);
    fileWriter.append("tickTime=2000\n");
    fileWriter.append("initLimit=10\n");
    fileWriter.append("syncLimit=5\n");
    fileWriter.append("clientPort=" + zooKeeperPort + "\n");
    fileWriter.append("maxClientCnxns=100\n");
    fileWriter.append("dataDir=" + zooKeeperDir.getAbsolutePath() + "\n");
    fileWriter.close();
    
  }
  
  public void start() throws IOException, InterruptedException {
    if (zooKeeperProcess != null)
      throw new IllegalStateException("Already started");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          MiniAccumuloCluster.this.stop();
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    zooKeeperProcess = exec(ZooKeeperServerMain.class, zooCfgFile.getAbsolutePath());

    Process initProcess = exec(Initialize.class);
    initProcess.getOutputStream().write("test\n".getBytes());
    initProcess.getOutputStream().write((rootPassword + "\n").getBytes());
    initProcess.getOutputStream().write((rootPassword + "\n").getBytes());
    initProcess.getOutputStream().flush();
    initProcess.waitFor();
    
    masterProcess = exec(Master.class);
    tabletServerProcess = exec(TabletServer.class);
    loggerProcess = exec(LogService.class);
  }

  public String getInstanceName() {
    return "test";
  }
  
  public String getZookeepers() {
    return "localhost:" + zooKeeperPort;
  }

  public void stop() throws IOException, InterruptedException {
    if (zooKeeperProcess != null)
      zooKeeperProcess.destroy();
    if (masterProcess != null)
      masterProcess.destroy();
    if (tabletServerProcess != null)
      tabletServerProcess.destroy();
    if (loggerProcess != null)
      loggerProcess.destroy();
    
    for (LogWriter lw : logWriters)
      lw.flush();
  }
}
