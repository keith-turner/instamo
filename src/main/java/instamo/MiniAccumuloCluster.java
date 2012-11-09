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
import java.util.TimerTask;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.tabletserver.TabletServer;
import org.apache.accumulo.server.util.Initialize;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.zookeeper.server.ZooKeeperServerMain;

/**
 * A utility class that will create Zookeeper and Accumulo processes that write all of their data to a single local directory. This class makes it easy to test
 * code against a real Accumulo instance. Its much more accurate for testing than MockAccumulo, but much slower than MockAccumulo.
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
      
      SimpleTimer.getInstance().schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            flush();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }, 1000, 1000);
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

  private File baseDir;
  private File libDir;
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

  private Process exec(Class<? extends Object> clazz, String... args) throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    
    classpath = confDir.getAbsolutePath() + File.pathSeparator + classpath;

    String className = clazz.getCanonicalName();
    
    ArrayList<String> argList = new ArrayList<String>();
    
    argList.addAll(Arrays.asList(javaBin, "-cp", classpath, "-Xmx128m", "-XX:+UseConcMarkSweepGC", "-XX:CMSInitiatingOccupancyFraction=75",
        className));
    argList.addAll(Arrays.asList(args));
    
    ProcessBuilder builder = new ProcessBuilder(argList);
    
    builder.environment().put("ACCUMULO_HOME", baseDir.getAbsolutePath());
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

  private void appendProp(FileWriter fileWriter, Property key, String value, Map<String,String> siteConfig) throws IOException {
    appendProp(fileWriter, key.getKey(), value, siteConfig);
  }

  private void appendProp(FileWriter fileWriter, String key, String value, Map<String,String> siteConfig) throws IOException {
    if (!siteConfig.containsKey(key))
      fileWriter.append("<property><name>" + key + "</name><value>" + value + "</value></property>\n");
  }

  /**
   * 
   * @param dir
   *          An empty or nonexistant temp directoy that Accumulo and Zookeeper can store data in. Creating the directory is left to the user. Java 7, Guava,
   *          and Junit provide methods for creating temporary directories.
   * @param rootPassword
   *          Initial root password for instance.
   * @param siteConfig
   *          Any system properties that needs to be set before Accumulo processes are started. These are properties that would normally be placed in
   *          accumulo-site.xml
   * @throws IOException
   */

  public MiniAccumuloCluster(File dir, String rootPassword, Map<String,String> siteConfig) throws IOException {

    if (dir.exists() && !dir.isDirectory())
      throw new IllegalArgumentException("Must pass in directory, " + dir + " is a file");
    
    if (dir.exists() && dir.list().length != 0)
      throw new IllegalArgumentException("Directory " + dir + " is not empty");
    
    this.rootPassword = rootPassword;

    baseDir = dir;
    libDir = new File(dir, "lib");
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
    libDir.mkdirs();
    
    zooKeeperPort = getRandomFreePort();

    File siteFile = new File(confDir, "accumulo-site.xml");
    
    FileWriter fileWriter = new FileWriter(siteFile);
    fileWriter.append("<configuration>\n");
    
    appendProp(fileWriter, Property.INSTANCE_DFS_URI, "file:///", siteConfig);
    appendProp(fileWriter, Property.INSTANCE_DFS_DIR, accumuloDir.getAbsolutePath(), siteConfig);
    appendProp(fileWriter, Property.INSTANCE_ZK_HOST, "localhost:" + zooKeeperPort, siteConfig);
    appendProp(fileWriter, Property.MASTER_CLIENTPORT, "" + getRandomFreePort(), siteConfig);
    appendProp(fileWriter, Property.TSERV_CLIENTPORT, "" + getRandomFreePort(), siteConfig);
    appendProp(fileWriter, Property.LOGGER_DIR, walogDir.getAbsolutePath(), siteConfig);
    appendProp(fileWriter, Property.TSERV_DATACACHE_SIZE, "10M", siteConfig);
    appendProp(fileWriter, Property.TSERV_INDEXCACHE_SIZE, "10M", siteConfig);
    appendProp(fileWriter, Property.TSERV_MAXMEM, "50M", siteConfig);
    appendProp(fileWriter, Property.TSERV_WALOG_MAX_SIZE, "100M", siteConfig);
    appendProp(fileWriter, Property.TSERV_NATIVEMAP_ENABLED, "false", siteConfig);
    // since there is a small amount of memory, check more frequently for majc... setting may not be needed in 1.5
    appendProp(fileWriter, Property.TSERV_MAJC_DELAY, "3", siteConfig);
    appendProp(fileWriter, Property.GENERAL_CLASSPATHS, libDir.getAbsolutePath(), siteConfig);
    appendProp(fileWriter, Property.GENERAL_DYNAMIC_CLASSPATHS, libDir.getAbsolutePath(), siteConfig);
    if (Constants.VERSION.startsWith("1.4"))
      appendProp(fileWriter, "logger.sort.buffer.size", "50M", siteConfig);

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

  /**
   * Starts Accumulo and Zookeeper processes. Can only be called once.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws IllegalStateException
   *           if already started
   */

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

    // TODO initialization could probably be done in process
    Process initProcess = exec(Initialize.class);
    initProcess.getOutputStream().write("test\n".getBytes());
    initProcess.getOutputStream().write((rootPassword + "\n").getBytes());
    initProcess.getOutputStream().write((rootPassword + "\n").getBytes());
    initProcess.getOutputStream().flush();
    initProcess.waitFor();
    
    masterProcess = exec(Master.class);
    tabletServerProcess = exec(TabletServer.class);
    if (Constants.VERSION.startsWith("1.4")) {
      try {
        loggerProcess = exec(this.getClass().getClassLoader().loadClass("org.apache.accumulo.server.logger.LogService"));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * @return Accumulo instance name
   */

  public String getInstanceName() {
    return "test";
  }
  
  /**
   * @return zookeeper connection string
   */

  public String getZookeepers() {
    return "localhost:" + zooKeeperPort;
  }

  /**
   * Stops Accumulo and Zookeeper processes. If stop is not called, there is a shutdown hook that is setup to kill the processes. Howerver its probably best to
   * call stop in a finally block as soon as possible.
   * 
   * @throws IOException
   * @throws InterruptedException
   */

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
