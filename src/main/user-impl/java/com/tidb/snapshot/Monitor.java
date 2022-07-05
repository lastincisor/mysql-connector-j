package com.tidb.snapshot;


import com.mysql.cj.jdbc.ConnectionImpl;
import java.sql.Driver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Monitor {


    private static final String QUERY_TIDB_SNAPSHOT_SQL =
            "select `secondary_ts` from `tidb_cdc`.`syncpoint_v1` where `cf` = \"{ticdcCFname}\" order by `primary_ts` desc limit 1";

    private Ticdc ticdc = new Ticdc();

    private String url;

    private Properties info;

    private java.sql.Connection conn;

    private ScheduledThreadPoolExecutor executor;

    private Driver driver;

    private static final AtomicInteger threadId = new AtomicInteger();


    public Monitor(Driver driver,String url,Properties info,ScheduledThreadPoolExecutor executor){
        this.driver = driver;
        this.url = url;
        this.info = info;
        this.executor = executor;
        createExecutor();
    }

    public Monitor(Driver driver){
        this.driver = driver;
    }

    public static Monitor of(Driver driver,String url,Properties info,ScheduledThreadPoolExecutor executor){
        return new Monitor(driver,url,info,executor);
    }

    public static Monitor of(Driver driver){
        return new Monitor(driver);
    }

    public Monitor setInfo(String url,Properties info){
        this.url = url;
        this.info = info;
        createExecutor();
        return this;
    }

    public void createExecutor(){
        if(this.executor != null){
            return;
        }
        String executorName = "reload-Thread-" + threadId.getAndIncrement();
        this.executor =
                new ScheduledThreadPoolExecutor(
                        Runtime.getRuntime().availableProcessors(),
                        (runnable) -> {
                            Thread newThread = new Thread(runnable);
                            newThread.setName(executorName);
                            newThread.setDaemon(true);
                            return newThread;
                        });
        this.executor.setKeepAliveTime(100 * 2, TimeUnit.MILLISECONDS);
        this.executor.allowCoreThreadTimeOut(true);
        this.executor.scheduleWithFixedDelay(
                this::reload, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public Ticdc get(){
        return ticdc;
    }

    public void reload(){
        Long start = System.currentTimeMillis();
        System.out.println("Snapshot-tidb_snapshot-reload:id:"+Thread.currentThread().getId()+",ts:"+this.ticdc.getGlobalSecondaryTs().get()+",Globallasttime:"+this.ticdc.getGloballasttime());
        if(this.url == null){
            return;
        }
        if("".equals(this.url)){
            return;
        }
        try {
            if(this.conn == null){
                conn = driver.connect(this.url,this.info);
            }
            String ticdcCFname = ((ConnectionImpl) conn).getProperties().get("ticdcCFname")+"";
            String sql = buildTidbSnapshotSql(ticdcCFname);
            if(sql == null){
                return;
            }
            java.sql.PreparedStatement stmt = conn.prepareStatement( sql);
            //System.out.println("Snapshot-tidb_snapshot-reload:"+this.ticdc.getGlobalSecondaryTs().get()+",Globallasttime:"+this.ticdc.getGloballasttime());
            try (final ResultSet resultSet = stmt.executeQuery(sql)) {
                while (resultSet.next()) {
                    final String secondaryTs = resultSet.getString("secondary_ts");
                    //System.out.println("Snapshot-tidb_snapshot-db:"+secondaryTs);
                    if(secondaryTs != null){
                        Long secondaryTsValue = Long.parseLong(secondaryTs);
                        if(ticdc.getGlobalSecondaryTs().get() != secondaryTsValue){
                            this.ticdc.getGlobalSecondaryTs().set(Long.parseLong(secondaryTs));
                            this.ticdc.getGloballasttime().set(System.currentTimeMillis());
                            System.out.println("Snapshot-tidb_snapshot-reload-db:"+this.ticdc.getGlobalSecondaryTs().get()+",Globallasttime:"+this.ticdc.getGloballasttime());
                            Long end = System.currentTimeMillis();
                            System.out.println("Snapshot-tidb_snapshot-reload-db:"+this.ticdc.getGlobalSecondaryTs().get()+",Globallasttime:"+this.ticdc.getGloballasttime());
                            System.out.println("cdcruntime:getSnapshot:"+(end-start)+",tidbCurrentTs:"+this.ticdc.getGlobalSecondaryTs().get());
                        }
                        //String tidb_snapshot = this.session.queryServerVariable("@@tidb_snapshot");
                        //System.out.println("Snapshot-tidb_snapshot-queryServerVariable:"+tidb_snapshot);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String buildTidbSnapshotSql(String ticdcCFname){
        if(ticdcCFname == null){
            return null;
        }
        String sql = null;
        if(ticdcCFname != null){
            sql = QUERY_TIDB_SNAPSHOT_SQL.replace("{ticdcCFname}",ticdcCFname);
        }
        return sql;
    }
}
