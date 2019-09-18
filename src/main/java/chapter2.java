import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import javax.sound.midi.Track;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class chapter2 {
    public static void main(String[] args) throws InterruptedException {
        new chapter2().run();
    }
    public void run() throws InterruptedException {

        Jedis conn = new Jedis("localhost");
        conn.select(15);
        testLoginCookies(conn);
    }
    public void testLoginCookies(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();

        update_token(conn, token, "username", "itemX");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String r = check_token(conn, token);
        System.out.println(r);
        System.out.println();
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        CleanSeassionThread thread = new CleanSeassionThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()){
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
    }
    public String check_token(Jedis conn,String token){
        return conn.hget("login:",token);
    }
    public void update_token(Jedis conn,String token,String user,String item) {
        long now = (System.currentTimeMillis() / 1000);
        conn.hset("login:", token, user);
        conn.zadd("recent:", now, token);
        if (item != null) {
            conn.zadd("viewed:" + token, now, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }
    public void add_to_card(Jedis conn,String session,String item,int count){
        if(count<=0)
            conn.hdel("cart:"+session,item);
        else
            conn.hset("cart:"+session,item,String.valueOf(count));
    }
    public String cache_request(Jedis conn,String request,Callback callback){
        if (!canCache(conn, request)){
            return callback != null ? callback.call(request) : null;
        }

        String pageKey = "cache:" + hashRequest(request);
        String content = conn.get(pageKey);

        if (content == null && callback != null){
            content = callback.call(request);
            conn.setex(pageKey, 300, content);
        }

        return content;
    }
    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }
    public boolean canCache(Jedis conn, String request) {
        try {
            URL url = new URL(request);
            HashMap<String,String> params = new HashMap<String,String>();
            if (url.getQuery() != null){
                for (String param : url.getQuery().split("&")){
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank < 10000;
        }catch(MalformedURLException mue){
            return false;
        }
    }
    public void schedule_row_cache(Jedis conn,String row_id,int delay){
        conn.zadd("delay:",delay,row_id);
        conn.zadd("shcedule:",System.currentTimeMillis()/1000,row_id);
    }
    public boolean isDynamic(Map<String,String> params) {
        return params.containsKey("_");
    }

    public String extractItemId(Map<String,String> params) {
        return params.get("item");
    }
    public interface  Callback{
        public String call(String quest);
    }
    public class CleanSeassionThread extends Thread{
        private Jedis conn;
        private int limit;
        private boolean quit;
        public CleanSeassionThread(int limit){
            conn=new Jedis("localhost");
            this.limit=limit;
            this.conn.select(15);
        }
        public void quit(){
            quit=true;
        }
        public void run(){
         while (!quit){
             long size=conn.zcard("recent:");
             if(size<=limit){
                 try {
                     sleep(1000);
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
                 continue;
             }
             int end_index= (int) Math.min(size-limit,100);
             Set<String> tokenSet=conn.zrange("recent:",0,end_index-1);
             String[] tokens=tokenSet.toArray(new String[tokenSet.size()]);
             ArrayList<String> session_keys=new ArrayList<>();
             for(String token:tokenSet){
                 session_keys.add("viewed:"+token);
             }
             conn.del(session_keys.toArray(new String[session_keys.size()]));
             conn.hdel("login:",tokens);
             conn.zrem("recent:",tokens);
         }
        }
    }
    public class CleanFullSession extends Thread{
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanFullSession(int limit){
            this.limit=limit;
            conn=new Jedis("localhost");
        }
        public void quit(){
            this.quit=true;
        }
        public  void run(){
            while (!quit){
                long size=conn.zcard("recent:");
                if(size<=limit) {
                    try {
                        sleep(1000);
                        continue;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                int endIndex=Math.min((int)size-limit,1000);
                Set<String> tokenSet=conn.zrange("recent:",0,endIndex);
                String[] tokens=tokenSet.toArray(new String[tokenSet.size()]);
                ArrayList<String> sessions=new ArrayList<>();
                for(String token:tokenSet){
                    sessions.add("viewed:" + token);
                    sessions.add("cart:" + token);
                }
                conn.del(sessions.toArray(new String[sessions.size()]));
                conn.hdel("login:",tokens);
                conn.zrem("recent:",tokens);
            }
        }
    }
    public class cache_rows extends Thread{
        private Jedis conn;
        private boolean quit;
        public cache_rows(){
            conn=new Jedis("localhost");
        }
        public void quit(){
            this.quit=true;
        }
        public void run(){
            while(!quit){
                Set<Tuple>  range= conn.zrangeWithScores("schedule:",0,0);
                Tuple next=range.size()>0?range.iterator().next():null;
                long now=System.currentTimeMillis()/1000;
                if(next!=null ||next.getScore()>now){
                    try {
                        sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            }
        }
    }
    public class rescale_viewed extends Thread{
        private  Jedis conn;
        private  boolean quit;
        public rescale_viewed(){
            this.conn=new Jedis("localhost");
        }
        public void quit(){
            this.quit=true;
        }
        public void run(){
            while (!quit){
                conn.zremrangeByRank("viewed:",0,-20001);
                conn.zinterstore("viewed:");
            }
        }
    }
}
