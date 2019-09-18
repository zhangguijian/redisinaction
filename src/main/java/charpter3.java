import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.*;

public class charpter3 {

    private static int corePoolSize=Runtime.getRuntime().availableProcessors();
    public void main(String[] args){
        ThreadPoolExecutor threadPoolExecutor= new ThreadPoolExecutor(corePoolSize,corePoolSize+1,101, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>(1000));
        threadPoolExecutor.submit(new publisher(3));
    }
    public class publisher extends Thread{
        private int num;
        private Jedis conn;
        public publisher(int num){
            conn=new Jedis("localhost");
            this.num=num;
        }
        public void run(){
            for(int i=0;i<num;i++){
                conn.publish("channel",String.valueOf(i));
                try {
                    sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public class run_pubsub extends Thread{
        private Jedis conn;
        public run_pubsub(){
            conn=new Jedis("localhost");
        }
        public void run(){
            JedisPubSub jedisPubSub=new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {

                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {

                }

                @Override
                public void onSubscribe(String channel, int subscribedChannels) {

                }

                @Override
                public void onUnsubscribe(String channel, int subscribedChannels) {

                }

                @Override
                public void onPUnsubscribe(String pattern, int subscribedChannels) {

                }

                @Override
                public void onPSubscribe(String pattern, int subscribedChannels) {

                }
            };
        }
    }
}
