import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ZParams;

import java.util.*;

public class charpter1 {
    private static final int ONE_WEEK_SECONDS=7*84000;
    private static final int VOTE_SCORE=432;

    private static final int ARTICLES_PER_PAGE=25;
    public static  void main(String[] args){
        new charpter1().run();
    }
    public void run(){
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        String articleId = post_article(
                conn, "username", "A title", "http://www.google.com");
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String,String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String,String> entry : articleData.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();

        article_vote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;

        System.out.println("The currently highest-scoring articles are:");
        List<Map<String,String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;

    }
    private void printArticles(List<Map<String,String>> articles){
        for (Map<String,String> article : articles){
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String,String> entry : article.entrySet()){
                if (entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
    public String post_article(Jedis conn,String user,String title,String link){
        String articleId = String.valueOf(conn.incr("article:"));

        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_SECONDS);

        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String,String> articleData = new HashMap<String,String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);
        conn.zadd("score:", now + VOTE_SCORE, article);
        conn.zadd("time:", now, article);

        return articleId;
    }
    public void article_vote(Jedis conn,String user,String arcticle){
        long cutoff=(System.currentTimeMillis()/1000)-ONE_WEEK_SECONDS;
        if(conn.zscore("time:",arcticle)<cutoff)
            return;
        String article_id=arcticle.substring(arcticle.indexOf(":")+1);
          Pipeline pipeline= conn.pipelined();
          pipeline.multi();
          pipeline.sadd("voted:"+article_id,user);
         Response<List<Object>> listResponsePipeline=pipeline.exec();//执行批量语句
         pipeline.sync();
//         for(Object object:listResponsePipeline.get()){
//             System.out.println(object);
//         }
         if((Long)listResponsePipeline.get().get(0)==1) {
//          Response<List<Object>> responseFromPipeLine=pipeline.exec();
//          System.out.println(responseFromPipeLine.get().get(0));
             pipeline.multi();
             pipeline.zincrby("score:" + article_id, VOTE_SCORE, arcticle);
             pipeline.hincrBy(arcticle, "votes", 1);
         }
          pipeline.exec();
          pipeline.sync();//close the pipeline
    }

    public List<Map<String,String>> getArticles(Jedis conn,int page){
         return getArticles(conn,page,"score:");
    }
    public List<Map<String,String>> getArticles(Jedis conn,int page,String order){
        int start=(page-1)*ARTICLES_PER_PAGE;
        int end=start+ARTICLES_PER_PAGE-1;
        Set<String> ids=conn.zrevrange(order,start,end);
        List<Map<String,String>> articles=new ArrayList<>();
        String[] idstring= ids.toArray(new String[ids.size()]);
        Pipeline pipeline=conn.pipelined();//事务开始
        pipeline.multi();
        for(String id:ids){
               pipeline.hgetAll(id);
//            Map<String,String> artcle=conn.hgetAll(id);
//            artcle.put("id",id);
//            articles.add(artcle);
        }
        Response<List<Object>> listResponsePipeLine=pipeline.exec();
        pipeline.sync();
        for(int i=0;i<listResponsePipeLine.get().size();i++){
            Map<String,String> article= (Map<String, String>) listResponsePipeLine.get().get(i);
            article.put("id",idstring[i]);
            articles.add(article);
        }
//        for(Object object:listResponsePipeLine.get()){
//             System.out.println(object);
//             Map<String,String> article= (Map<String, String>) object;
//             articles.add(article);
//         }
        return articles;
    }
    public void addGroups(Jedis conn,String articleId,String[] toAdd){
        String article="article:"+articleId;
        for(String group :toAdd){
            conn.sadd("group:"+group,article);
        }
    }
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;
        if (!conn.exists(key)) {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }
}
