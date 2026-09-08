import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration; import java.util.*;
public class OffsetCheck {
  public static void main(String[] a) {
    String topic=a[0]; long expected=Long.parseLong(a[1]); String proto=a[2]; int mpr=a.length>3?Integer.parseInt(a[3]):500;
    Properties p=new Properties();
    p.put("bootstrap.servers","localhost:9092"); p.put("group.id","verify-"+System.nanoTime());
    p.put("group.protocol",proto); p.put("auto.offset.reset","earliest"); p.put("enable.auto.commit","false");
    p.put("max.poll.records",String.valueOf(mpr));
    p.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
    Map<TopicPartition,Long> next=new HashMap<>(); long count=0, gaps=0, dups=0, t0=System.currentTimeMillis();
    try (KafkaConsumer<byte[],byte[]> c=new KafkaConsumer<>(p)) {
      c.subscribe(List.of(topic));
      while (count<expected) {
        ConsumerRecords<byte[],byte[]> rs=c.poll(Duration.ofSeconds(5));
        if (rs.isEmpty() && System.currentTimeMillis()-t0>90000) break;
        for (ConsumerRecord<byte[],byte[]> r: rs) { TopicPartition tp=new TopicPartition(r.topic(),r.partition()); long n=next.getOrDefault(tp,0L);
          if (r.offset()<n) dups++; else if (r.offset()>n) gaps++; next.put(tp,Math.max(n,r.offset()+1)); count++; }
      }
      long consumeMs=System.currentTimeMillis()-t0;
      for (int i=0;i<3;i++) for (ConsumerRecord<byte[],byte[]> r: c.poll(Duration.ofMillis(500))) { TopicPartition tp=new TopicPartition(r.topic(),r.partition()); if (r.offset()<next.getOrDefault(tp,0L)) dups++; count++; }
      Set<TopicPartition> asg=c.assignment();
      c.seekToBeginning(asg);
      long posAfterSeek=asg.isEmpty()?-1:c.position(asg.iterator().next());
      ConsumerRecord<byte[],byte[]> first=null; long seekT=System.currentTimeMillis();
      for (int i=0;i<40 && first==null;i++) for (ConsumerRecord<byte[],byte[]> r: c.poll(Duration.ofMillis(500))) { first=r; break; }
      System.out.printf("topic=%s proto=%s mpr=%d partitions=%d count=%d expected=%d gaps=%d dups=%d consumeMs=%d | afterSeek: position=%d firstOffset=%s waitedMs=%d%n",
        topic,proto,mpr,asg.size(),count,expected,gaps,dups,consumeMs,posAfterSeek,first==null?"none":first.offset(),System.currentTimeMillis()-seekT);
    }
  }
}
