 <h1> Introduction</h1>
 <p> This is a very simple example to show how register a custom listener by RTopic(Distributed topic 
 messages are delivered to all message listeners across Redis cluster) to monitor every RLocalCachedMap 
 instance.This listener can get the KEY and VALUE this in target map directly and basically replace 
 the map listener of RLocalCachedMapCache(available only in Redisson PRO edition) for RLocalCachedMap(open source edition).</p>
 <p> FYI @see https://github.com/redisson/redisson/wiki/7.-distributed-collections</p>
        
