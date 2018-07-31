
Reproduction for defect ARTEMIS-1993.  Defect reproduced against 2.5.0 and master (f0c13622ac7e821a81a354b0242ef5235b6e82df)

1. Create a fresh Artemis Broker

```
artemis create /some/where --user admin --password admin --allow-anonymous
```

2. Edit ```/some/where/etc/broker.xml``` and add the following line to the catch all address setting.  Also within
```artemis.profile``` increase the Xmx parameter from 2 to 4g.

``` 
<max-delivery-attempts>-1</max-delivery-attempts>
```

3. Start the Broker

```
"/some/where/bin/artemis" run
```

4. Create an anycast queue 'queue'

```
curl --user admin:admin 'http://localhost:8161/console/jolokia/exec/org.apache.activemq.artemis:broker=%220.0.0.0%22/createQueue(java.lang.String,java.lang.String,boolean,java.lang.String)/queue/queue/false/ANYCAST/'
```

5. Run this reproduction

```
mvn clean install exec:java
```

6. Tail the log file and greping for ArrayIndexOutOfBoundsException.  The reproduction relies on vagaries of thread scheduling within the Broker.  On my machine,
the issue occurs within 5-10mins. YMMD.  Also note the occurence of the AMQ222151 immediately before the ArrayIndexOutOfBoundsException.

```
tail f /some/where//artemis.log  | grep ArrayIndexOutOfBoundsException.
```



