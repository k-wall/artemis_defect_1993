/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kwall.artemis1993;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.impl.NetSocketImpl;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.impl.ProtonConnectionImpl;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.message.Message;

public class Artemis1993
{

    private static final int NUMBER_TO_FILL = 2000;

    private final String _host;
    private final int _port;
    private final String _address;

    public static void main(String[] args) throws Exception
    {

        final String host = "localhost";
        final int port = 5672;
        final String address = "queue";

        Artemis1993 artemis1993 = new Artemis1993(host, port, address);

        artemis1993.doTest(artemis1993);
    }

    public Artemis1993(final String host, final int port, final String address)
    {
        _host = host;
        _port = port;
        _address = address;
    }

    private void doTest(final Artemis1993 artemis1993) throws Exception
    {
        artemis1993.fillQueue();

        int numberOfConsumerThreads = 2;
        ExecutorService executors = Executors.newFixedThreadPool(numberOfConsumerThreads);

        for (int i = 0; i < numberOfConsumerThreads; i++)
        {
            executors.submit(artemis1993.consumeAndReleaseForever("consumer" + i));
        }

        executors.shutdown();
        executors.awaitTermination(1, TimeUnit.DAYS);
    }

    private void fillQueue() throws Exception
    {
        final CountDownLatch latch = new CountDownLatch(NUMBER_TO_FILL);
        Vertx vertx = Vertx.vertx();

        // Create the Vert.x AMQP client instance
        ProtonClient client = ProtonClient.create(vertx);

        // Connect, then use the event loop thread to process the connection
        client.connect(_host, _port, res -> {
            if (res.succeeded())
            {

                ProtonConnection connection = res.result();
                connection.open();

                ProtonSender sender = connection.createSender(_address);
                ((Target) sender.getTarget()).setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

                sender.open();

                System.out.println("Awaiting " + latch.getCount());

                sender.sendQueueDrainHandler(event -> {

                    for (int i = 0; i < NUMBER_TO_FILL; i++)
                    {
                        Message message = Proton.message();
                        message.setBody(new AmqpValue(i));
                        sender.send(message, delivery -> {
                            latch.countDown();
                        });
                    }
                });
            }
            else
            {
                throw new RuntimeException(res.cause());
            }
        });

        latch.await();
        vertx.runOnContext(event -> {
            System.out.format("%d messages put on queue: %s%n", NUMBER_TO_FILL, _address);
        });
        vertx.close();
    }

    private Runnable consumeAndReleaseForever(final String threadName)
    {
        return () -> {
            while (true)
            {
                try
                {
                    consumeReleasingForRandomPeriodThenAbort(threadName);
                }
                catch (Exception e)
                {
                    System.err.println(String.format("Ignored exception %s", e.getClass().getSimpleName()));
                }
            }
        };
    }

    private void consumeReleasingForRandomPeriodThenAbort(final String consumerName)
    {
        Vertx vertx = null;
        try
        {
            final CompletableFuture<ProtonConnection> future = new CompletableFuture<>();

            vertx = Vertx.vertx();

            ProtonClient client = ProtonClient.create(vertx);

            client.connect(_host, _port, (AsyncResult<ProtonConnection> res) -> {
                if (res.succeeded())
                {
                    ProtonConnection connection = res.result();
                    connection.open();

                    final AtomicInteger releasedCount = new AtomicInteger();

                    ProtonReceiver protonReceiver = connection
                            .createReceiver(_address)
                            .setAutoAccept(false)
                            .setPrefetch(5)
                            .handler((delivery, msg) -> {
                                if (!future.isDone())
                                {
                                    double random = Math.random();
                                    if (random > 0.99d)
                                    {
                                        System.out.format(
                                                "%s: Forced socket closed after %d release(s)%n",
                                                consumerName,
                                                releasedCount.get());
                                        future.complete(connection);
                                    }
                                    else
                                    {
                                        releasedCount.incrementAndGet();
                                        delivery.disposition(Released.getInstance(), true);
                                    }
                                }
                            });
                    ((Source) protonReceiver.getSource()).setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

                    protonReceiver.open();
                }
                else
                {
                    throw new RuntimeException(res.cause());
                }
            });

            ProtonConnection connection = future.get();
            // Want to disconnect the socket -- surely there is a better way??
            NetSocket netSocket = getSocketFrom(connection);
            netSocket.close();
            Channel channel = ((NetSocketImpl) netSocket).channelHandlerContext().channel();
            channel.close();
            vertx.close();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (vertx != null)
            {
                vertx.close();
            }
        }
    }

    private NetSocket getSocketFrom(final ProtonConnection connection)
    {
        try
        {
            Field transportField = ProtonConnectionImpl.class.getDeclaredField("transport");
            transportField.setAccessible(true);
            Field socketField = Class.forName("io.vertx.proton.impl.ProtonTransport").getDeclaredField("socket");
            socketField.setAccessible(true);
            Object transport = transportField.get(connection);
            return (NetSocket) socketField.get(transport);
        }
        catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }
}