# Order listener — asynchronous message consumption

Consumes messages from a Solace queue **asynchronously** using the `xlibb/solace` `Listener`
(push-based delivery via `onMessage`, with explicit acknowledgement through the `Caller`).
On startup the example also publishes a few sample messages, so a single `bal run` demonstrates
the complete flow:

```
producer -> broker queue -> Listener -> onMessage(message, caller) -> caller->ack
```

## Prerequisites

- Ballerina Swan Lake 2201.12.0 or later
- Docker (for a local Solace PubSub+ broker), or access to any Solace broker / Solace Cloud service

## 1. Start a Solace broker

```bash
docker run -d --name solace-example \
  -p 55554:55555 -p 8080:8080 \
  --shm-size=1g \
  -e username_admin_globalaccesslevel=admin -e username_admin_password=admin \
  solace/solace-pubsub-standard:latest
```

Wait until the message VPN is operational (takes ~30 seconds — SEMP responds before guaranteed
messaging is ready):

```bash
until [ "$(curl -s -u admin:admin \
  'http://localhost:8080/SEMP/v2/monitor/msgVpns/default?select=state' \
  | grep -o '"state":"[^"]*"')" = '"state":"up"' ]; do echo "waiting for VPN..."; sleep 3; done
echo "broker ready"
```

## 2. Provision the queue

```bash
curl -X POST -u admin:admin -H 'Content-Type: application/json' \
  http://localhost:8080/SEMP/v2/config/msgVpns/default/queues \
  -d '{"queueName":"demo/orders","accessType":"exclusive","permission":"delete","ingressEnabled":true,"egressEnabled":true}'
```

(With Solace Cloud, create a queue named `demo/orders` from the broker console instead, and set
the connection values via `Config.toml` — see below.)

## 3. Configure (optional)

The defaults match the local Docker broker above. To point at another broker, create a
`Config.toml`:

```toml
brokerUrl = "tcps://<host>.messaging.solace.cloud:55443"
vpnName = "<your-vpn>"
username = "<username>"
password = "<password>"
```

## 4. Run

```bash
bal run
```

Expected output:

```
[init] Published 3 sample message(s) to queue 'demo/orders'
[init] Listening on queue 'demo/orders' at tcp://localhost:55554. Press Ctrl+C to stop.
[onMessage] received: 'Order-1'
[onMessage] acknowledged: 'Order-1'
[onMessage] received: 'Order-2'
[onMessage] acknowledged: 'Order-2'
[onMessage] received: 'Order-3'
[onMessage] acknowledged: 'Order-3'
```

The listener keeps running — publish more messages any time (for example through another producer,
or the broker's "Try Me!" console) and they appear in `onMessage` as they arrive.

## Variations

- **Automatic acknowledgement**: set `ackMode: solace:AUTO_ACK` in `@solace:ServiceConfig` and drop
  the `Caller` parameter — messages are acknowledged automatically after `onMessage` returns
  successfully.
- **Direct topic subscription** (at-most-once): use `topicName: "orders/>"` instead of `queueName`.
- **Durable topic endpoint**: use `topicName` with `endpointType: solace:DURABLE` and an
  `endpointName`.
- **Negative acknowledgement**: call `caller->nack(message, requeue = true)` to have the broker
  redeliver the message, or `requeue = false` to route it to the dead message queue.

## Testing against an unreleased build of the module

To run this example against a locally-built `xlibb/solace` (instead of the release on Ballerina
Central), publish the package to your local repository first:

```bash
cd ../../ballerina
bal pack && bal push --repository=local
```

and add the following to this example's `Ballerina.toml`:

```toml
[[dependency]]
org = "xlibb"
name = "solace"
version = "0.5.0"
repository = "local"
```
