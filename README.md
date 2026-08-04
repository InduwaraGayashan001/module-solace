## Ballerina Solace connector 

[Solace PubSub+](https://docs.solace.com/) is an advanced event-broker platform that enables event-driven communication across distributed applications using multiple messaging patterns such as publish/subscribe, request/reply, and queue-based messaging. It supports standard messaging protocols, including JMS, MQTT, AMQP, and REST, enabling seamless integration across diverse systems and environments.

The `xlibb/solace` package provides APIs to interact with Solace PubSub+ brokers through the JCSMP API. It allows developers to programmatically produce and consume messages, manage topics and queues, and implement robust, event-driven solutions that leverage Solace’s high-performance messaging capabilities within Ballerina applications.

## Examples

The [examples](examples) directory contains complete, runnable samples:

- [Order listener](examples/order-listener) — asynchronous (push-based) message consumption from a queue using the `solace:Listener`, with explicit acknowledgement through the `solace:Caller`.

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 21. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Download and install [Docker](https://www.docker.com/get-started).

   > **Note**: Ensure that the Docker daemon is running before executing any tests.

4. Export Github Personal access token with read package permissions as follows,

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

   ```bash
   ./gradlew clean build
   ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build the without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To run tests against different environments:

   ```bash
   ./gradlew clean test -Pgroups=<Comma separated groups/test cases>
   ```

5. To debug the package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

6. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

7. Publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

8. Publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```
