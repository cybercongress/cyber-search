# Development environment

## Prestart
* Instal Java 8 JDK
* Install Docker and Docker Compose
* Install Intellij Idea

## Run Kafka, Elassandra, Prometheus and Grafana
### Start containers(required)
For mac:
```bash
docker-compose -f dev-environment/env-mac.yml up -d
```
For linux family:
```bash
docker-compose -f dev-environment/env.yml up -d
```
### Bootstrap Elassandra with keyspaces(required)
```bash
docker cp dev-environment/elassandra-bootstrap.cql  elassandra-search:/elassandra-bootstrap.cql
docker exec -it elassandra-search bash
cqlsh -f elassandra-bootstrap.cql
```
Then copy paste all content from elassandra-bootstrap.cql to interactive console and press enter. Write twice exit to 
 exit from container shell.

### Stop kafka and delete kafka data(cheat sheet)
```bash
docker stop fast-data-dev-search
docker rm fast-data-dev-search
```
### Stop elassandra and delete elassandra data(cheat sheet)
```bash
docker stop elassandra-search
docker rm elassandra-search
```

## Start required chain nodes(Examples)
### Run parity node(cheat sheet)
```bash
sudo  docker run -d -p 8545:8545 --name parity_eth \
-v ${REPLACE_IT_BY_HOST_FOLDER}:/cyberdata parity/parity:stable \
--db-path /cyberdata --jsonrpc-hosts all --jsonrpc-interface all --jsonrpc-threads 4
```

### Access chains from remote machine(mars)(cheat sheet)
```bash
ssh -L 18332:localhost:18332 -L 8332:localhost:8332 \
-L 18545:localhost:18545 -L 8545:localhost:8545 \
mars@staging.cyber.fund  -p 33322
```

## Import project to Intellij Idea
Open Project in idea by selecting: Import Project -> selecting build.gradle file from the repository root
![Select Build Gradle](images/select-build-gradle.png)
![Import Settings](images/gradle-settings.png)
Wait for dependency downloading and indexation

## Run Pump, Dump, or Contract Summary from intellij Idea
Go to EthereumPumpApplication.kt and press green triangle on left to the code (on example line 23):
![Start Pump](images/start-pump.png)

Pump will fail due to lack of environment property CHAIN, let's define it: Select "Edit Configuration"

![Select Edit Run Configuration](images/select-edit-configuration.png)

Add next properties:

![Add variables](images/add-environment-variables.png)

Now, run pump one more time, it should start. If, you use parity endpoint different rather that localhost:8545, than add
 one more env variable called **CHAIN_NODE_URL**. Same for Dumps, Contract Summaries and etc.
