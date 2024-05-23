# MAPE Loop
## What is a MAPE Loop?
The MAPE Loop is a architectural pattern that is used to monitor, analyze, plan, and execute actions on a system. This repository contains the implementation of a MAPE Loop for the purpose of managing a distributed storage system. The MAPE Loop is implemented in Java and uses ActiveMQ for phase communication.
## What is the purpose of this MAPE implementaion?
## Software Requirements
This implementation is built using Java 17 and Maven. <br>
The following dependencies are additionaly required to run the MAPE Loop:
1. Apache ActiveMQ [Version 5.18.3](https://activemq.apache.org/components/classic/download/) for phase communication
2. Apache Kafka Broker (***optional***, if run in demo mode) [Version 3.7.0](https://kafka.apache.org/downloads) for data streaming
3. smartmontools (only on machines running Linux) [Version 7.2](https://www.smartmontools.org/) for disk monitoring
4. dCache (only on machines running Linux) [Version > 9.2.0](https://github.com/dCache/dcache) as storage system
