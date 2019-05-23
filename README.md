# DSPA 2019 - Semester Project

Course Project for Data Stream Processing and Analytics FS2019

How to SETUP the project:

Prerequisites:
- git
- Java version 8 (tested with java version 1.8.0_212 java openjdk amd64)
- Maven (tested on Maven 3.6.0)
For Flink, Scala binary on version 2.11:
- Kafka 2.11-2.1.0: This is very important, currently Kafka is on version
2.2.0, but at the beginning of the semester it was on version 2.1.0. There
has been some changes in the properties for connecting to zookeeper and we
never changed them. With Kafka 2.1.0 it should run well.
- Zookeeper, should be included in kafka download.


Setup:
1. Checkout repository from gitlab. Download the data.

2. Install Kafka into a custom directory. Note that we assume it will run
on the same machine the same as we had in the exercise sessions.

3. Open terminal on the same level as the pom.xml (and this README) and run:

mvn clean package

This should download the necessary libraries automatically.

4. Unzip data into a well known location. Rename that folder to "input" and
on the same level create a new folder "working". You should end up with
something like this:

/root/data/
          /input/
                /streams/
                /tables/
          /working/

Our code will copy everything from input to working directory and make some
small changes. If you need to make changes to the data, change them in the
input and NOT in the working directory. Everything in the working directory
is deleted.

5. Open config.properties which is located in the folder with pom.xml (and
this README). First change inputDirectory and workingDirectory according to
the last step (4). Check the field writeToKafka, it should be true if not
change it. The configuration file also gives access to the assignment
specific configuration which we don't have to change right now.

6. Copy the scripts from tools/kafka into the kafka installation folder. Run
the following scripts in this order: startup_script.sh ; create_topics.sh ;

7. Run main class src/main/java/main/Main once. This will take a while as it
now writes all the data from the input directory into Kafka.

8. Since we don't want to write to Kafka all the time we change the according
configuration in config.properties: writeToKafka = false.

Second Startup:
When you start Kafka a second time, for example with the scripts, then you only
need to check 5-8 again. Make sure that you write at least once to Kafka every
time you shutdown Kafka. When you do not shut down Kafka you don't need to do
any of these steps at all.

How to run: