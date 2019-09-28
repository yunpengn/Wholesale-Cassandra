# Wholesale Cassandra

This is the Wholesale project implemented with Cassandra. It is part of the requirements for the module [CS4224 Distributed Databases](https://nusmods.com/modules/CS4224/distributed-databases) at the [National University of Singapore](http://www.nus.edu.sg).

## Setup Development Environment

- Install the latest version of [IntelliJ IDEA](https://www.jetbrains.com/idea/).
- Clone the repository by `git clone git@github.com:yunpengn/Wholesale-Cassandra.git`.
- Click `Import Project` and select `build.gralde`.
- Wait for Gradle to complete the setup.

## Run Project on Server

- Clone the repository by `git clone git@github.com:yunpengn/Wholesale-Cassandra.git`.
- Go to the folder by `cd Wholesale-Cassandra`
- Build the project by `./gradlew shadowJar` (this command may take some time for the first time).
- Run the generated JAR by `java -jar build/libs/Wholesale-Cassandra-1.0-SNAPSHOT-all.jar`.

## Create Schema & Import Data

- Download data from [here](https://www.comp.nus.edu.sg/~cs4224/project-files.zip) to the `data/` folder.
- Unzip the downloaded file.
- Create the schema by `cqlsh -f src/main/resources/schema.cql`.
- Import data by `cqlsh -f src/main/resources/loaddata.cql`.

## Licence

[GNU General Public Licence 3.0](LICENSE)
