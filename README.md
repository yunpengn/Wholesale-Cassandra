# Wholesale Cassandra

This is the Wholesale project implemented with Cassandra. It is part of the requirements for the module [CS4224 Distributed Databases](https://nusmods.com/modules/CS4224/distributed-databases) at the [National University of Singapore](http://www.nus.edu.sg).

[This repository](https://github.com/yunpengn/Wholesale-Cassandra) presents our approach to the project. Our team consists of

- [Niu Yunpeng](https://github.com/yunpengn)
- [Wang Junming](https://github.com/junming403)
- [Xiang Hailin](https://github.com/Hailinx)

## Setup Development Environment

- Install the latest version of [IntelliJ IDEA](https://www.jetbrains.com/idea/).
- Clone the repository by `git clone git@github.com:yunpengn/Wholesale-Cassandra.git`.
- Click `Import Project` and select `build.gradle`.
- Wait for Gradle to complete the setup.

## Run Project on Server

- Clone the repository by `git clone git@github.com:yunpengn/Wholesale-Cassandra.git`.
- Run command `./start.sh run xact_filename` where `xact_filename` is the path of input transaction file.

## Create Schema & Import Data

- Download data from [here](https://www.comp.nus.edu.sg/~cs4224/project-files.zip) to the `data/` folder.
- Unzip the downloaded file.
- Create the schema by `./start.sh createschema`.
- Import data by `./start.sh loaddata`.

## Licence

[GNU General Public Licence 3.0](LICENSE)
