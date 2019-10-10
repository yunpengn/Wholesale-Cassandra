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

## Setup Server Environment

- For each server that will be a Cassandra node:
    - Make sure you have installed Java 8 on the server.
    - Follow the guide [here](http://cassandra.apache.org/doc/latest/getting_started/installing.html) to install Cassandra.
    - Modify `cassandra.yaml` accordingly to fit your use cases.
    - Start Cassandra.
- Clone the repository by `git clone git@github.com:yunpengn/Wholesale-Cassandra.git` to the servers where the Java application will be run.

## Run Project on Server

- To create the database schema:
    - Login to any Cassandra node and run `./start.sh createschema`.
- To import data:
    - Download data from [here](https://www.comp.nus.edu.sg/~cs4224/project-files.zip).
    - Unzip the downloaded file and put extracted folders (`data-files/` & `xact-files/`) under `data/` folder.
    - Run `./start.sh loaddata`.
- To run a single Java instance:
    - Use the command `./start.sh run xact_filename consistency_level` where `xact_filename` is the path of input transaction file and `consistency_level` is the consistency level.
- To conduct an experiment:
    - Make sure you are in a shell of which you have access to all 5 Cassandra nodes.
    - Run `./experiment.sh build` to pull the latest code and compile the Java instances.
    - Run `./experiment.sh run NC consistency_level` to conduct an experiment with `NC` number of clients and `consistency_level` being the consistency level.
- After an experiment is done:
    - Retrieve the output from all servers by `scp -r cs4224f@xcnd??:/temp/cs4224f/Wholesale-Cassandra/log/*.err.log ~/Downloads/log/`.
    - Calculate Statistic by `./start.sh st path_of_log_folder NC` where `path_of_log_folder` is the path to the folder in which `*.err.log` are saved and `NC` is the number of Java instances in the experiment.

## Check your SSH connection to all 5 servers

- We assume you have SSH access to all 5 nodes: `xcnd25`, `xcnd26`, `xcnd27`, `xcnd28` and `xcnd29`. This has been enforced in the experiment script. Otherwise, you will not be able to conduct an experiment using the given script.
- To obtain SSH access to these 5 nodes:
    - Put your public key in the file `/home/stuproj/cs4224f/.ssh/authorized_keys`.
    - In your local computer, add the following lines to `~/.ssh/config`:

```
Host xcnd25
    HostName <ip_or_domain_name>
    IdentityFile ~/.ssh/<private_key>
    User cs4224f

Host xcnd26
    HostName <ip_or_domain_name>
    IdentityFile ~/.ssh/<private_key>
    User cs4224f

Host xcnd27
    HostName <ip_or_domain_name>
    IdentityFile ~/.ssh/<private_key>
    User cs4224f

Host xcnd28
    HostName <ip_or_domain_name>
    IdentityFile ~/.ssh/<private_key>
    User cs4224f

Host xcnd29
    HostName <ip_or_domain_name>
    IdentityFile ~/.ssh/<private_key>
    User cs4224f
```

## Licence

[GNU General Public Licence 3.0](LICENSE)
