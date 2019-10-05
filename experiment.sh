#!/bin/bash

# Schedules a job on a certain machine.
schedule_job() {
  jobID=$1
  echo "Begins to schedule job with job ID=$((jobID + 1)):"

  machineID="xcnd$((25 + $jobID % 5))"
  echo "The job will be scheduled on machine IP=${machineID}."

  input_file="data/xact-files/$((jobID + 1)).txt"
  stdout_file="log/$((jobID + 1)).out.log"
  stderr_file="log/$((jobID + 1)).err.log"
  ssh $machineID "cd /temp/cs4224f/Wholesale-Cassandra && java -jar build/libs/Wholesale-Cassandra-1.0-SNAPSHOT-all.jar < ${input_file} > ${stdout_file} 2> ${stderr_file} &" > /dev/null &
  echo "Have runned job ID=$((jobID + 1)) with input from ${input_file}."
}

# Schedules an experiment by running a certain number of jobs.
schedule_experiment() {
  # Cleans on each machine.
  for ((c=0; c<5; c++)); do
    machineID="xcnd$((25 + $c % 5))"
    ssh $machineID "cd /temp/cs4224f/Wholesale-Cassandra && rm -rf log && mkdir log"
    echo "Have built on machine ID=${machineID}."
  done

  # Schedules job one-by-one.
  for ((c=0; c<$1; c++)); do
    schedule_job $c
  done
}

schedule_build() {
  # Builds on each machine.
  for ((c=0; c<5; c++)); do
    machineID="xcnd$((25 + $c % 5))"
    ssh $machineID "cd /temp/cs4224f/Wholesale-Cassandra && git pull && ./gradlew shadowJar"
    echo "Have built on machine ID=${machineID}."
  done
}

# Schedules an experiment.
if [[ $1 == "" ]]; then
  echo "Please specify parameter."
  exit
fi
echo "Begins an experiment with size=$1"
schedule_experiment $1

