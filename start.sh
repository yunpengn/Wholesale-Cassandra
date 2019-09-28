#!/usr/bin/env bash

project_path=$(dirname $(realpath $0))
cql_path="$project_path/src/main/resources"
data_path="$project_path/data/data-files"

create_schema() {
    cd $project_path
    cqlsh -f src/main/resources/schema.cql --request-timeout=3600
}

load_data() {
    cd $data_path
    file="order-line.csv"
    modified="modified-order-line.csv"

    if [[ -f $modified ]]; then
        rm $modified
    fi

    sed 's:,null,:,,:g' $file > $modified

    cd $project_path
    cqlsh -f src/main/resources/loaddata.cql

    cd $data_path
    rm $modified
}


if [[ "$1" == "createschema" ]]; then
    create_schema
elif [[ "$1" == "loaddata" ]]; then
    load_data
else
    echo "unknown command"
fi
