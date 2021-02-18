#!/bin/bash
echo "Create a local postgres database.."
if [ -z $1 ]; then
        echo "Input path not provided, aborting program"
        exit 0
fi
createdb -U postgres $1
echo "Database created: $1"
