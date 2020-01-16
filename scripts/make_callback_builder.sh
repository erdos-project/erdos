#!/bin/bash

function usage {
    echo "Usage: $(basename $0) max_read_streams max_write_streams"
}

if [ $1 == "-h" ]
then
    usage
    exit 0
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR
python3 make_callback_builder.py > ../src/dataflow/callback_builder.rs build $1 $2
rustfmt ../src/dataflow/callback_builder.rs
