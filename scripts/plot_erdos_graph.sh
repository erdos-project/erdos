#!/bin/bash
# $1 path to the gv file
#
# Example:
# $ plot_erdos_graph.sh erdos.gv # plots erdos.png

filename=$(basename -- "$1")
extension="${filename##*.}"
filename="${filename%.*}"

filepath=$(dirname "$1")
dot -Tpng $1 > $filepath/$filename.png
