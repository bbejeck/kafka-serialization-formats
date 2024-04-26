#!/bin/zsh

for SCHEMA in "$@"
 do
   echo "Generating code for flatbuffer $SCHEMA"
   flatc  --java -o generated-flatbuffers/ --gen-mutable --schema src/main/flatbuffers/"$SCHEMA"
done