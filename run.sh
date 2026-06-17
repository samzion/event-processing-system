#!/bin/bash
rm -rf out && mkdir -p out
find src/main/java -name "*.java" | xargs javac -d out
java -cp out com.eventprocessor.MainForEventService
