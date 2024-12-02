#!/bin/bash

./gradlew build

./graldew runStreams -Pargs=basic

./gradlew runStreams -Pargs=ktable # There's a single output result became the sample data has the same key.

./gradlew runStreams -Pargs=joins

./gradlew runStreams -Pargs=aggregate