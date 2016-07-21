#!/bin/bash

base_dir=$(dirname $0)/..

CLASSPATH=$CLASSPATH:$base_dir/build/libs

java kafka.examples.ConsumerProducerAvroDemo1 $@
