#!/bin/sh
# The standalone launcher of the Kafka distribution this fake host runs from
# (a real one is <dist>/bin/connect-standalone.sh, which starts a JVM with the
# worker properties passed as $1). U8c only reads the unit's argv; this stub
# prints it so a person can see what the unit would run.
echo "connect-standalone.sh $*"
