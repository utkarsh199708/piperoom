# The fake host of the host-target setup breaks (U8c, dev/scenarios.md "Setup
# breaks: host target"). It plays a Linux host that runs Kafka Connect from an
# extracted distribution under a project directory, which is what the host
# target of the setup runner renders (internal/setup/targets/host, U8a): a JDK,
# the distribution's bin/ scripts, the systemd unit service_install writes and
# the worker files properties_write writes.
#
# Nothing here runs systemd or a Connect worker: devctl's checks read the same
# host files a setup run reads (the unit's ExecStart argv, the worker
# properties and the java the unit pins) through `docker exec`
# (cmd/devctl/setupbreaks_host.go). The real `systemctl --user` path is U8a's
# own integration test and U8-R's real runs.
#
# The JRE is the java the healthy checks read: Debezium 3.2 needs Java 17, and
# this is 21. jdk-11 is the old runtime `java-too-old` points the unit at.
FROM eclipse-temurin:21-jre

# The project as service_install and properties_write leave it. start.sh
# copies it to /piperoom-host/project at every start, so a container restart
# heals every break the three scenarios apply.
COPY base /opt/piperoom/host-base
# The old JDK `java-too-old` pins in the unit: a Java 11 runtime that answers
# `java -version` the way a host's JDK 11 does.
COPY jdk-11 /opt/piperoom/jdk-11
COPY start.sh /opt/piperoom/start.sh

# A Windows checkout carries no executable bit, and the distribution's
# launchers, the JDK shim and the start script must be executable in the image.
RUN chmod 0755 /opt/piperoom/start.sh /opt/piperoom/jdk-11/bin/java \
 && chmod 0755 /opt/piperoom/host-base/kafka/bin/connect-standalone.sh \
               /opt/piperoom/host-base/kafka/bin/connect-distributed.sh

CMD ["bash", "/opt/piperoom/start.sh"]
