FROM alpine:latest AS kafka_dist

ARG scala_version=2.13
ARG kafka_version=3.5.0
ARG kafka_distro_base_url=https://dlcdn.apache.org/kafka

ENV kafka_distro=kafka_$scala_version-$kafka_version.tgz
ENV kafka_distro_asc=$kafka_distro.asc

RUN apk add --no-cache gnupg gnupg-keyboxd

WORKDIR /var/tmp

RUN wget -q $kafka_distro_base_url/$kafka_version/$kafka_distro
RUN wget -q $kafka_distro_base_url/$kafka_version/$kafka_distro_asc
RUN wget -q $kafka_distro_base_url/KEYS

RUN gpg --import KEYS
RUN gpg --verify $kafka_distro_asc $kafka_distro

RUN tar -xzf $kafka_distro 
RUN rm -r kafka_$scala_version-$kafka_version/bin/windows

RUN wget https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/1.9.1/snowflake-kafka-connector-1.9.1.jar -P kafka_$scala_version-$kafka_version/libs/ 
RUN wget https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/1.0.1/bc-fips-1.0.1.jar -P kafka_$scala_version-$kafka_version/libs/
RUN wget https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/1.0.3/bcpkix-fips-1.0.3.jar -P kafka_$scala_version-$kafka_version/libs/

FROM openjdk:11-jre-slim

ARG scala_version=2.13
ARG kafka_version=3.5.0

ENV KAFKA_VERSION=$kafka_version \
    SCALA_VERSION=$scala_version \
    KAFKA_HOME=/opt/kafka

ENV PATH=${PATH}:${KAFKA_HOME}/bin

RUN mkdir ${KAFKA_HOME} && apt-get update && apt-get install curl -y && apt-get clean

COPY --from=kafka_dist /var/tmp/kafka_$scala_version-$kafka_version ${KAFKA_HOME}

RUN echo $pwd

COPY SF_connect.properties ${KAFKA_HOME}/config/
COPY sf-connect-standalone.properties ${KAFKA_HOME}/config/

RUN chmod a+x ${KAFKA_HOME}/bin/*.sh

CMD ["/opt/kafka/bin/connect-standalone.sh", "/opt/kafka/config/sf-connect-standalone.properties", "/opt/kafka/config/SF_connect.properties"]