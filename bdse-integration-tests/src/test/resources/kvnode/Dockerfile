# https://hub.docker.com/_/java/

FROM openjdk:8-jdk-alpine
VOLUME /tmp

# I honestly don't know how to run docker under docker.
# Just specifing `-v /var/run/docker.sock:/var/run/docker.sock` does not work.
# So install alpine docker client inside of the container.
RUN apk update && apk add docker

ADD target/bdse-kvnode-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app.jar", "ru.csc.bdse.Application"]