FROM ioreg.azurecr.io/sbt-container:scala2.11.11-sbt0.13.15

WORKDIR /home/play

#break out the SBT dependency steps to help utilize docker cache
#https://medium.com/playmoweb-android-ios-development/speed-up-your-builds-with-docker-cache-bfed14c051bf#.jjywrdgnj
COPY build.sbt /home/play
COPY project /home/play/project

RUN sbt update

#build the app
COPY . /home/play
COPY resolve.conf.tpl /etc/resolv.conf

RUN sbt clean test stage

FROM openjdk:8-jre

COPY --from=0 /home/play/target/universal/stage /home/play/

COPY --from=0 /var/java_sizeit.sh .

CMD . ./java_sizeit.sh && /home/play/bin/sensors -Dplay.crypto.secret=thisisevenmoresecretthanthelastone
