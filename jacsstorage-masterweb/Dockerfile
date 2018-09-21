# Build the jade agent container
FROM scientificlinux/sl:7
ARG SSH_PRIVATE_KEY
ARG TAG_JACS=master

LABEL description="JADE Master Service"

# Install git
RUN yum install -y git

# Setup SSH for github access
RUN mkdir /root/.ssh
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa

RUN chmod 700 /root/.ssh/id_rsa
RUN ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts

# Install JRE for running
RUN yum install -y java-1.8.0-openjdk-devel

RUN /bin/bash -c echo "export $(dirname $(dirname $(readlink $(readlink $(which javac)))))" > /etc/profile.d/java8.sh
RUN source /etc/profile.d/java8.sh

# Checkout the code
RUN mkdir /tmp/jade
WORKDIR /tmp/jade
RUN /bin/git clone git@github.com:JaneliaSciComp/jacs-storage.git .
RUN /bin/git checkout $TAG_JACS
# Build the code
RUN ./gradlew installDist

# remove the ssh keys as they are not needed anymore
RUN rm -rf /root/.ssh

# Install the app
RUN mkdir /app

RUN cp -a jacsstorage-masterweb/build/install/jacsstorage-masterweb /app/

# Create jacs user (these can be overridden)
ARG UNAME=jacs
ARG GNAME=jacsdata
ARG UID=1047
ARG GID=1070
RUN groupadd -g $GID $GNAME && \
    useradd -m -u $UID -g $GID -s /bin/bash $UNAME

RUN mkdir /tmp/jade/logs

RUN chown -R $UID:$GID /app
RUN chown -R $UID:$GID /tmp/jade/logs
 
# Switch to jacs user and configure the app
USER $UNAME

EXPOSE 8080

# Command for running the application
ENTRYPOINT ["/app/jacsstorage-masterweb/bin/jacsstorage-masterweb", "-b", "0.0.0.0", "-p", "8080"]
