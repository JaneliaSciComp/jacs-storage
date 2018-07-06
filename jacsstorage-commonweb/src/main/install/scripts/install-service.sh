#!/bin/bash

serviceName=$1
serviceVersion=$2

yum -y remove ${serviceName}
yum -y install ${serviceName}/build/distributions/${serviceName}-${serviceVersion}-1.i386.rpm
systemctl daemon-reload
