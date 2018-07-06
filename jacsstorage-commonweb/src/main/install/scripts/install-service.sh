#!/bin/bash

serviceName="${jacs.runtime.env.serviceName}"
serviceVersion="${jacs.runtime.env.serviceVersion}"

yum -y remove ${serviceName}
yum -y install ${serviceName}/build/distributions/${serviceName}-${serviceVersion}-1.i386.rpm
systemctl daemon-reload
