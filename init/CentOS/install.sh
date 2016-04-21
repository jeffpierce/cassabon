#!/bin/sh

GROUP=cassabon

# Application configuration
#install --owner=root --group=root --mode=0644 cassabon.yaml /etc/cassabon.yaml

# Service details
install --owner=root --group=root --mode=0644 sysconfig /etc/sysconfig/cassabon
install --owner=root --group=root --mode=0755 initscript /etc/init.d/cassabon
install --owner=root --group=root --mode=0644 logrotate /etc/logrotate.d/cassabon

# Health check file
install --owner=root --group=root --mode=0755 -d /var/db/cassabon
install --owner=root --group=root --mode=0644 health /var/db/cassabon

# Log directory, writable by Cassabon user
install --owner=root --group=$GROUP --mode=0775 -d /var/log/cassabon

# Application binary, executable by Cassabon user
#install --owner=root --group=$GROUP --mode=0750 cassabon /usr/local/bin/cassabon
