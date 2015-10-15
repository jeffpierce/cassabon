#!/bin/sh
# Get five minutes of stresstest metrics for the period ending now().

TO=`date +%s`
FROM=$(($TO - 300))

curl -X GET "localhost:8080/metrics?path=foo.bar.baz.count&path=foo.bar.baz.min&path=foo.bar.baz.max&path=foo.bar.baz.sum&path=foo.bar.baz.average&from=$FROM&to=$TO"
echo ""
