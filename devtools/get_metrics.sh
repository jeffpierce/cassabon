#!/bin/sh
# Get five minutes of stresstest metrics for the period ending now().

TO=`date +%s`

case $1 in
    d)
        FROM=$(($TO - 86450))
        ;;
    h)
        FROM=$(($TO - 3600))
        ;;
    *)
        FROM=$(($TO - 300))
        ;;
esac

echo "Start of query range: `date -j -r $FROM`"
echo "curl -X GET" \""127.0.0.1:8080/metrics?path=foo.bar.baz.count&path=foo.bar.baz.min&path=foo.bar.baz.max&path=foo.bar.baz.sum&path=foo.bar.baz.average&from=$FROM&to=$TO"\"
curl -X GET "127.0.0.1:8080/metrics?path=foo.bar.baz.count&path=foo.bar.baz.min&path=foo.bar.baz.max&path=foo.bar.baz.sum&path=foo.bar.baz.average&from=$FROM&to=$TO"
echo ""
