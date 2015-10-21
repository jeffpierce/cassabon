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

for p in foo.bar.baz.count foo.bar.baz.min foo.bar.baz.max foo.bar.baz.sum foo.bar.baz.average; do
    echo "curl -X GET" \""127.0.0.1:8080/metrics?path=${p}&from=$FROM&to=$TO"\"
    curl -X GET "127.0.0.1:8080/metrics?path=${p}&from=$FROM&to=$TO"
    echo ""
done
