# cassabon
The upstream repo for Change Cassabon is [cassabon][https://github.com/jeffpierce/cassabon/], which is an open source project supported by Change.org

[![Circle CI](https://circleci.com/gh/change/cassabon/tree/master.svg?style=svg)](https://circleci.com/gh/change/cassabon/tree/master)

Carbon daemon using Cassandra as the backend, implemented in Go.

[GoDoc](https://godoc.org/github.com/change/cassabon)

## What does cassabon do?

Cassabon receives your carbon metrics and stores them in Cassandra (hence the name, Cassabon), which makes it a lot easier to scale your time-series metrics infrastructure since you no longer need to re-hash a graphite ring to add more capacity!  If you need more stats listeners, bring up more Cassabon instances.  If you need more stats storage, bring up more Cassandra instances.  That simple!

It also acts as an API for Graphite or graphite-api (using the Cyanite reader) to retrieve the stats to display.

## How do I use Cassabon?

* Build it.
* Fill out the cassabon.yaml configuration file by giving it the addresses of the Elasticsearch and Cassandra servers you're using.
* Start it up!

...okay, it's a little more in depth than that. Documentation can be found in the project wiki, although it's currently a work in progress.

## Can I contribute to Cassabon?

Of course! We welcome contributions to Cassabon.

## Can I send pickle-format stats to Cassabon?

Not yet, but Soon(TM).

## How can I monitor Cassabon's performance?

Cassabon sends out stats about how it peforms via statsd.  Simply configure your statsd server in the cassabon.yaml file, and you'll get a wealth of time-series metrics about its performance!

## What software does Cassabon require?

Cassabon requires Elasticsearch and Cassandra to function.  It's tested with ElasticSearch 1.7 and Cassandra 2.0.14, but should work with all versions of both pieces of software newer than that.  If you're using graphite or graphite-API to pull stats from Cassabon, you'll need to install the cyanite reader to do so.

## Credits

A big shoutout to [Change.org](https://change.org), who sponsored the development of this software, and to [@pyr](https://github.com/pyr), author of the [Cyanite](https://github.com/pyr/cyanite) project, as Cassabon wouldn't be a thing without him having the idea to use Cassandra as a storage backend for carbon-style time series data.
