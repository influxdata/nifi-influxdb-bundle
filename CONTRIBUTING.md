# Contributing to InfluxDB-Apache NiFi Processor Bundle

## Bug reports

Before you file an issue, please search existing issues in case it has already been filed, or perhaps even fixed. If you file an issue, please include the following.

* Full details of your operating system (or distribution) e.g. 64-bit Ubuntu 14.04.
* The version of Apache NiFi and the version of InfluxDB you are running
* Whether you installed it using a pre-built package, or built it from source.
* A small test case, if applicable, that demonstrates the issues.
* A screenshot, when appropriate, to demonstrate any issues that are related to the UI

Remember the golden rule of bug reports: 
**The easier you make it for us to reproduce the problem, the faster it will get fixed.**
If you have never written a bug report before, or if you want to brush up on your bug reporting skills, 
we recommend reading [Simon Tatham's essay "How to Report Bugs Effectively."](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html)

Please note that issues are _not the place to file general questions_ such as "How do I use the InfluxDB-NiFi bundle?" 
Questions of this nature should be sent to the [InfluxDB Community Forum](https://community.influxdata.com/t/influxdb-apache-nifi-bundle/), not filed as issues. Issues like this will be closed.

## Feature requests

We really like to receive feature requests, as it helps us prioritize our work. 
Please be clear about your requirements, as incomplete feature requests may simply be closed if we don't understand what you would like to see added to the project.

## Contributing to the source code

The InfluxDB-Apache NiFi Processor Bundle is built using Java. 

## Submitting a pull request

To submit a pull request you should fork the nifi-influxdb-bundle repository, and make your change on a feature branch of your fork. Then generate a pull request from your branch against _master_ of the nifi-influxdb-bundle repository. Include in your pull request details of your change -- the why _and_ the how -- as well as the testing your performed. Also, be sure to run the test suite with your change in place. Changes that cause tests to fail cannot be merged.

There will usually be some back and forth as we finalize the change, but once that completes it may be merged.

To assist in review for the PR, please add the following to your pull request comment:

```md
* [ ] CHANGELOG.md updated
* [ ] Rebased/mergable
* [ ] Tests pass
* [ ] Sign [CLA](https://influxdata.com/community/cla/) (if not already signed)
```

## Signing the CLA

If you are going to be contributing back to the project please take a second to sign our CLA, which can be found
[on our website](https://influxdata.com/community/cla/).

## Getting the source

Setup the project structure and fetch the repo like so:

```bash
  mkdir $HOME/nifi-influxdb-bundle
  cd $HOME/nifi-influxdb-bundle
  git clone git@github.com:influxdata/nifi-influxdb-bundle .
```

## Cloning a fork

If you wish to work with fork of InfluxDB-NiFi bundle, your own fork for example, you must still follow the directory structure above. But instead of cloning the main repo, instead clone your fork. Follow the steps below to work with a fork:

```bash
  mkdir $HOME/nifi-influxdb-bundle
  cd $HOME/nifi-influxdb-bundle
  git clone git@github.com:<username>/nifi-influxdb-bundle .
```

## Build and Test

Make sure you have `docker` and `maven` installed and the project structure as shown above. We provide a `scripts/build.sh` to quickly build and test InfluxDB-NiFi bundle, so all you'll need to do is run the following:

```bash
  cd $HOME/nifi-influxdb-bundle
  ./scripts/build.sh
```

The NiFi NAR file will be located in `$HOME/nifi-influxdb-bundle/nifi-influx-database-nar/target/`.

To run the tests, execute the following command:

```bash
  cd $HOME/nifi-influxdb-bundle
  make clean test
```

For convenience we provide a small shell script which starts InfluxDB server locally:

```bash
  cd $HOME/nifi-influxdb-bundle
  ./scripts/influxdb-restart.sh
```

