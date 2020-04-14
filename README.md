# HBridge
Simple ~~MQTT~~ TCP Message Bridge written in Haskell

**It is under development and is really simple now. Everything including structure can vary at any time.**


## Strucure

This bridge supports forwarding ~~MQTT~~ messages between brokers. It is designed to connect to more than two brokers and can forward messags on both directions.

```
    Broker 1  -----------    -----------  Broker 1'
                        |    |
    Broker 2  --------- Bridge ---------  Broker 2'
      ...               |    |              ...
    Broker N  -----------    -----------  Broker N'
```

## Some Details
The bridge requires a config file `config.json` to describe all the brokers it connects to. A sample one can be found at `./etc/`. It will also be generated when you run the test case.

When the bridge starts, it will try connecting to brokers described in config file. It maintains a broadcast channel which contains all the messages it received. For each broker, there is a forwarding topic list and a subscription list to control the bridging process. Messages that do not match the forwarding list will not be put into the channel, and only messages matching the subscription list will be forwarded to certain brokers.

## How to Run
A simple test case is provided. Note that you should run test to open "brokers":

```bash
    $ stack test
    $ stack exec -- HBridge-exe etc/config.json +RTS -T
```

Then you can see the "connecting", "sending" and "forwarding" results. And some real-time statistics can be viewed at `localhost:22333`, provided by `ekg` package.

You can also specify some logging configs such as log file path, log level and whether output to `stderr`.

Your own test cases can be written in `test/Spec.hs`.

## Problems
- **MQTT message bridging is not implemented**
- Bad code style
