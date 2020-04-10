# HBridge
Simple ~~MQTT~~ RPC Bridge written in Haskell

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
The bridge requires a config file `config.json` to describe all the brokers it connects to. A sample one can be found at `./etc/`.

When the bridge starts, it will try connecting to brokers described in config file. It maintains a broadcast channel which contains all the messages it received. For each broker, there is a forwarding topic list and a subscription list to control the bridging process. Messages that do not match the forwarding list will not be put into the channel, and only messages matching the subscription list will be forwarded to certain brokers.

## How to Run
A simple test case is provided. Run

```bash
    $ stack test
    $ stack repl
    ...
    *Main HBridge> main
```

Then you can see the "sending" and "forwarding" results.

## Problems

- Many exceptions are not handled
- Topic matching function is not implemented
- **MQTT message bridging is not implemented**
- Logging function is really silly
- (Bad code style, chaotic string using and redundant modules)
