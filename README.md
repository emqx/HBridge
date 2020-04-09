# HBridge
Simple MQTT Bridge written in Haskell

**It is under development and is really simple now. Everything including structure can vary at any time.**


## Strucure

This bridge supports forwarding MQTT messages between brokers. It is **concurrently** designed to connect to more than two brokers and can forward messags on both directions.

```
    Broker 1  -----------    -----------  Broker 1'
                        |    |
    Broker 2  --------- Bridge ---------  Broker 2'
      ...               |    |              ...
    Broker N  -----------    -----------  Broker N'

```

## Problems

- Require a better structure
- Messages may loop between brokers
- Topic function is not implemented
