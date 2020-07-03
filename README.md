# ulisse

A tool for getting MySQL snapshots and publishing binlog events to Redis ordered queue.

## Install
```
npm install -g ulisse
```

## Usage

```
$ ulisse --help
Usage: ulisse.js -c <val>

Options:
  --conf, -c  configuration file path
  --debug     debug
```

## Configuration
```json
{
  "id": 1,
  "redis": {
    "host": "",
    "port": 6379
  },
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "",
    "password": ""
  },
  "dest": "REDISPUBLISHCHANNEL",
  "snapRelaxedFlushTimeout": 5000,
  "exitOnError": true
}
```

