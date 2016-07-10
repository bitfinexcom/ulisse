'use strict'

const fs = require('fs')
const _ = require('lodash')
const program = require('commander')
const async = require('async')
const Redis = require('ioredis')
const ZongJi = require('zongji')

program
  .version('0.0.1')
  .option('-c, --conf <val>', 'configuration file')
  .option('--debug', 'debug')
  .parse(process.argv)

if (!program.conf) {
  program.conf = 'config.json'
}

var conf = _.extend(
  {},
  JSON.parse(fs.readFileSync(__dirname + '/' + program.conf, 'UTF8'))
)

var STATUS = {
  processing : 0
}

Redis.Promise.onPossiblyUnhandledRejection(e => {
  STATUS.processing = 0
  console.log(e)
})

var redis = Redis.createClient(conf.redis, {
  dropBufferSupport: true
})

redis.on('error', e => {
  STATUS.processing = 0
  console.log(e)
})

var cli = null

function start() {
  cli = new ZongJi(conf.mysql)

  cli.on('binlog', function(evt) {
    let query = null

    if (evt.query) {
      query = { text: evt.query.replace(/\\/gm, ''), type: 'string' }
    } else {
      let tableId = evt.tableId + ''
      let tableName = evt.tableMap && evt.tableMap[tableId] ? evt.tableMap[tableId].tableName : 'unknown'
      query = { table: tableName, rows: evt.rows, type: 'object' }
      if (!evt.rows) {
        query = null
      }
    }

    if (!query) return

    redis.publish(conf.dest, JSON.stringify(query))
  })

  function zerror(e) {
    console.error(e)
    cli.stop()
    setTimeout(function() {
      cli = null
    }, 2500)
  }

  cli.on('error', zerror)
  cli.ctrlConnection.on('error', zerror)

  cli.start({
    startAtEnd: true,
    includeEvents: ['query', 'rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows']
  })
}

setInterval(function() {
  if (cli) return
  start()
}, 5000)

start()
