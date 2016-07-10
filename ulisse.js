'use strict'

const fs = require('fs')
const _ = require('lodash')
const program = require('commander')
const async = require('async')
const ZongJi = require('zongji')
const lutils = require('./utils')

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
}

var rc_sub = lutils.redis_cli(conf.redis)
var rc_pub = lutils.redis_cli(conf.redis)

var cli = null

cli = new ZongJi(conf.mysql)

cli.on('binlog', function(evt) {
  let query = null

  if (evt.query) {
    query = { text: evt.query.replace(/\\/gm, ''), type: 'statement' }
  } else {
    let tableId = evt.tableId + ''
    let tableName = evt.tableMap && evt.tableMap[tableId] ? evt.tableMap[tableId].tableName : 'unknown'
    query = { table: tableName, rows: evt.rows, type: 'row' }
    if (query.rows) {
      _.each(query.rows, (row, ix) => {
        if (!row.before && !row.after) { 
          query.rows[ix] = { before: row }
        }
      })
    } else {
      query = null
    }
  }

  if (!query) return

  console.log(JSON.stringify(query))
  rc_pub.publish(conf.dest, JSON.stringify(query))
})

function zerror(e) {
  console.error(e)
  cli.stop()
}

cli.on('error', zerror)
cli.ctrlConnection.on('error', zerror)

cli.start({
  startAtEnd: true,
  includeEvents: ['query', 'rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows']
})
