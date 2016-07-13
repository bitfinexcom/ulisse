const events = require('events')
const _ = require('lodash')
const async = require('async')
const Table = require('./Table')
const mysql = require('mysql')
const ZongJi = require('zongji')

class Ulisse extends events.EventEmitter {

  constructor(conf) {
    super()

    this.conf = conf

    this.conf.mysql.supportBigNumbers = true
    this.conf.mysql.bigNumberStrings = true

    this.binlog = null
    this.binlog = new ZongJi(conf.mysql)
    this.cli = mysql.createConnection(conf.mysql)

    this.tables = {}
  }

  parse(evt) {
    let query = null

    if (evt.query) {
      query = { text: evt.query.replace(/\\/gm, ''), type: 'statement' }
    } else {
      const tableId = evt.tableId + ''
      const tableName = evt.tableMap && evt.tableMap[tableId] ? evt.tableMap[tableId].tableName : 'unknown'
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

    return query
  }

  table(t) {
    let table = null

    if (this.tables[t]) {
      table = this.tables[t]
    } else {
      table = this.tables[t] = new Table(t)
      table.on('action', evts => {
        this.emit('action', evts)
      })
    }

    return table
  } 

  reconnectDb() {
    this.cli = mysql.createConnection(this.conf.mysql)

    this.cli.connect(err => {
      if (!err) return

      setTimeout(() => {
        this.reconnectDb()
      }, 2000)
    })

    this.cli.on('error', err => {
      console.error('DB_CLI ERROR', err)
      if(err.code === 'PROTOCOL_CONNECTION_LOST') {
        this.reconnectDb()
      } else {
        throw err
      }
    })
  }

  start() {
    this.reconnectDb()

    this.binlog.on('binlog', (evt) => {
      var query = this.parse(evt)
      if (!query) return

      if (query.type === 'row') {
	const table = this.table(query.table)
        table.handleRowChange(query)
      } else {
        this.emit('action', [{ a: 'dbe', o: query }])
      }
    })

    function zerror(e) {
      console.error(e)
      this.binlog.stop()
    }

    this.binlog.on('error', zerror)
    this.binlog.ctrlConnection.on('error', zerror)

    this.binlog.start({
      startAtEnd: true,
      includeEvents: ['query', 'rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows']
    })
  }

  snap(db, tables, opts = {}) {
    var conn = this.cli
    let resync = []

    _.each(tables, t => {
      const table = this.table(t)
      const isDbResync = opts.force || !table.synced
      table.isDbResync = isDbResync
      if (isDbResync) resync.push(t)
    })

    console.log('SNAPSHOT START', db, tables)
 
    async.auto({
      db: next => {
        conn.query('USE ' + db, next)
      },
      lock: ['db', (res, next) => {
        if (!resync.length) return next()

        conn.query('FLUSH TABLES ' + resync + ' WITH READ LOCK', next)
      }],
      sleep: ['lock', (res, next) => {
        if (!resync.length) return next()
        setTimeout(next, 800)
      }],
      handle: ['sleep', (res, next) => {
        const aseries = []

        _.each(tables, t => {
          aseries.push(next => {
            const table = this.table(t) 
            if (table.isDbResync) {
              console.log('QUERY', t)
              conn.query('SELECT * FROM ' + t, (err, rows) => {
                table.reset()
                if (_.isArray(rows)) {
                  _.each(rows, row => {
                    table.handleRowChange({ type: 'row', table: t, rows: [{ before: row }] })
                  })
                }
                table.synced = 1
                table.emitSnapshot()
                next(err)
              })
            } else {
              table.emitSnapshot()
            }
          })
        })

        async.series(aseries, next)
      }],
      unlock: ['handle', (res, next) => {
        if (!resync.length) return next()
        conn.query('UNLOCK TABLES', next)
      }]
    }, (err, res) => {
      console.log('SNAPSHOT END', tables, err)
      _.each(tables, t => {
        const table = this.table(t)
        table.isDbResync = false
      })
    })
  }
}

module.exports = Ulisse
