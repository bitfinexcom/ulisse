const events = require('events')
const _ = require('lodash')
const async = require('async')
const mysql = require('mysql')
const ZongJi = require('zongji')

class Binlog extends events.EventEmitter {

  constructor(conf) {
    super()

    this.conf = conf
    this.conf.mysql.supportBigNumbers = true
    this.conf.mysql.bigNumberStrings = true

    this.binlog = null
    this.binlog = new ZongJi(conf.mysql)
    this.cli = mysql.createConnection(conf.mysql)
  }

  parse(evt) {
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

    return query
  }

  start() {
    this.binlog.on('binlog', (evt) => {
      var query = this.parse(evt)
  
      if (!query) return
   
      this.emit('action', [{ a: 'dbe', o: query }])
    })

    function zerror(e) {
      console.error(e)
      this.binlog.stop()
    }

    this.binlog.on('error', zerror)
    this.binlog.ctrlConnection.on('error', zerror)

    this.binlog.start({
      startAtEnd: true,
      includeEvents: ['query', 'rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows', 'unknown']
    })
  }

  snap(db, tables) {
    var conn = this.cli

    console.log('SNAPSHOT START', db, tables)
    async.auto({
      db: next => {
        conn.query('USE ' + db, next)
      },
      lock: ['db', (res, next) => {
        conn.query('FLUSH TABLES ' + tables + ' WITH READ LOCK', next)
      }],
      sleep: ['lock', (res, next) => {
        setTimeout(next, 800)
      }],
      data: ['sleep', (res, next) => {
        var data = {}
        var aseries = []

        _.each(tables, table => {
          aseries.push(next => {
            conn.query('SELECT * FROM ' + table, (err, rows) => {
              if (_.isArray(rows)) data[table] = rows
              next(err)
            })
          })
        })

        async.series(aseries, (err) => {
          if (err) console.error(err)
          next(null, data)
        })
      }],
      emit: ['data', (res, next) => {
        if (!res.data) return next()
        
        _.each(tables, table => {
          var data = res.data[table]
          if (!data) return

          this.emit('action', [{ a: 'dbe', o: { table: table, type: 'snap_begin' } }])
          
          console.log('SNAPSHOT', table, data.length)
          var chunks = _.chunk(data, 1000)
          _.each(chunks, rows => {
            this.emit('action', _.map(rows, row => {
              return { a: 'dbe', o: { table: table, rows: [{ before: row }], type: 'row' } }
            }))
          })

          this.emit('action', [{ a: 'dbe', o: { table: table, type: 'snap_end' } }])
        })
 
        next()
      }],
      unlock: ['emit', (res, next) => {
        conn.query('UNLOCK TABLES', next)
      }]
    }, (err, res) => {
      console.log('SNAPSHOT END', tables, err)
    })
  }
}

module.exports = Binlog
