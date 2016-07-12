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

  start() {
    this.cli = mysql.createConnection(this.conf.mysql)

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

  snap(db, tables, opts = {}) {
    var conn = this.cli
    let missing = []

    _.each(tables, t => {
      if (opts.force || !this.tables[t]) {
        missing.push(t)
      }
    })

    console.log('SNAPSHOT START', db, tables)
 
    async.auto({
      db: next => {
        conn.query('USE ' + db, next)
      },
      lock: ['db', (res, next) => {
        if (!missing.length) return next()

        conn.query('FLUSH TABLES ' + missing + ' WITH READ LOCK', next)
      }],
      sleep: ['lock', (res, next) => {
        if (!missing.length) return next()
        setTimeout(next, 800)
      }],
      data: ['sleep', (res, next) => {
        const data = {}
        const aseries = []

        _.each(_.difference(tables, missing), t => {
          data[t] = this.tables[t].rows()
        })

        _.each(missing, t => {
          const table = this.tables[t] = this.tables[t] || new Table()
          table.reset()

          aseries.push(next => {
            console.log('QUERY', t)
            conn.query('SELECT * FROM ' + t, (err, rows) => {
              if (_.isArray(rows)) {
                data[t] = rows
                _.each(rows, row => {
                  table.handleRowChange({ type: 'row', table: t, rows: [{ before: row }] })
                })
              }
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
        
        _.each(tables, t => {
          const data = res.data[t]
          const table = this.tables[t]

          if (!data) return
          console.log('SNAPSHOT', t, data.length)

          const evts = _.map(data, row => {
            return { a: 'dbe', o: { table: t, rows: [{ before: row }], type: 'row' } }
          })

          this.emit('action', [{ a: 'dbe', o: { table: t, type: 'snap_begin' } }])          
          this.emit('action', evts)
          this.emit('action', [{ a: 'dbe', o: { table: t, type: 'snap_end' } }])
        })
 
        next()
      }],
      unlock: ['emit', (res, next) => {
        if (!missing.length) return next()
        conn.query('UNLOCK TABLES', next)
      }]
    }, (err, res) => {
      console.log('SNAPSHOT END', tables, err)
    })
  }
}

module.exports = Ulisse
