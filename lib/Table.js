const events = require('events')
const _ = require('lodash')
const async = require('async')

class Table extends events.EventEmitter {

  constructor(name) {
    super()

    this.name = name
    this._rows = new Map()
  }

  rows() {
    return Array.from(this._rows.values())
  }

  reset() {
    this._rows.clear()
  }

  emitSnapshot() {
    const rows = this.rows()
    console.log('SNAPSHOT', this.name, rows.length)

    this.emit('action', [{ a: 'dbe', o: { table: this.name, type: 'snap_begin' } }])          
    this.emit('action', _.map(rows, row => {
      return { a: 'dbe', o: { table: this.name, type: 'row', rows: [{ before: row }] } }
    }))
    this.emit('action', [{ a: 'dbe', o: { table: this.name, type: 'snap_end' } }])
  }

  handleRowChange(evt, opts = {}) {
    if (evt.type !== 'row') return
    if (!evt.rows.length) return

    let row = evt.rows[0]
    let op = null
    if (row.before && row.after) {
      op = 'update'
      row = row.after
    } else if (row.before) {
      op = 'insert'
      row = row.before
    } else if (row.after) {
      op = 'delete'
      row = row.after
    } else {
      row = null
    }

    if (op && row) {
      switch (op) {
        case 'insert':
        case 'update':
          this._rows.set(row.id, row)
          break
        case 'delete':
          this._rows.delete(row.id)
          break
      }
    }

    if (!opts.silent) {
      this.emit('action', [evt])
    }
  }
}

module.exports = Table
