const events = require('events')
const _ = require('lodash')

class Table extends events.EventEmitter {
  constructor (name) {
    super()

    this.name = name
    this._rows = new Map()
  }

  rows () {
    return Array.from(this._rows.values())
  }

  reset () {
    this._rows.clear()
  }

  emitSnapshot () {
    const rows = this.rows()
    console.log('SNAPSHOT', this.name, rows.length)

    this.emit('action', [{ a: 'dbe', o: { table: this.name, type: 'snap_begin' } }])
    this.emit('action', _.map(rows, row => {
      return { a: 'dbe', o: { table: this.name, type: 'row', rows: [{ before: row }] } }
    }))
    this.emit('action', [{ a: 'dbe', o: { table: this.name, type: 'snap_end' } }])
  }

  handleRowChange (evt, opts = {}) {
    if (evt.type !== 'row') return
    if (!evt.rows.length) return

    if (this.stored) {
      let row = evt.rows[0]

      switch (evt.op) {
        case 'UpdateRows':
          row = row.after
          break
        case 'WriteRows':
          row = row.before
          break
        case 'DeleteRows':
          row = row.after || row.before
          break
        default:
          row = null
          break
      }

      if (row) {
        switch (evt.op) {
          case 'WriteRows':
          case 'UpdateRows':
            this._rows.set(row.id, row)
            break
          case 'DeleteRows':
            this._rows.delete(row.id)
            break
        }
      }
    }

    if (!opts.silent) {
      this.emit('action', [{ a: 'dbe', o: evt }])
    }
  }
}

module.exports = Table
