const events = require('events')

class Table extends events.EventEmitter {
  constructor (name) {
    super()

    this.name = name
  }

  startSnapshot () {
    console.log('SNAPSHOT_START', this.name)
    this.emit('action', [{ a: 'dbe', o: { table: this.name, type: 'snap_begin' } }])
  }

  endSnapshot () {
    console.log('SNAPSHOT_END', this.name)
    this.emit('action', [{ a: 'dbe', o: { table: this.name, type: 'snap_end' } }])
  }

  handleRowChange (evt, opts = {}) {
    if (evt.type !== 'row') return
    if (!evt.rows.length) return

    if (!opts.silent) {
      this.emit('action', [{ a: 'dbe', o: evt }])
    }
  }
}

module.exports = Table
