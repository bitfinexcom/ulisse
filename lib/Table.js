const events = require('events')
const _ = require('lodash')
const async = require('async')

class Table {

  constructor() {
    this._rows = new Map()
  }

  rows() {
    return Array.from(this._rows.values())
  }

  reset() {
    this._rows.clear()
  }

  handleRowChange(evt) {
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
  }
}

module.exports = Table
