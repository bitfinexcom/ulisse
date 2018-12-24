'use strict'

const path = require('path')
const _ = require('lodash')
const Ulisse = require('./lib/Ulisse')
const lutils = require('./utils')

const program = require('yargs')
  .option('conf', {
    alias: 'c',
    default: 'config.json',
    describe: 'configuration file path',
    demand: true,
    type: 'string'
  })
  .option('debug', {
    describe: 'debug',
    default: false,
    type: 'boolean'
  })
  .help('help')
  .version()
  .usage('Usage: $0 -c <val>')
  .argv

const conf = _.extend(
  {},
  require(path.join(__dirname, '/', program.conf))
)

if (!conf.id) {
  conf.id = 1
}

const QOUT = {}

function qout (t, data) {
  t = t || 'default'
  if (!QOUT[t]) QOUT[t] = []

  for (let i = 0; i < data.length; i++) {
    QOUT[t].push(data[i])
  }
}

function flush () {
  const rpl = []

  _.each(QOUT, (evts, k) => {
    if (!evts.length) {
      return
    }

    const data = evts.splice(0, 50)

    for (let i = 0; i < data.length; i++) {
      rpl.push(['publish', conf.dest, JSON.stringify(data[i])])
    }
  })

  if (!rpl.length) {
    setTimeout(flush, 50)
    return
  }

  pubRc.pipeline(rpl).exec(() => {
    const relaxed = (Date.now() - ulisse._snapTs) < (conf.snapRelaxedFlushTimeout || 30000)
    setTimeout(flush, relaxed ? 25 : 2)
  })
}

flush()

const subRc = lutils.redis_cli(conf.redis)
const pubRc = lutils.redis_cli(conf.redis)

const ulisse = new Ulisse(_.pick(conf, ['mysql', 'forwardStatements', 'schemas']))

ulisse.start()

ulisse.on('action', (t, evts) => {
  qout(t, evts)
})

ulisse.on('error-critical', (err) => {
  console.error('CRITICAL', err)
  if (conf.exitOnError) {
    process.exit(-1)
  }
})

subRc.on('message', (channel, msg) => {
  try {
    msg = JSON.parse(msg)
  } catch (e) {
    console.error(e, msg)
    msg = null
  }

  if (!msg) return

  console.log('COMMAND', msg)
  handleCommand(msg)
})

const handleCommand = (msg) => {
  switch (msg.action) {
    case 'snap':
      ulisse.snap.apply(ulisse, msg.args)
      break
  }
}

subRc.subscribe('ulisse:' + conf.id)
