'use strict'

const fs = require('fs')
const _ = require('lodash')
const program = require('commander')
const async = require('async')
const Ulisse = require('./lib/Ulisse')
const lutils = require('./utils')

program
  .version('0.0.3')
  .option('-c, --conf <val>', 'configuration file')
  .option('--debug', 'debug')
  .parse(process.argv)

if (!program.conf) {
  program.conf = 'config.json'
}

const conf = _.extend(
  {},
  JSON.parse(fs.readFileSync(__dirname + '/' + program.conf, 'UTF8'))
)

if (!conf.id) {
  conf.id = 1
}

const QOUT = {}

var qout = (t) => {
  t = t || 'default'
  
  if (QOUT[t]) return QOUT[t]

  const out = async.queue((job, cb) => {
    if (job.data.length === 1) {
      const evt = job.data[0]
      rc_pub.publish(conf.dest, JSON.stringify(evt), cb)
    } else {
      const rpl = []

      _.each(job.data, evt => {
        rpl.push(['publish', conf.dest, JSON.stringify(evt)])
      })

      rc_pub.pipeline(rpl).exec(cb)
    }
  })

  QOUT[t] = out
  return out
}

const rc_sub = lutils.redis_cli(conf.redis)
const rc_pub = lutils.redis_cli(conf.redis)

const ulisse = new Ulisse({
  mysql: conf.mysql
})

ulisse.start()

ulisse.on('action', (t, evts) => {
  const out = qout(t)
  _.each(_.chunk(evts, 10), chunk => {
    out.push({ data: chunk })
  })
})
  
rc_sub.on('message', (channel, msg) => {
  try {
    msg = JSON.parse(msg)
  } catch(e) {
    console.error(e, msg)
    msg = null
  }

  if (!msg) return

  console.log('COMMAND', msg)
  handleCommand(msg)
})

var handleCommand = (msg) => {
  switch (msg.action) {
    case 'snap':
      ulisse.snap.apply(ulisse, msg.args)
    break
  }
}

rc_sub.subscribe('ulisse:' + conf.id)
