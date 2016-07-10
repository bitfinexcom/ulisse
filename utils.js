'use strict'

const Redis = require('ioredis')

Redis.Promise.onPossiblyUnhandledRejection(e => {
  STATUS.processing = 0
  console.log(e)
})

var redis_cli = (conf, label = 'default') => {
  var redis = Redis.createClient(conf, {
    dropBufferSupport: true
  })

  redis.on('error', e => {
    console.error('RedisCli error:', label, e)
  })

  return redis
}

module.exports = {
  redis_cli: redis_cli
}
