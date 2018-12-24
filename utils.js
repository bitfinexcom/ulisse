'use strict'

const Redis = require('ioredis')

const cliRedis = (conf, label = 'default') => {
  const redis = Redis.createClient(conf)

  redis.on('error', e => {
    console.error('RedisCli error:', label, e)
  })

  return redis
}

module.exports = {
  redis_cli: cliRedis
}
