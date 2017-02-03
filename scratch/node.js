process.env.DEBUG = 'feed:rethinkdb'

require('babel-register')
var LeaderFeed = require('../src/rethinkdb/index').default
var r = require('rethinkdbdash')

let feed = new LeaderFeed(r, 'test')

feed.start({ table: 'leaderfeed' })
  .then(function (node) {
    console.log('started leader feed on', node.id)
  }, function (error) {
    console.error('got error', error)
  })