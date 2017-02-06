process.env.DEBUG = 'feed:*'

require('babel-register')
var LeaderFeed = require('../src/index').default.MongoDB
var mongodb = require('mongodb')

let feed = new LeaderFeed(mongodb, 'mongodb://localhost:27017/test')

feed.start({ collection: 'leaderfeed' })
  .then(function (node) {
    console.log('started leader feed on', node.id)
  }, function (error) {
    console.error('got error', error)
  })