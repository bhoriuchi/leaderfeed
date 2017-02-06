import ChangeFeed from '../ChangeFeed'

export default class RedisChangeFeed extends ChangeFeed {
  constructor(leaderfeed, collection) {
    super(leaderfeed, collection)
  }

  changes (done) {
    return done()
  }
}