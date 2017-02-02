import LeaderFeed from '../LeaderFeed'

const DEFAULT_HEARTBEAT_INTERVAL = 1000

export default class MongoLeaderFeed extends LeaderFeed {
  constructor (driver, db, options) {
    super(options, DEFAULT_HEARTBEAT_INTERVAL)
  }
}