import _ from 'lodash'
import Debug from 'debug'

import {
  FOLLOWER,
  LEADER,
  VALUE,
  TIMESTAMP,
  CHANGE,
  HEARTBEAT
} from '../common/constants'

import LeaderFeed from '../LeaderFeed'

const debug = Debug('feed:rethinkdb')
const ID = '_id'
const DEFAULT_HEARTBEAT_INTERVAL = 1000
const DEFAULT_COLLECTION_SIZE = 100000

export default class MongoLeaderFeed extends LeaderFeed {
  /**
   * initializes the leaderfeed
   * @param driver
   * @param db
   * @param options
   */
  constructor (driver, url, options) {
    debug('initializing leader feed')

    if (!driver) throw new Error('no driver specified')
    if (_.isString(url)) throw new Error('no url specified')

    super(options, DEFAULT_HEARTBEAT_INTERVAL)

    this.collection = null

    this._url = url
    this._driver = driver
    this._collectionName = null
  }

  /**
   * establishes a connection
   * @param options
   * @param done
   * @returns {*}
   * @private
   */
  _start (options, done) {
    try {
      let { collection } = options

      if (!_.isString(collection)) return done(new Error('missing collection argument'))
      this._collectionName = collection

      return this._driver.connect(this._url, this._options, (error, db) => {
        if (error) return done(error)
        this.db = db
        return done()
      })
    } catch (error) {
      return done(error)
    }
  }

  /**
   * create the db and table if they do not exist
   * @param done
   * @private
   */
  _create (done) {
    try {
      return this.db.listCollections({ name: this._collectionName })
        .toArray((error, collections) => {
          if (error) return done(error)
          if (collections.length) return done()

          return this.db.createCollection(this._collectionName, {
            capped: true,
            size: DEFAULT_COLLECTION_SIZE
          }, (error, collection) => {
            if (error) return done(error)
            this.collection = collection
            return done()
          })
        })
    } catch (error) {
      return done(error)
    }
  }

  /**
   * sends a heartbeat
   * @param done
   * @private
   */
  _heartbeat (done) {
    debug('heartbeat update')

    let db = this.db
    let collection = this.collection

    return this.collection.update({
      [ID]: LEADER
    }, {
      [VALUE]: this.id
    }, {
      upsert: true,
      $currentDate: {
        [TIMESTAMP]: { $type: 'timestamp' }
      }
    })


    let r = this.r
    let table = this.collection

    // insert a heartbeat
    table.insert({
      [ID]: LEADER,
      [VALUE]: this.id,
      [TIMESTAMP]: r.now()
    }, {
      durability: 'hard',
      conflict: 'update'
    })
      .do((summary) => {
        return summary('errors').ne(0).branch(
          r.error(summary('first_error')),
          true
        )
      })
      .run(this.connection)
      .then(() => {
        return done()
      }, error => {
        done(error)
        return this._changeState(FOLLOWER)
      })
  }

  /**
   * sets up a subscription
   * @param done
   * @returns {Promise.<TResult>}
   * @private
   */
  _subscribe (done) {
    let { r, connection, table, db } = this

    this.collection = r.db(db).table(table)

    return r.db(db)
      .table(table)
      .changes()
      .run(connection)
      .then((cursor) => {
        debug('changefeed started')

        // after the cursor is obtained, change state to follower
        this._changeState(FOLLOWER)

        cursor.each((error, change) => {
          if (error) {
            debug('changefeed error: %O', error)
            return this._changeState(FOLLOWER)
          }

          let data = _.get(change, 'new_val')
          let id = _.get(data, ID)
          let value = _.get(data, VALUE)

          // emit the appropriate event
          return id === LEADER
            ? this.emit(HEARTBEAT, value)
            : this.emit(CHANGE, change)
        })
        done(null, this)
      }, done)
  }
}