import _ from 'lodash'
import Debug from 'debug'
import doneFactory from '../common/doneFactory'

import {
  FOLLOWER,
  LEADER,
  VALUE,
  TIMESTAMP,
  NEW_LEADER,
  CHANGE
} from '../common/constants'

import LeaderFeed from '../LeaderFeed'

const debug = Debug('feed:rethinkdb')
const DEFAULT_DB = 'test'
const ID = 'id'
const DEFAULT_HEARTBEAT_INTERVAL = 1000

export default class RethinkLeaderFeed extends LeaderFeed {
  /**
   * initializes the leaderfeed
   * @param driver
   * @param db
   * @param options
   */
  constructor (driver, db, options) {
    debug('initializing leader feed')

    if (!driver) throw new Error('no driver specified')
    if (_.isObject(db)) {
      options = db
      db = DEFAULT_DB
    }

    super(options, DEFAULT_HEARTBEAT_INTERVAL)

    this.r = null
    this.db = db || DEFAULT_DB
    this.table = null
    this.collection = null
    this._driver = driver
  }

  /**
   * establishes a connection and starts the heartbeat
   * @param table
   * @param connection
   * @param callback
   * @returns {Promise}
   */
  start (table, connection, callback) {
    if (_.isFunction(connection)) {
      callback = connection
      connection = null
    }
    callback = _.isFunction(callback) ? callback : () => false

    debug('starting feed')

    // return a promise for flexibility
    return new Promise ((resolve, reject) => {

      // create a done handler that will handle callbacks and promises
      let done = doneFactory(callback, resolve, reject)

      try {
        if (!_.isString(table)) return done(new Error('missing table argument'))
        this.table = table

        // intelligently connect to the rethinkdb database
        if (!_.isFunction(this._driver.connect) || _.has(this._driver, '_poolMaster')) {
          this.connection = null
          this.r = !_.has(this._driver, '_poolMaster')
            ? this._driver(this._options)
            : this._driver
          return this._subscribe(done)

        } else {
          if (connection) {
            this.connection = connection
            this.r = this._driver
            return this._subscribe(done)
          } else {
            return this._driver.connect(this.options, (error, conn) => {
              if (error) return done(error)
              this.connection = conn
              this.r = this._driver
              return this._subscribe(done)
            })
          }
        }
      } catch (error) {
        callback(error)
        return reject(error)
      }
    })
  }

  /**
   * sends a heartbeat
   * @returns {Promise.<TResult>}
   * @private
   */
  _heartbeat () {
    debug('heartbeat update')
    let r = this.r
    let table = this.collection

    // insert a heartbeat
    return table.insert({
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
      .then(summary => {
        return summary
      }, error => {
        debug('heartbeat update error: %O', error)
        return this._changeState(FOLLOWER)
      })
  }

  /**
   * Sets up subscription
   * @private
   * @param self
   * @param done
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

        cursor.each((error, change) => {
          if (error) {
            debug('changefeed error: %O', error)
            return this._changeState(FOLLOWER)
          }

          let data = _.get(change, 'new_val')
          let id = _.get(data, ID)
          let value = _.get(data, VALUE)

          switch (id) {
            case LEADER:
              if (value !== this.id) debug('heartbeat from %s', value)

              // check if a new leader has been elected
              if (this.leader && this.leader !== value) this.emit(NEW_LEADER, value)
              this.leader = value

              // if leader, do not time out self, otherwise restart the timeout
              this.leader === this.id
                ? this._clearElectionTimeout()
                : this._restartElectionTimeout()

              return (this.state === LEADER && value !== this.id)
                ? this._changeState(FOLLOWER)
                : Promise.resolve()

            default:
              this.emit(CHANGE, change)
              break
          }
        })
        done(null, this)

        // after the cursor is obtained, change state to follower
        return this._changeState(FOLLOWER)
      }, done)
  }
}