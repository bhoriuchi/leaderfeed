import _ from 'lodash'
import Debug from 'debug'
import EventEmitter from 'events'
import hat from 'hat'
import {
  FOLLOWER,
  LEADER,
  VALUE,
  TIMESTAMP,
  NEW_STATE,
  NEW_LEADER,
  CHANGE
} from '../common/constants'

const debug = Debug('feed:rethinkdb')
const DEFAULT_DB = 'test'
const ID = 'id'
const DEFAULT_APPEND_INTERVAL = 1000

/**
 * create a new done handler
 * @private
 * @param callback
 * @param resolve
 * @param reject
 * @returns {done}
 */
function doneFactory (callback, resolve, reject) {
  callback = _.isFunction(callback) ? callback : () => false
  resolve = _.isFunction(resolve) ? resolve : () => false
  reject = _.isFunction(reject) ? reject : () => false

  return function done (error, success) {
    if (error) {
      callback(error)
      return reject(success)
    }
    callback(null, success)
    return resolve(success)
  }
}

/**
 * Sets up subscription
 * @private
 * @param self
 * @param done
 */
function subscribe (main, done) {
  let { r, connection, table, db } = main

  main.collection = r.db(db).table(table)

  return r.db(db)
    .table(table)
    .changes()
    .run(connection)
    .then((cursor) => {
      debug('changefeed started')

      cursor.each((error, change) => {
        if (error) {
          debug('changefeed error: %O', error)
          return main.changeState(FOLLOWER)
        }

        let data = _.get(change, 'new_val')
        let id = _.get(data, ID)
        let value = _.get(data, VALUE)

        switch (id) {
          case LEADER:
            if (value !== main.id) debug('heartbeat from %s', value)

            // check if a new leader has been elected
            if (main.leader && main.leader !== value) main.emit(NEW_LEADER, value)
            main.leader = value

            // if leader, do not time out self, otherwise restart the timeout
            main.leader === main.id
              ? main._clearElectionTimeout()
              : main._restartElectionTimeout()

            return (main.state === LEADER && value !== main.id)
              ? main.changeState(FOLLOWER)
              : Promise.resolve()

          default:
            main.emit(CHANGE, change)
            break
        }
      })
      done(null, main)

      // after the cursor is obtained, change state to follower
      return main.changeState(FOLLOWER)
    }, done)
}

/**
 * add heartbeats to the table
 * @private
 * @param main
 * @returns {Promise.<TResult>}
 */
function heartbeat (main) {
  debug('heartbeat update')
  let r = main.r
  let table = r.db(main.db).table(main.table)

  // insert a heartbeat
  return table.insert({
    [ID]: LEADER,
    [VALUE]: main.id,
    [TIMESTAMP]: main.r.now()
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
    .run(main.connection)
    .then(summary => {
      return summary
    }, error => {
      debug('heartbeat update error: %O', error)
      return main.changeState(FOLLOWER)
    })
}

export default class RethinkLeaderFeed extends EventEmitter {
  /**
   * initializes the leaderfeed
   * @param driver
   * @param db
   * @param options
   */
  constructor (driver, db, options) {
    super()
    debug('initializing leader feed')

    if (!driver) throw new Error('no driver specified')
    if (_.isObject(db)) {
      options = db
      db = DEFAULT_DB
    }
    this.id = hat()
    this.db = db || DEFAULT_DB
    this.state = null
    this._driver = driver
    this._options = options || {}
    this._electionTimeout = null
    this._heartbeatInterval = null

    let { heartbeatIntervalMs, electionTimeoutMinMs, electionTimeoutMaxMs } = this._options
    delete this._options.heartbeatIntervalMs
    delete this._options.electionTimeoutMinMs
    delete this._options.electionTimeoutMaxMs

    let min = _.isNumber(electionTimeoutMinMs) ? Math.floor(electionTimeoutMinMs) : null
    let max = _.isNumber(electionTimeoutMaxMs) ? Math.floor(electionTimeoutMaxMs) : null

    // calculate timeout thresholds
    this._heartbeatIntervalMs = _.isNumber(heartbeatIntervalMs)
      ? Math.floor(heartbeatIntervalMs)
      : DEFAULT_APPEND_INTERVAL

    this._electionTimeoutMinMs = (min && min >= (this._heartbeatIntervalMs * 2))
      ? min
      : this._heartbeatIntervalMs * 2
    this._electionTimeoutMaxMs = (max && max >= (this._electionTimeoutMinMs * 2))
      ? max
      : this._electionTimeoutMinMs * 2
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
          return subscribe(this, done)

        } else {
          if (connection) {
            this.connection = connection
            this.r = this._driver
            return subscribe(this, done)
          } else {
            return this._driver.connect(this.options, (error, conn) => {
              if (error) return done(error)
              this.connection = conn
              this.r = this._driver
              return subscribe(this, done)
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
   * change the state of the current leaderfeed
   * @param state
   * @returns {Promise}
   */
  changeState (state) {
    if (state === this.state) return false
    debug('changed state %s', state)
    this.emit(NEW_STATE, state)

    switch (state) {
      case LEADER:
        this.state = LEADER
        this._clearElectionTimeout()

        // send the first heartbeat and start the heartbeat interval
        return heartbeat(this).then(() => {
          this._restartHeartbeatInterval()
        })

      case FOLLOWER:
        this.state = FOLLOWER
        this._restartElectionTimeout()
        return Promise.resolve()
    }
  }

  /**
   * clear the heartbeat interval
   * @private
   */
  _clearHeartbeatInterval () {
    if (this._heartbeatInterval) clearInterval(this._heartbeatInterval)
    this._heartbeatInterval = null
  }

  /**
   * clear heartbeat interval and restart
   * @private
   */
  _restartHeartbeatInterval () {
    this._clearHeartbeatInterval()
    this._heartbeatInterval = setInterval(() => {
      return heartbeat(this)
    }, this._heartbeatIntervalMs)
  }

  /**
   * clear election timeout
   * @private
   */
  _clearElectionTimeout () {
    if (this._electionTimeout) clearTimeout(this._electionTimeout)
    this._electionTimeout = null
  }

  /**
   * clear election timeout and restart
   * @private
   */
  _restartElectionTimeout () {
    this._clearElectionTimeout()
    this._electionTimeout = setTimeout(() => {
      return this.changeState(LEADER)
    }, this.randomElectionTimeout)
  }

  /**
   * retrns truthy if leaderfeed is the leader
   * @returns {boolean}
   */
  get isLeader () {
    return this.state === LEADER || this.leader === this.id
  }

  /**
   * generates a random number within the election timeout threshold
   * @returns {number}
   */
  get randomElectionTimeout () {
    return Math.floor(
      Math.random() * (this._electionTimeoutMaxMs - this._electionTimeoutMinMs + 1) + this._electionTimeoutMinMs
    )
  }
}