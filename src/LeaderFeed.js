import _ from 'lodash'
import Debug from 'debug'
import doneFactory from './common/doneFactory'
import EventEmitter from 'events'
import hat from 'hat'
import {
  FOLLOWER,
  LEADER,
  HEARTBEAT,
  NEW_STATE,
  NEW_LEADER,
  SUB_STARTED,
  SUB_ERROR
} from './common/constants'

const debug = Debug('feed:rethinkdb')

export default class LeaderFeed extends EventEmitter {
  /**
   * calculates the election timeout and stores common property values
   * @param options
   * @param DEFAULT_HEARTBEAT_INTERVAL
   */
  constructor (options, DEFAULT_HEARTBEAT_INTERVAL) {
    super()
    debug('initializing leader feed')

    this.id = hat()
    this.state = null

    this._options = options || {}
    this._electionTimeout = null
    this._heartbeatInterval = null

    // get common options
    let {
      createIfMissing,
      heartbeatIntervalMs,
      electionTimeoutMinMs,
      electionTimeoutMaxMs
    } = this._options

    delete this._options.createIfMissing
    delete this._options.heartbeatIntervalMs
    delete this._options.electionTimeoutMinMs
    delete this._options.electionTimeoutMaxMs

    let min = _.isNumber(electionTimeoutMinMs) ? Math.floor(electionTimeoutMinMs) : null
    let max = _.isNumber(electionTimeoutMaxMs) ? Math.floor(electionTimeoutMaxMs) : null

    this._createIfMissing = _.isBoolean(createIfMissing)
      ? createIfMissing
      : true

    // calculate timeout thresholds
    this._heartbeatIntervalMs = _.isNumber(heartbeatIntervalMs)
      ? Math.floor(heartbeatIntervalMs)
      : DEFAULT_HEARTBEAT_INTERVAL

    this._electionTimeoutMinMs = (min && min >= (this._heartbeatIntervalMs * 2))
      ? min
      : this._heartbeatIntervalMs * 2
    this._electionTimeoutMaxMs = (max && max >= (this._electionTimeoutMinMs * 2))
      ? max
      : this._electionTimeoutMinMs * 2
  }

  /**
   * starts the leaderfeed and subscription
   * @param options
   * @param callback
   * @returns {Promise}
   */
  start (options, callback = () => false) {
    if (!_.isObject(options) || _.isEmpty(options)) throw new Error('invalid options')
    if (!_.isFunction(callback)) throw new Error('invalid callback')

    return new Promise((resolve, reject) => {
      let done = doneFactory(
        error => error ? callback(error) : callback(null, this),
        () => resolve(this),
        reject
      )

      // start the heartbeat listener
      this.on(HEARTBEAT, (leader) => {
        if (leader !== this.id) debug('heartbeat from %s', leader)

        // check if a new leader has been elected
        if (this.leader && this.leader !== leader) this.emit(NEW_LEADER, leader)
        this.leader = leader

        // if leader, do not time out self, otherwise restart the timeout
        this.leader === this.id
          ? this._clearElectionTimeout()
          : this._restartElectionTimeout()

        // if the the node thinks it is the leader but the heartbeat
        // says otherwise, change to follower
        if (this.state === LEADER && leader !== this.id) return this._changeState(FOLLOWER)
      })
        .on(SUB_ERROR, error => {
          return this._changeState(FOLLOWER)
        })
        .on(SUB_STARTED, () => {
          return this._changeState(FOLLOWER)
        })

      // if create successful, attempt to start
      return this._start(options, (error) => {
        if (error) {
          debug('error during start %O', error)
          return done(error)
        }

        debug('_start successful')

        // attempt to create
        return this.create(error => {
          if (error) {
            debug('error during create %O', error)
            return done(error)
          }

          debug('create successful')

          // if start and create are successful, attempt to subscribe
          debug('starting feed')
          return this.subscribe(done)
        })
      })
    })
  }

  /**
   * creates a db/store/table/collection if missing and createIfMissing is true
   * @param done
   * @return {*}
   */
  create (done) {
    if (!this._createIfMissing) return done()

    return this._create(error => {
      if (error) return done(error)
      return done()
    })
  }

  /**
   * start subscription
   * @param done
   * @return {*}
   */
  subscribe (done) {
    return this._subscribe(error => {
      if (error) {
        debug('error during subscribe %O', error)
        this.emit()
        return done(error)
      }

      debug('subscribe successful')
      this.emit(SUB_STARTED)
      done(null, this)
    })
  }

  /**
   * change the state of the current leaderfeed
   * @param state
   * @returns {Promise}
   */
  _changeState (state) {
    if (state === this.state) return false
    debug('changed state %s', state)
    this.emit(NEW_STATE, state)

    switch (state) {
      case LEADER:
        this.state = LEADER
        this._clearElectionTimeout()

        // send the first heartbeat and start the heartbeat interval
        return this._heartbeat((error) => {
          if (error) {
            // if unable to set the heartbeat, cleat the interval and become follower
            debug('error sending heartbeat %O', error)
            this._clearHeartbeatInterval()
            return this._changeState(FOLLOWER)
          }
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
      return this._heartbeat((error) => {
        // if there was an error updating the heartbeat, cancel the interval
        if (error) this._clearHeartbeatInterval()
      })
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
      return this._changeState(LEADER)
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

  /************************************************
   * Methods that should be overridden
   ************************************************/

  /**
   * should update the leader/heartbeat metadata with a timestamp
   * and callback with error as first argument or no arguments if successful
   * @param done
   * @returns {*}
   * @private
   */
  _heartbeat (done) {
    return done()
  }

  /**
   * should create a change feed that emits the following events
   *
   * Event:heartbeat => leader
   * Event:change => { new_val, old_val }
   *
   * and then call done with error as the first argument or no arguments if successful
   *
   * @param done
   * @returns {*}
   * @private
   */
  _subscribe (done) {
    return done()
  }

  /**
   * sould take a hash of options and perform any initializations
   * or database connection steps then call done as an error as the first
   * argument or no arguments if successful
   * @param options
   * @param done
   * @returns {*}
   * @private
   */
  _start (options, done) {
    return done()
  }

  /**
   * should create the store/table/collection if it does not exist
   * and the createIfMissing option is set to true
   * and then call the done callback with error or no arguments
   * @param done
   * @return {*}
   * @private
   */
  _create (done) {
    return done()
  }
}