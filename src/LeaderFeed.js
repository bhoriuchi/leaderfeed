import _ from 'lodash'
import Debug from 'debug'
import doneFactory from './common/doneFactory'
import EventEmitter from 'events'
import hat from 'hat'
import { FOLLOWER, LEADER, HEARTBEAT, NEW_STATE, NEW_LEADER } from './common/constants'

const debug = Debug('feed:rethinkdb')

export default class LeaderFeed extends EventEmitter {
  constructor (options, DEFAULT_HEARTBEAT_INTERVAL) {
    super()

    this.id = hat()
    this.state = null

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
        if (leader !== this.id) debug('heartbeat from %s', value)

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

      return this._start(options, (error) => {
        if (error) {
          debug('error during start %O', error)
          return done(error)
        }

        debug('starting feed')
        return this._subscribe(done)
      })
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
}