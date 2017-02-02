import _ from 'lodash'
import Debug from 'debug'
import EventEmitter from 'events'
import hat from 'hat'
import { FOLLOWER, LEADER, NEW_STATE } from './common/constants'

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
        return this._heartbeat().then(() => {
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
      return this._heartbeat()
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

  /*
   * Methods that should be overridden
   */
  _heartbeat () {
    return Promise.resolve()
  }

  _subscribe () {
    return Promise.resolve()
  }
}