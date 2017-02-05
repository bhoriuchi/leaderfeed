import _ from 'lodash'
import Debug from 'debug'

import {
  LEADER,
  VALUE,
  TIMESTAMP,
  CHANGE,
  HEARTBEAT,
  SUB_ERROR
} from '../common/constants'

import LeaderFeed from '../LeaderFeed'

const DEFAULT_HEARTBEAT_INTERVAL = 1000

export default class RedisLeaderFeed extends LeaderFeed {
  constructor (driver, options) {
    if (!driver) throw new Error('no driver specified')
    if (!_.isObject(options) || _.isEmpty(options)) throw new Error('no options specefied')
    super(options, DEFAULT_HEARTBEAT_INTERVAL)

    this._driver = driver
    this.pub = null
    this.sub = null
    this.channel = null
    this._pubConnected = false
    this._subConnected = false
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
      let { channel } = options
      if (!_.isString(channel)) return done(new Error('missing channel'))
      this.channel = channel

      // connect the pub and sub clients
      this.pub = this._driver.createClient(this._options)
      this.sub = this._driver.createClient(this._options)

      this.pub.once('ready', () => {
        this._pubConnected = true
        if (this._pubConnected && this._subConnected) return done()
      })
        .once('error', (error) => {
          if (!this._pubConnected && !this._subConnected) return done(error)
        })

      this.sub.once('ready', () => {
        this._pubConnected = true
        if (this._pubConnected && this._subConnected) return done()
      })
        .once('error', (error) => {
          if (!this._pubConnected && !this._subConnected) return done(error)
        })

    } catch (error) {
      return done(error)
    }
  }

  /**
   * passthrough
   * @param done
   * @private
   */
  _create(done) {
    // since redis uses pre-created db immediately call done
    return done()
  }

  /**
   * elects a new leader
   * @param id
   * @param done
   * @returns {*}
   * @private
   */
  _elect (id, done) {
    try {
      this.pub.publish(this.channel, id)
    } catch (error) {
      return done(error)
    }
  }

  /**
   * sends a heartbeat
   * @param done
   * @returns {*}
   * @private
   */
  _heartbeat (done) {
    try {
      this.pub.publish(this.channel, this.id)
    } catch (error) {
      return done(error)
    }
  }

  /**
   * subscribes to a channel
   * @param done
   * @returns {*}
   * @private
   */
  _subscribe (done) {
    try {
      sub.on('subscribe', (channel, count) => {
        if (channel === this.channel) return done()
      })
        .on('message', (channel, leader) => {
          if (channel === this.channel) this.emit(HEARTBEAT, leader)
        })
      this.sub.subscribe(this.channel)
    } catch (error) {
      return done(error)
    }
  }

  /**
   * unsubscribes from a channel
   * @param done
   * @returns {*}
   * @private
   */
  _unsubscribe (done) {
    try {
      this.sub.unsubscribe()
      this.sub.quit()
      this.pub.quit()
      return done()
    } catch (error) {
      return done(error)
    }
  }
}