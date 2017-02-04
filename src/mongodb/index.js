import _ from 'lodash'
import Debug from 'debug'
import pad from '../common/pad'

import {
  LEADER,
  TYPE,
  VALUE,
  TIMESTAMP,
  CHANGE,
  HEARTBEAT,
  SUB_ERROR
} from '../common/constants'

import LeaderFeed from '../LeaderFeed'

const debug = Debug('feed:rethinkdb')
const ID = '_id'
const DEFAULT_HEARTBEAT_INTERVAL = 1000
const DEFAULT_COLLECTION_SIZE = 100000
const DEFAULT_MAX_DOCS = 20

export default class MongoLeaderFeed extends LeaderFeed {
  /**
   * initializes the leaderfeed
   * @param driver
   * @param db
   * @param options
   */
  constructor (driver, url, options) {
    debug('initializing leader feed')
    if (_.isObject(url)) {
      options = url
      url = null
    }

    super(options, DEFAULT_HEARTBEAT_INTERVAL)

    // check if the driver is a db or driver
    this.db = _.isObject(driver) && !_.isFunction(driver.connect)
      ? driver
      : null

    if (!driver && !this.db) throw new Error('no driver specified')
    if (!_.isString(url) && !this.db) throw new Error('no url specified')

    this.collection = null

    this._url = url
    this._driver = this.db
      ? null
      : driver
    this._collectionName = null

    // mongo capped collection create options
    this._createOpts = {
      capped: true,
      size: this._options.collectionSizeBytes || DEFAULT_COLLECTION_SIZE,
      max: this._options.collectionMaxDocs || DEFAULT_MAX_DOCS
    }

    // remove the size options
    delete this._options.collectionSizeBytes
    delete this._options.collectionMaxDocs
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

      // if the db is already connected we are done
      if (this.db) return done()

      // otherwise connect it
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

          // if the collection exists, get it and return done
          if (collections.length) {
            return this.db.collection(this._collectionName, (error, collection) => {
              if (error) return done(error)
              this.collection = collection
              return done()
            })
          }

          // if the collection doesnt exist, create it and add 1 record
          return this.db.createCollection(this._collectionName, this._createOpts, (error, collection) => {
            if (error) return done(error)
            this.collection = collection

            return collection.insertOne({
              [TYPE]: pad(LEADER),
              [VALUE]: pad(id),
              [TIMESTAMP]: Date.now()
            }, (error) => {
              if (error) return done(error)
              return done()
            })
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

    this.collection.insertOne({
      [TYPE]: pad(LEADER),
      [VALUE]: pad(id),
      [TIMESTAMP]: Date.now()
    }, error => {
      return error
        ? done(error)
        : done()
    })
  }

  /**
   * sets up a subscription
   * @param done
   * @returns {Promise.<TResult>}
   * @private
   */
  _subscribe (done) {
    let stream = this.collection.find({}, {
      tailable: true,
      awaitdata: true
    })
      .stream()

    stream.on('data', (data) => {
      let type = _.get(data, TYPE, '').trim()
      let value = _.get(data, VALUE)

      return type === LEADER
        ? this.emit(HEARTBEAT, value)
        : this.emit(CHANGE, data)
    })
    stream.on('error', (error) => {
      debug('stream error: %O', error)
      return this.emit(SUB_ERROR, error)
    })
  }
}