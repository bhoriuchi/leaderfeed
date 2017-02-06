import _ from 'lodash'
import Debug from 'debug'
import doneFactory from '../common/doneFactory'
import ChangeFeed from '../ChangeFeed'
import pad from '../common/pad'

import {
  TYPE,
  VALUE,
  TIMESTAMP
} from '../common/constants'

const debug = Debug('feed:mongodb:changes')
const logCollection = 'changefeed'
const DEFAULT_COLLECTION_SIZE = 100000
const DEFAULT_MAX_DOCS = 20
const INIT = 'init'
const OLD_VAL = 'old_val'
const DOC_MAX_CHARS = 5000
const COL_NAME_MAX_CHARS = 50

export default class MongoChangeFeed extends ChangeFeed {
  constructor (leaderfeed, collection) {
    super(leaderfeed, collection)
    this._driver = leaderfeed._driver
    this._url = leaderfeed._url
    this._options = leaderfeed._options
    this.db = leaderfeed.db
    this.log = null

    // mongo capped collection create options
    this._createOpts = {
      capped: true,
      size: this._options.collectionSizeBytes || DEFAULT_COLLECTION_SIZE,
      max: this._options.collectionMaxDocs || DEFAULT_MAX_DOCS
    }

    this._docMaxChars = this._options.docMaxChars || DOC_MAX_CHARS

    // remove the size options
    delete this._options.collectionSizeBytes
    delete this._options.collectionMaxDocs
    delete this._options.docMaxChars
  }

  changes (done) {
    return this._connect(error => {
      if (error) return done(error)

      return this._createLog((error, log) => {
        if (error) return done(error)
        this.log = log

        return this.db.collection(this._collection, (error, collection) => {
          if (error) return done(error)
          this.collection = collection

          return this._subscribe(error => {
            if (error) {
              debug('subscribe error %O', error)
              return done(error)
            }
            return done(null, this)
          })
        })
      })
    })
  }

  /**
   * connect to the database
   * @param done
   * @returns {*}
   * @private
   */
  _connect (done) {
    try {
      // if the db is already connected we are done
      if (this.db) return done()

      // otherwise connect it
      return this._driver.connect(this._url, this._options, (error, db) => {
        if (error) return done(error)
        this.db = db
        return done()
      })
    } catch (error) {
      debug('error in _start %O', error)
      return done(error)
    }
  }

  _appendLog (type, id, old_val, callback) {
    return new Promise((resolve, reject) => {
      let done = doneFactory(callback, resolve, reject)
      try {
        return this.log.insertOne({
          [TYPE]: pad(type),
          collection: pad(this._collection, COL_NAME_MAX_CHARS),
          [VALUE]: pad(id),
          [TIMESTAMP]: Date.now(),
          [OLD_VAL]: pad(JSON.stringify(old_val), this._docMaxChars)
        }, error => {
          if (error) debug('appendLog error %O', error)
          return error
            ? done(error)
            : done()
        })
      } catch (error) {
        debug('append error %O', error)
        return done(error)
      }
    })
  }

  _createLog (done) {
    try {
      return this.db.listCollections({ name: logCollection })
        .toArray((error, collections) => {
          if (error) return done(error)

          // if the collection exists, get it and return done
          if (collections.length) {
            return this.db.collection(logCollection, (error, collection) => {
              if (error) return done(error)
              return done(null, collection)
            })
          }

          // if the collection doesnt exist, create it and add 1 record
          return this.db.createCollection(logCollection, this._createOpts, (error, collection) => {
            if (error) return done(error)

            return collection.insertOne({
              [TYPE]: pad(INIT),
              collection: pad(this._collection, COL_NAME_MAX_CHARS),
              [VALUE]: pad(INIT),
              [TIMESTAMP]: Date.now(),
              [OLD_VAL]: pad(INIT, this._docMaxChars)
            }, (error) => {
              if (error) return done(error)
              return done(null, collection)
            })
          })
        })
    } catch (error) {
      return done(error)
    }
  }

  _subscribe (done) {
    let wait = true
    let skip = null

    try {
      this._stream = this.log.find({}, {
        tailable: true,
        awaitdata: true
      })
        .stream()

      this._stream.on('data', (data) => {
        // skip the inital data until there has been no data for 500ms
        if (skip) clearTimeout(skip)
        if (wait) {
          skip = setTimeout(() => {
            wait = false
          }, 500)
          return true
        }

        try {
          let name = _.get(data, 'collection').trim()

          // filter old records
          if (name !== this._collection) return true

          let type = _.get(data, TYPE, '').trim()
          let value = _.get(data, VALUE).trim()
          let old_val = JSON.parse(_.get(data, 'old_val').trim())

          if (type.match(/delete/i)) {
            this.emit('change', { old_val, new_val: null })
          } else {
            this.collection.findOne({ _id: this._driver.ObjectId(value) }, (error, result) => {
              if (error) return debug('find error %O', error)
              this.emit('change', { old_val, new_val: result })
            })
          }
        } catch (error) {
          debug('stream data error: %O', error)
        }
      })
        .on('error', (error) => {
          debug('stream error: %O', error)
        })

      return done(null, this)
    } catch (error) {
      return done(error)
    }
  }

  insertOne (doc, options, callback) {
    if (_.isFunction(options)) {
      callback = options
      options = {}
    }
    callback = _.isFunction(callback) ? callback : () => false

    return new Promise((resolve, reject) => {
      let done = doneFactory(callback, resolve, reject)

      return this.collection.insertOne(doc, options, (error, result) => {
        if (error) return done(error)
        if (!result) return done(null, result)

        this._appendLog('insertOne', result.insertedId, null, (error) => {
          if (error) debug('insertOne error: %O', error)
          return done(null, result)
        })
      })
    })
  }

  insertMany (docs, options, callback) {
    if (_.isFunction(options)) {
      callback = options
      options = {}
    }
    callback = _.isFunction(callback) ? callback : () => false

    return new Promise((resolve, reject) => {
      let done = doneFactory(callback, resolve, reject)

      return this.collection.insertMany(docs, options, (error, result) => {
        if (error) return done(error)
        if (!result) return done(null, result)

        return Promise.all(_.map(result.insertedIds, (id) => {
          return this._appendLog('insertOne', id, null)
        }))
          .then(() => {
            return done(null, result)
          }, error => {
            debug('insertMany error: %O', error)
            return done(null, result)
          })
      })
    })
  }

  updateOne () {

  }

  updateMany () {

  }

  findOneAndUpdate () {

  }

  deleteOne () {

  }

  deleteMany () {

  }

  findOneAndDelete () {

  }
}