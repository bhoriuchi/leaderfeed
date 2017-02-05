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
    if (!driver) throw new Error('no driver specified')
    if (_.isObject(db)) {
      options = db
      db = DEFAULT_DB
    }

    super(options, DEFAULT_HEARTBEAT_INTERVAL)

    this.r = null
    this._db = db || DEFAULT_DB
    this._table = null
    this._driver = driver
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
      let { table, connection } = options

      if (!_.isString(table)) return done(new Error('missing table argument'))
      this._table = table

      // intelligently connect to the rethinkdb database
      if (!_.isFunction(this._driver.connect) || _.has(this._driver, '_poolMaster')) {
        this.connection = null
        this.r = !_.has(this._driver, '_poolMaster')
          ? this._driver(this._options)
          : this._driver
        return done()

      } else {
        if (connection) {
          this.connection = connection
          this.r = this._driver
          return done()
        } else {
          return this._driver.connect(this.options, (error, conn) => {
            if (error) return done(error)
            this.connection = conn
            this.r = this._driver
            return done()
          })
        }
      }
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
    let r = this.r

    // create the db and table if they do not exist
    return r.dbList()
      .contains(this._db)
      .branch(
        r.db(this._db)
          .tableList()
          .contains(this._table)
          .branch(
            true,
            r.db(this._db).tableCreate(this._table, { primaryKey: ID })
          ),
        r.dbCreate(this._db)
          .do(() => r.db(this._db).tableCreate(this._table, { primaryKey: ID }))
      )
      .run(this.connection)
      .then(() => {
        this.db = r.db(this._db)
        this.table = this.db.table(this._table)
        return done()
      }, done)
  }

  /**
   * sends a heartbeat
   * @param done
   * @private
   */
  _heartbeat (done) {
    debug('heartbeat update')
    let r = this.r
    let table = this.table

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
        return done(error)
      })
  }

  /**
   * sets up a subscription
   * @param done
   * @returns {Promise.<TResult>}
   * @private
   */
  _subscribe (done) {
    try {
      return this.table.changes()
        .run(this.connection)
        .then((cursor) => {
          debug('changefeed started')

          cursor.each((error, change) => {
            if (error) {
              debug('changefeed error: %O', error)
              return this.emit(SUB_ERROR, error)
            }

            let data = _.get(change, 'new_val')
            let id = _.get(data, ID)
            let value = _.get(data, VALUE)

            // emit the appropriate event
            return id === LEADER
              ? this.emit(HEARTBEAT, value)
              : this.emit(CHANGE, change)
          })

          return done(null, this)
        }, done)
    } catch (error) {
      done(error)
    }
  }
}