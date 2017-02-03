'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var _ = _interopDefault(require('lodash'));
var Debug = _interopDefault(require('debug'));
var EventEmitter = _interopDefault(require('events'));
var hat = _interopDefault(require('hat'));

// leader record schema properties
var VALUE = 'value';
var TIMESTAMP = 'timestamp';

// raft states
var LEADER = 'leader';
var FOLLOWER = 'follower';

// events
var HEARTBEAT = 'heartbeat';
var CHANGE = 'change';
var NEW_STATE = 'new state';
var NEW_LEADER = 'new leader';
var SUB_ERROR = 'subscribe error';
var SUB_STARTED = 'subscribe started';

var CONST = {
  VALUE: VALUE,
  TIMESTAMP: TIMESTAMP,
  LEADER: LEADER,
  FOLLOWER: FOLLOWER,
  HEARTBEAT: HEARTBEAT,
  CHANGE: CHANGE,
  NEW_LEADER: NEW_LEADER,
  NEW_STATE: NEW_STATE,
  SUB_ERROR: SUB_ERROR,
  SUB_STARTED: SUB_STARTED
};

/**
 * create a new done handler
 * @private
 * @param callback
 * @param resolve
 * @param reject
 * @returns {done}
 */
function doneFactory(callback, resolve, reject) {
  callback = _.isFunction(callback) ? callback : function () {
    return false;
  };
  resolve = _.isFunction(resolve) ? resolve : function () {
    return false;
  };
  reject = _.isFunction(reject) ? reject : function () {
    return false;
  };

  return function done(error, success) {
    if (error) {
      callback(error);
      return reject(success);
    }
    callback(null, success);
    return resolve(success);
  };
}

var classCallCheck = function (instance, Constructor) {
  if (!(instance instanceof Constructor)) {
    throw new TypeError("Cannot call a class as a function");
  }
};

var createClass = function () {
  function defineProperties(target, props) {
    for (var i = 0; i < props.length; i++) {
      var descriptor = props[i];
      descriptor.enumerable = descriptor.enumerable || false;
      descriptor.configurable = true;
      if ("value" in descriptor) descriptor.writable = true;
      Object.defineProperty(target, descriptor.key, descriptor);
    }
  }

  return function (Constructor, protoProps, staticProps) {
    if (protoProps) defineProperties(Constructor.prototype, protoProps);
    if (staticProps) defineProperties(Constructor, staticProps);
    return Constructor;
  };
}();





var defineProperty = function (obj, key, value) {
  if (key in obj) {
    Object.defineProperty(obj, key, {
      value: value,
      enumerable: true,
      configurable: true,
      writable: true
    });
  } else {
    obj[key] = value;
  }

  return obj;
};



var inherits = function (subClass, superClass) {
  if (typeof superClass !== "function" && superClass !== null) {
    throw new TypeError("Super expression must either be null or a function, not " + typeof superClass);
  }

  subClass.prototype = Object.create(superClass && superClass.prototype, {
    constructor: {
      value: subClass,
      enumerable: false,
      writable: true,
      configurable: true
    }
  });
  if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass;
};











var possibleConstructorReturn = function (self, call) {
  if (!self) {
    throw new ReferenceError("this hasn't been initialised - super() hasn't been called");
  }

  return call && (typeof call === "object" || typeof call === "function") ? call : self;
};

var debug$1 = Debug('feed:rethinkdb');

var LeaderFeed = function (_EventEmitter) {
  inherits(LeaderFeed, _EventEmitter);

  /**
   * calculates the election timeout and stores common property values
   * @param options
   * @param DEFAULT_HEARTBEAT_INTERVAL
   */
  function LeaderFeed(options, DEFAULT_HEARTBEAT_INTERVAL) {
    classCallCheck(this, LeaderFeed);

    var _this = possibleConstructorReturn(this, (LeaderFeed.__proto__ || Object.getPrototypeOf(LeaderFeed)).call(this));

    debug$1('initializing leader feed');

    _this.id = hat();
    _this.state = null;

    _this._options = options || {};
    _this._electionTimeout = null;
    _this._heartbeatInterval = null;

    // get common options
    var _this$_options = _this._options,
        createIfMissing = _this$_options.createIfMissing,
        heartbeatIntervalMs = _this$_options.heartbeatIntervalMs,
        electionTimeoutMinMs = _this$_options.electionTimeoutMinMs,
        electionTimeoutMaxMs = _this$_options.electionTimeoutMaxMs;


    delete _this._options.createIfMissing;
    delete _this._options.heartbeatIntervalMs;
    delete _this._options.electionTimeoutMinMs;
    delete _this._options.electionTimeoutMaxMs;

    var min = _.isNumber(electionTimeoutMinMs) ? Math.floor(electionTimeoutMinMs) : null;
    var max = _.isNumber(electionTimeoutMaxMs) ? Math.floor(electionTimeoutMaxMs) : null;

    _this._createIfMissing = _.isBoolean(createIfMissing) ? createIfMissing : true;

    // calculate timeout thresholds
    _this._heartbeatIntervalMs = _.isNumber(heartbeatIntervalMs) ? Math.floor(heartbeatIntervalMs) : DEFAULT_HEARTBEAT_INTERVAL;

    _this._electionTimeoutMinMs = min && min >= _this._heartbeatIntervalMs * 2 ? min : _this._heartbeatIntervalMs * 2;
    _this._electionTimeoutMaxMs = max && max >= _this._electionTimeoutMinMs * 2 ? max : _this._electionTimeoutMinMs * 2;
    return _this;
  }

  /**
   * starts the leaderfeed and subscription
   * @param options
   * @param callback
   * @returns {Promise}
   */


  createClass(LeaderFeed, [{
    key: 'start',
    value: function start(options) {
      var _this2 = this;

      var callback = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : function () {
        return false;
      };

      if (!_.isObject(options) || _.isEmpty(options)) throw new Error('invalid options');
      if (!_.isFunction(callback)) throw new Error('invalid callback');

      return new Promise(function (resolve, reject) {
        var done = doneFactory(function (error) {
          return error ? callback(error) : callback(null, _this2);
        }, function () {
          return resolve(_this2);
        }, reject);

        // start the heartbeat listener
        _this2.on(HEARTBEAT, function (leader) {
          if (leader !== _this2.id) debug$1('heartbeat from %s', leader);

          // check if a new leader has been elected
          if (_this2.leader && _this2.leader !== leader) _this2.emit(NEW_LEADER, leader);
          _this2.leader = leader;

          // if leader, do not time out self, otherwise restart the timeout
          _this2.leader === _this2.id ? _this2._clearElectionTimeout() : _this2._restartElectionTimeout();

          // if the the node thinks it is the leader but the heartbeat
          // says otherwise, change to follower
          if (_this2.state === LEADER && leader !== _this2.id) return _this2._changeState(FOLLOWER);
        }).on(SUB_ERROR, function (error) {
          return _this2._changeState(FOLLOWER);
        }).on(SUB_STARTED, function () {
          return _this2._changeState(FOLLOWER);
        });

        // if create successful, attempt to start
        return _this2._start(options, function (error) {
          if (error) {
            debug$1('error during start %O', error);
            return done(error);
          }

          debug$1('_start successful');

          // attempt to create
          return _this2.create(function (error) {
            if (error) {
              debug$1('error during create %O', error);
              return done(error);
            }

            debug$1('create successful');

            // if start and create are successful, attempt to subscribe
            debug$1('starting feed');
            return _this2.subscribe(done);
          });
        });
      });
    }

    /**
     * creates a db/store/table/collection if missing and createIfMissing is true
     * @param done
     * @return {*}
     */

  }, {
    key: 'create',
    value: function create(done) {
      if (!this._createIfMissing) return done();

      return this._create(function (error) {
        if (error) return done(error);
        return done();
      });
    }

    /**
     * start subscription
     * @param done
     * @return {*}
     */

  }, {
    key: 'subscribe',
    value: function subscribe(done) {
      var _this3 = this;

      return this._subscribe(function (error) {
        if (error) {
          debug$1('error during subscribe %O', error);
          _this3.emit();
          return done(error);
        }

        debug$1('subscribe successful');
        _this3.emit(SUB_STARTED);
        done(null, _this3);
      });
    }

    /**
     * change the state of the current leaderfeed
     * @param state
     * @returns {Promise}
     */

  }, {
    key: '_changeState',
    value: function _changeState(state) {
      var _this4 = this;

      if (state === this.state) return false;
      debug$1('changed state %s', state);
      this.emit(NEW_STATE, state);

      switch (state) {
        case LEADER:
          this.state = LEADER;
          this._clearElectionTimeout();

          // send the first heartbeat and start the heartbeat interval
          return this._heartbeat(function (error) {
            if (error) {
              // if unable to set the heartbeat, cleat the interval and become follower
              debug$1('error sending heartbeat %O', error);
              _this4._clearHeartbeatInterval();
              return _this4._changeState(FOLLOWER);
            }
            _this4._restartHeartbeatInterval();
          });

        case FOLLOWER:
          this.state = FOLLOWER;
          this._restartElectionTimeout();
          return Promise.resolve();
      }
    }

    /**
     * clear the heartbeat interval
     * @private
     */

  }, {
    key: '_clearHeartbeatInterval',
    value: function _clearHeartbeatInterval() {
      if (this._heartbeatInterval) clearInterval(this._heartbeatInterval);
      this._heartbeatInterval = null;
    }

    /**
     * clear heartbeat interval and restart
     * @private
     */

  }, {
    key: '_restartHeartbeatInterval',
    value: function _restartHeartbeatInterval() {
      var _this5 = this;

      this._clearHeartbeatInterval();
      this._heartbeatInterval = setInterval(function () {
        return _this5._heartbeat(function (error) {
          // if there was an error updating the heartbeat, cancel the interval
          if (error) _this5._clearHeartbeatInterval();
        });
      }, this._heartbeatIntervalMs);
    }

    /**
     * clear election timeout
     * @private
     */

  }, {
    key: '_clearElectionTimeout',
    value: function _clearElectionTimeout() {
      if (this._electionTimeout) clearTimeout(this._electionTimeout);
      this._electionTimeout = null;
    }

    /**
     * clear election timeout and restart
     * @private
     */

  }, {
    key: '_restartElectionTimeout',
    value: function _restartElectionTimeout() {
      var _this6 = this;

      this._clearElectionTimeout();
      this._electionTimeout = setTimeout(function () {
        return _this6._changeState(LEADER);
      }, this.randomElectionTimeout);
    }

    /**
     * retrns truthy if leaderfeed is the leader
     * @returns {boolean}
     */

  }, {
    key: '_heartbeat',


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
    value: function _heartbeat(done) {
      return done();
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

  }, {
    key: '_subscribe',
    value: function _subscribe(done) {
      return done();
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

  }, {
    key: '_start',
    value: function _start(options, done) {
      return done();
    }

    /**
     * should create the store/table/collection if it does not exist
     * and the createIfMissing option is set to true
     * and then call the done callback with error or no arguments
     * @param done
     * @return {*}
     * @private
     */

  }, {
    key: '_create',
    value: function _create(done) {
      return done();
    }
  }, {
    key: 'isLeader',
    get: function get$$1() {
      return this.state === LEADER || this.leader === this.id;
    }

    /**
     * generates a random number within the election timeout threshold
     * @returns {number}
     */

  }, {
    key: 'randomElectionTimeout',
    get: function get$$1() {
      return Math.floor(Math.random() * (this._electionTimeoutMaxMs - this._electionTimeoutMinMs + 1) + this._electionTimeoutMinMs);
    }
  }]);
  return LeaderFeed;
}(EventEmitter);

var debug = Debug('feed:rethinkdb');
var DEFAULT_DB = 'test';
var ID = 'id';
var DEFAULT_HEARTBEAT_INTERVAL = 1000;

var RethinkLeaderFeed = function (_LeaderFeed) {
  inherits(RethinkLeaderFeed, _LeaderFeed);

  /**
   * initializes the leaderfeed
   * @param driver
   * @param db
   * @param options
   */
  function RethinkLeaderFeed(driver, db, options) {
    classCallCheck(this, RethinkLeaderFeed);

    if (!driver) throw new Error('no driver specified');
    if (_.isObject(db)) {
      options = db;
      db = DEFAULT_DB;
    }

    var _this = possibleConstructorReturn(this, (RethinkLeaderFeed.__proto__ || Object.getPrototypeOf(RethinkLeaderFeed)).call(this, options, DEFAULT_HEARTBEAT_INTERVAL));

    _this.r = null;
    _this.db = db || DEFAULT_DB;
    _this.table = null;
    _this.collection = null;
    _this._driver = driver;
    return _this;
  }

  /**
   * establishes a connection
   * @param options
   * @param done
   * @returns {*}
   * @private
   */


  createClass(RethinkLeaderFeed, [{
    key: '_start',
    value: function _start(options, done) {
      var _this2 = this;

      try {
        var table = options.table,
            connection = options.connection;


        if (!_.isString(table)) return done(new Error('missing table argument'));
        this.table = table;

        // intelligently connect to the rethinkdb database
        if (!_.isFunction(this._driver.connect) || _.has(this._driver, '_poolMaster')) {
          this.connection = null;
          this.r = !_.has(this._driver, '_poolMaster') ? this._driver(this._options) : this._driver;
          return done();
        } else {
          if (connection) {
            this.connection = connection;
            this.r = this._driver;
            return done();
          } else {
            return this._driver.connect(this.options, function (error, conn) {
              if (error) return done(error);
              _this2.connection = conn;
              _this2.r = _this2._driver;
              return done();
            });
          }
        }
      } catch (error) {
        return done(error);
      }
    }

    /**
     * create the db and table if they do not exist
     * @param done
     * @private
     */

  }, {
    key: '_create',
    value: function _create(done) {
      var _this3 = this;

      var r = this.r;

      // create the db and table if they do not exist
      return r.dbList().contains(this.db).branch(r.db(this.db).tableList().contains(this.table).branch(true, r.db(this.db).tableCreate(this.table, { primaryKey: ID })), r.dbCreate(this.db).do(function () {
        return r.db(_this3.db).tableCreate(_this3.table, { primaryKey: ID });
      })).run(this.connection).then(function () {
        _this3.collection = r.db(_this3.db).table(_this3.table);
        return done();
      }, done);
    }

    /**
     * sends a heartbeat
     * @param done
     * @private
     */

  }, {
    key: '_heartbeat',
    value: function _heartbeat(done) {
      var _table$insert,
          _this4 = this;

      debug('heartbeat update');
      var r = this.r;
      var table = this.collection;

      // insert a heartbeat
      table.insert((_table$insert = {}, defineProperty(_table$insert, ID, LEADER), defineProperty(_table$insert, VALUE, this.id), defineProperty(_table$insert, TIMESTAMP, r.now()), _table$insert), {
        durability: 'hard',
        conflict: 'update'
      }).do(function (summary) {
        return summary('errors').ne(0).branch(r.error(summary('first_error')), true);
      }).run(this.connection).then(function () {
        return done();
      }, function (error) {
        done(error);
        return _this4._changeState(FOLLOWER);
      });
    }

    /**
     * sets up a subscription
     * @param done
     * @returns {Promise.<TResult>}
     * @private
     */

  }, {
    key: '_subscribe',
    value: function _subscribe(done) {
      var _this5 = this;

      var r = this.r,
          connection = this.connection,
          table = this.table,
          db = this.db;


      return r.db(db).table(table).changes().run(connection).then(function (cursor) {
        debug('changefeed started');

        cursor.each(function (error, change) {
          if (error) {
            debug('changefeed error: %O', error);
            return _this5.emit(SUB_ERROR, error);
          }

          var data = _.get(change, 'new_val');
          var id = _.get(data, ID);
          var value = _.get(data, VALUE);

          // emit the appropriate event
          return id === LEADER ? _this5.emit(HEARTBEAT, value) : _this5.emit(CHANGE, change);
        });

        return done(null, _this5);
      }, done);
    }
  }]);
  return RethinkLeaderFeed;
}(LeaderFeed);

var index = {
  CONST: CONST,
  RethinkDB: RethinkLeaderFeed
};

module.exports = index;
