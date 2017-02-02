'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var _ = _interopDefault(require('lodash'));
var Debug = _interopDefault(require('debug'));
var EventEmitter = _interopDefault(require('events'));
var hat = _interopDefault(require('hat'));

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

// leader record schema properties
var VALUE = 'value';
var TIMESTAMP = 'timestamp';

// raft states
var LEADER = 'leader';
var FOLLOWER = 'follower';

// events
var CHANGE = 'change';
var NEW_STATE = 'new state';
var NEW_LEADER = 'new leader';

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

  function LeaderFeed(options, DEFAULT_HEARTBEAT_INTERVAL) {
    classCallCheck(this, LeaderFeed);

    var _this = possibleConstructorReturn(this, (LeaderFeed.__proto__ || Object.getPrototypeOf(LeaderFeed)).call(this));

    _this.id = hat();
    _this.state = null;

    _this._options = options || {};
    _this._electionTimeout = null;
    _this._heartbeatInterval = null;

    var _this$_options = _this._options,
        heartbeatIntervalMs = _this$_options.heartbeatIntervalMs,
        electionTimeoutMinMs = _this$_options.electionTimeoutMinMs,
        electionTimeoutMaxMs = _this$_options.electionTimeoutMaxMs;

    delete _this._options.heartbeatIntervalMs;
    delete _this._options.electionTimeoutMinMs;
    delete _this._options.electionTimeoutMaxMs;

    var min = _.isNumber(electionTimeoutMinMs) ? Math.floor(electionTimeoutMinMs) : null;
    var max = _.isNumber(electionTimeoutMaxMs) ? Math.floor(electionTimeoutMaxMs) : null;

    // calculate timeout thresholds
    _this._heartbeatIntervalMs = _.isNumber(heartbeatIntervalMs) ? Math.floor(heartbeatIntervalMs) : DEFAULT_HEARTBEAT_INTERVAL;

    _this._electionTimeoutMinMs = min && min >= _this._heartbeatIntervalMs * 2 ? min : _this._heartbeatIntervalMs * 2;
    _this._electionTimeoutMaxMs = max && max >= _this._electionTimeoutMinMs * 2 ? max : _this._electionTimeoutMinMs * 2;
    return _this;
  }

  /**
   * change the state of the current leaderfeed
   * @param state
   * @returns {Promise}
   */


  createClass(LeaderFeed, [{
    key: '_changeState',
    value: function _changeState(state) {
      var _this2 = this;

      if (state === this.state) return false;
      debug$1('changed state %s', state);
      this.emit(NEW_STATE, state);

      switch (state) {
        case LEADER:
          this.state = LEADER;
          this._clearElectionTimeout();

          // send the first heartbeat and start the heartbeat interval
          return this._heartbeat().then(function () {
            _this2._restartHeartbeatInterval();
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
      var _this3 = this;

      this._clearHeartbeatInterval();
      this._heartbeatInterval = setInterval(function () {
        return _this3._heartbeat();
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
      var _this4 = this;

      this._clearElectionTimeout();
      this._electionTimeout = setTimeout(function () {
        return _this4._changeState(LEADER);
      }, this.randomElectionTimeout);
    }

    /**
     * retrns truthy if leaderfeed is the leader
     * @returns {boolean}
     */

  }, {
    key: '_heartbeat',


    /*
     * Methods that should be overridden
     */
    value: function _heartbeat() {
      return Promise.resolve();
    }
  }, {
    key: '_subscribe',
    value: function _subscribe() {
      return Promise.resolve();
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

    debug('initializing leader feed');

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
   * establishes a connection and starts the heartbeat
   * @param table
   * @param connection
   * @param callback
   * @returns {Promise}
   */


  createClass(RethinkLeaderFeed, [{
    key: 'start',
    value: function start(table, connection, callback) {
      var _this2 = this;

      if (_.isFunction(connection)) {
        callback = connection;
        connection = null;
      }
      callback = _.isFunction(callback) ? callback : function () {
        return false;
      };

      debug('starting feed');

      // return a promise for flexibility
      return new Promise(function (resolve, reject) {

        // create a done handler that will handle callbacks and promises
        var done = doneFactory(callback, resolve, reject);

        try {
          if (!_.isString(table)) return done(new Error('missing table argument'));
          _this2.table = table;

          // intelligently connect to the rethinkdb database
          if (!_.isFunction(_this2._driver.connect) || _.has(_this2._driver, '_poolMaster')) {
            _this2.connection = null;
            _this2.r = !_.has(_this2._driver, '_poolMaster') ? _this2._driver(_this2._options) : _this2._driver;
            return _this2._subscribe(done);
          } else {
            if (connection) {
              _this2.connection = connection;
              _this2.r = _this2._driver;
              return _this2._subscribe(done);
            } else {
              return _this2._driver.connect(_this2.options, function (error, conn) {
                if (error) return done(error);
                _this2.connection = conn;
                _this2.r = _this2._driver;
                return _this2._subscribe(done);
              });
            }
          }
        } catch (error) {
          callback(error);
          return reject(error);
        }
      });
    }

    /**
     * sends a heartbeat
     * @returns {Promise.<TResult>}
     * @private
     */

  }, {
    key: '_heartbeat',
    value: function _heartbeat() {
      var _table$insert,
          _this3 = this;

      debug('heartbeat update');
      var r = this.r;
      var table = this.collection;

      // insert a heartbeat
      return table.insert((_table$insert = {}, defineProperty(_table$insert, ID, LEADER), defineProperty(_table$insert, VALUE, this.id), defineProperty(_table$insert, TIMESTAMP, r.now()), _table$insert), {
        durability: 'hard',
        conflict: 'update'
      }).do(function (summary) {
        return summary('errors').ne(0).branch(r.error(summary('first_error')), true);
      }).run(this.connection).then(function (summary) {
        return summary;
      }, function (error) {
        debug('heartbeat update error: %O', error);
        return _this3._changeState(FOLLOWER);
      });
    }

    /**
     * Sets up subscription
     * @private
     * @param self
     * @param done
     */

  }, {
    key: '_subscribe',
    value: function _subscribe(done) {
      var _this4 = this;

      var r = this.r,
          connection = this.connection,
          table = this.table,
          db = this.db;


      this.collection = r.db(db).table(table);

      return r.db(db).table(table).changes().run(connection).then(function (cursor) {
        debug('changefeed started');

        cursor.each(function (error, change) {
          if (error) {
            debug('changefeed error: %O', error);
            return _this4._changeState(FOLLOWER);
          }

          var data = _.get(change, 'new_val');
          var id = _.get(data, ID);
          var value = _.get(data, VALUE);

          switch (id) {
            case LEADER:
              if (value !== _this4.id) debug('heartbeat from %s', value);

              // check if a new leader has been elected
              if (_this4.leader && _this4.leader !== value) _this4.emit(NEW_LEADER, value);
              _this4.leader = value;

              // if leader, do not time out self, otherwise restart the timeout
              _this4.leader === _this4.id ? _this4._clearElectionTimeout() : _this4._restartElectionTimeout();

              return _this4.state === LEADER && value !== _this4.id ? _this4._changeState(FOLLOWER) : Promise.resolve();

            default:
              _this4.emit(CHANGE, change);
              break;
          }
        });
        done(null, _this4);

        // after the cursor is obtained, change state to follower
        return _this4._changeState(FOLLOWER);
      }, done);
    }
  }]);
  return RethinkLeaderFeed;
}(LeaderFeed);

var index = {
  RethinkDB: RethinkLeaderFeed
};

module.exports = index;
