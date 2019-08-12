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
      error = error instanceof Error ? error : new Error(error);
      callback(error);
      return reject(error);
    }
    callback(null, success);
    return resolve(success);
  };
}

// leader record schema properties




// raft states
var LEADER = 'leader';
var FOLLOWER = 'follower';

// leaderfeed status
var STOPPED = 'stopped';
var STOPPING = 'stopping';
var STARTING = 'starting';
var STARTED = 'started';

// events
var HEARTBEAT = 'heartbeat';
var HEARTBEAT_ERROR = 'heartbeat error';

var NEW_STATE = 'new state';
var NEW_LEADER = 'new leader';
var SUB_ERROR = 'subscribe error';
var SUB_STARTED = 'subscribe started';

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

var debug = Debug('feed:base');

var LeaderFeed = function (_EventEmitter) {
  inherits(LeaderFeed, _EventEmitter);

  /**
   * calculates the election timeout and stores common property values
   * @param options
   * @param DEFAULT_HEARTBEAT_INTERVAL
   */
  function LeaderFeed(options, DEFAULT_HEARTBEAT_INTERVAL, ChangeFeed) {
    classCallCheck(this, LeaderFeed);

    var _this = possibleConstructorReturn(this, (LeaderFeed.__proto__ || Object.getPrototypeOf(LeaderFeed)).call(this));

    debug('initializing leader feed');

    _this.id = hat();
    _this.state = null;
    _this.started = false;
    _this.status = STOPPED;
    _this.leader = null;
    _this.ChangeFeed = ChangeFeed;

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
   * creates a new changefeed
   * @param options
   * @param callback
   */


  createClass(LeaderFeed, [{
    key: 'changes',
    value: function changes(collection) {
      var _this2 = this;

      var callback = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : function () {
        return false;
      };

      callback = _.isFunction(callback) ? callback : function () {
        return false;
      };

      return new Promise(function (resolve, reject) {
        return new _this2.ChangeFeed(_this2, collection).changes(function (error, changefeed) {
          if (error) {
            callback(error);
            return reject(error);
          }
          callback(null, changefeed);
          return resolve(changefeed);
        });
      });
    }

    /**
     * starts the leaderfeed and subscription
     * @param options
     * @param callback
     * @returns {Promise}
     */

  }, {
    key: 'start',
    value: function start(options) {
      var _this3 = this;

      var callback = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : function () {
        return false;
      };

      callback = _.isFunction(callback) ? callback : function () {
        return false;
      };

      return new Promise(function (resolve, reject) {
        var done = doneFactory(function (error) {
          return error ? callback(error) : callback(null, _this3);
        }, function () {
          return resolve(_this3);
        }, reject);

        if (_this3.status === STARTING) return done(new Error('leaderfeed is currently starting'));
        if (_this3.status === STARTED) return done(new Error('leaderfeed already started'));
        if (!_.isObject(options) || _.isEmpty(options)) return done(new Error('invalid options'));
        _this3.status = STARTING;

        // start the heartbeat listener
        _this3.on(HEARTBEAT, function (leader) {
          if (leader !== _this3.id) debug('heartbeat from %s', leader);

          // check if a new leader has been elected
          if (_this3.leader && _this3.leader !== leader) _this3.emit(NEW_LEADER, leader);
          _this3.leader = leader;

          // if leader, do not time out self, otherwise restart the timeout
          _this3.leader === _this3.id ? _this3._clearElectionTimeout() : _this3._restartElectionTimeout();

          // if the the node thinks it is the leader but the heartbeat
          // says otherwise, change to follower
          if (_this3.state === LEADER && leader !== _this3.id) {
            _this3._clearHeartbeatInterval();
            return _this3._changeState(FOLLOWER);
          }
        }).on(SUB_ERROR, function (error) {
          debug('%s %O', SUB_ERROR, error);
          return _this3._changeState(FOLLOWER);
        }).on(SUB_STARTED, function () {
          _this3.status = STARTED;
          return _this3._changeState(FOLLOWER);
        }).on(HEARTBEAT_ERROR, function (error) {
          debug('%s %O', HEARTBEAT_ERROR, error);
          return _this3._changeState(FOLLOWER);
        });

        // if create successful, attempt to start
        return _this3._start(options, function (error) {
          if (error) {
            debug('error during start %O', error);
            return done(error);
          }

          debug('_start successful');

          // attempt to create
          return _this3.create(function (error) {
            if (error) {
              debug('error during create %O', error);
              return done(error);
            }

            debug('create successful');

            // if start and create are successful, attempt to subscribe
            debug('starting feed');
            return _this3.subscribe(done);
          });
        });
      });
    }

    /**
     * stops the leaderfeed
     * @param callback
     * @returns {Promise}
     */

  }, {
    key: 'stop',
    value: function stop(callback) {
      var _this4 = this;

      return new Promise(function (resolve, reject) {
        var done = doneFactory(callback, resolve, reject);

        switch (_this4.status) {
          case STARTING:
            return done('leaderfeed cannot be stopped while starting');
          case STOPPED:
            return done('leaderfeed is already stopped');
          case STOPPING:
            return done('leaderfeed is currently stopping');
          default:
            _this4.status = STOPPING;
            break;
        }

        _this4._clearHeartbeatInterval();
        _this4._clearElectionTimeout();
        _this4._unsubscribe(function (error) {
          // on error restart the services
          if (error) {
            _this4.state === LEADER ? _this4._restartHeartbeatInterval() : _this4._restartElectionTimeout();
            _this4.status = STARTED;
            return done(error);
          }
          _this4.status = STOPPED;
          return done();
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
      try {
        if (!this._createIfMissing) return done();

        return this._create(function (error) {
          if (error) return done(error);
          return done();
        });
      } catch (error) {
        return done(error);
      }
    }

    /**
     * elects the specified id or self if no id
     * @param id
     * @param callback
     * @returns {Promise}
     */

  }, {
    key: 'elect',
    value: function elect(id, callback) {
      var _this5 = this;

      if (_.isFunction(id)) {
        callback = id;
        id = this.id;
      }
      id = _.isString(id) ? id : this.id;

      return new Promise(function (resolve, reject) {
        var done = doneFactory(callback, resolve, reject);
        return _this5._elect(id, done);
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
      var _this6 = this;

      try {
        return this._subscribe(function (error) {
          if (error) {
            debug('error during subscribe %O', error);
            _this6.emit();
            return done(error);
          }

          debug('subscribe successful');
          _this6.emit(SUB_STARTED);
          done(null, _this6);
        });
      } catch (error) {
        return done(error);
      }
    }

    /**
     * change the state of the current leaderfeed
     * @param state
     * @returns {Promise}
     */

  }, {
    key: '_changeState',
    value: function _changeState(state) {
      var _this7 = this;

      if (state === this.state) return false;
      debug('changed to state: %s', state);

      switch (state) {
        case LEADER:
          this.state = LEADER;
          this.leader = this.id;
          this._clearElectionTimeout();
          this.emit(NEW_STATE, state);

          // send the first heartbeat and start the heartbeat interval
          return this._heartbeat(function (error) {
            if (error) {
              // if unable to set the heartbeat, cleat the interval and become follower
              debug('error sending heartbeat %O', error);
              _this7._clearHeartbeatInterval();
              return _this7._changeState(FOLLOWER);
            }
            _this7._restartHeartbeatInterval();
          });

        case FOLLOWER:
          this.state = FOLLOWER;
          this.leader = null;
          this._restartElectionTimeout();
          this.emit(NEW_STATE, state);
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
      var _this8 = this;

      this._clearHeartbeatInterval();
      this._heartbeatInterval = setInterval(function () {
        return _this8._heartbeat(function (error) {
          // if there was an error updating the heartbeat, cancel the interval
          if (error) {
            _this8._clearHeartbeatInterval();
            _this8.emit(HEARTBEAT_ERROR, error);
          }
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
      var _this9 = this;

      this._clearElectionTimeout();
      this._electionTimeout = setTimeout(function () {
        return _this9._changeState(LEADER);
      }, this.randomElectionTimeout);
    }

    /**
     * retrns truthy if leaderfeed is the leader
     * @returns {boolean}
     */

  }, {
    key: '_create',


    /************************************************
     * Methods that should be overridden
     ************************************************/

    /**
     * should create the store/table/collection if it does not exist
     * and the createIfMissing option is set to true
     * and then call the done callback with error or no arguments
     * @param done
     * @return {*}
     * @private
     */
    value: function _create(done) {
      return done();
    }

    /**
     * should elect the id specified and callback done with error or no arguments if successful
     * @param id
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_elect',
    value: function _elect(id, done) {
      return done();
    }

    /**
     * should update the leader/heartbeat metadata with a timestamp
     * and callback with error as first argument or no arguments if successful
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_heartbeat',
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
     * should stop the subscription and callback done with error or no arguments if successful
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_unsubscribe',
    value: function _unsubscribe(done) {
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

module.exports = LeaderFeed;
