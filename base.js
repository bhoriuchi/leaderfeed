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



// raft states
var LEADER = 'leader';
var FOLLOWER = 'follower';

// events
var HEARTBEAT = 'heartbeat';

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

var debug = Debug('feed:rethinkdb');

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
          if (leader !== _this2.id) debug('heartbeat from %s', value);

          // check if a new leader has been elected
          if (_this2.leader && _this2.leader !== leader) _this2.emit(NEW_LEADER, leader);
          _this2.leader = leader;

          // if leader, do not time out self, otherwise restart the timeout
          _this2.leader === _this2.id ? _this2._clearElectionTimeout() : _this2._restartElectionTimeout();

          // if the the node thinks it is the leader but the heartbeat
          // says otherwise, change to follower
          if (_this2.state === LEADER && leader !== _this2.id) return _this2._changeState(FOLLOWER);
        });

        return _this2._start(options, function (error) {
          if (error) {
            debug('error during start %O', error);
            return done(error);
          }

          debug('starting feed');
          return _this2._subscribe(done);
        });
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
      var _this3 = this;

      if (state === this.state) return false;
      debug('changed state %s', state);
      this.emit(NEW_STATE, state);

      switch (state) {
        case LEADER:
          this.state = LEADER;
          this._clearElectionTimeout();

          // send the first heartbeat and start the heartbeat interval
          return this._heartbeat(function (error) {
            if (error) {
              // if unable to set the heartbeat, cleat the interval and become follower
              debug('error sending heartbeat %O', error);
              _this3._clearHeartbeatInterval();
              return _this3._changeState(FOLLOWER);
            }
            _this3._restartHeartbeatInterval();
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
      var _this4 = this;

      this._clearHeartbeatInterval();
      this._heartbeatInterval = setInterval(function () {
        return _this4._heartbeat(function (error) {
          // if there was an error updating the heartbeat, cancel the interval
          if (error) _this4._clearHeartbeatInterval();
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
      var _this5 = this;

      this._clearElectionTimeout();
      this._electionTimeout = setTimeout(function () {
        return _this5._changeState(LEADER);
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
