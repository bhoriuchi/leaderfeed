'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var _ = _interopDefault(require('lodash'));
var Debug = _interopDefault(require('debug'));
var EventEmitter = _interopDefault(require('events'));
var hat = _interopDefault(require('hat'));

// leader record schema properties
var VALUE = 'value';
var TIMESTAMP = 'timestamp';
var TYPE = 'type';

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
var CHANGE = 'change';
var NEW_STATE = 'new state';
var NEW_LEADER = 'new leader';
var SUB_ERROR = 'subscribe error';
var SUB_STARTED = 'subscribe started';

var CONST = {
  VALUE: VALUE,
  TIMESTAMP: TIMESTAMP,
  TYPE: TYPE,
  LEADER: LEADER,
  FOLLOWER: FOLLOWER,
  STOPPED: STOPPED,
  STOPPING: STOPPING,
  STARTING: STARTING,
  STARTED: STARTED,
  HEARTBEAT: HEARTBEAT,
  HEARTBEAT_ERROR: HEARTBEAT_ERROR,
  CHANGE: CHANGE,
  NEW_LEADER: NEW_LEADER,
  NEW_STATE: NEW_STATE,
  SUB_ERROR: SUB_ERROR,
  SUB_STARTED: SUB_STARTED
};

function pad(str) {
  var len = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 32;

  return String(new Array(len + 1).join(' ') + str).slice(-1 * Math.abs(len));
}

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

var debug$1 = Debug('feed:base');

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
    _this.started = false;
    _this.status = STOPPED;

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

      callback = _.isFunction(callback) ? callback : function () {
        return false;
      };

      return new Promise(function (resolve, reject) {
        var done = doneFactory(function (error) {
          return error ? callback(error) : callback(null, _this2);
        }, function () {
          return resolve(_this2);
        }, reject);

        if (_this2.status === STARTING) return done(new Error('leaderfeed is currently starting'));
        if (_this2.status === STARTED) return done(new Error('leaderfeed already started'));
        if (!_.isObject(options) || _.isEmpty(options)) return done(new Error('invalid options'));
        _this2.status = STARTING;

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
          debug$1('%s %O', SUB_ERROR, error);
          return _this2._changeState(FOLLOWER);
        }).on(SUB_STARTED, function () {
          _this2.status = STARTED;
          return _this2._changeState(FOLLOWER);
        }).on(HEARTBEAT_ERROR, function (error) {
          debug$1('%s %O', HEARTBEAT_ERROR, error);
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
     * stops the leaderfeed
     * @param callback
     * @returns {Promise}
     */

  }, {
    key: 'stop',
    value: function stop(callback) {
      var _this3 = this;

      return new Promise(function (resolve, reject) {
        var done = doneFactory(callback, resolve, reject);

        switch (_this3.status) {
          case STARTING:
            return done('leaderfeed cannot be stopped while starting');
          case STOPPED:
            return done('leaderfeed is already stopped');
          case STOPPING:
            return done('leaderfeed is currently stopping');
          default:
            _this3.status = STOPPING;
            break;
        }

        _this3._clearHeartbeatInterval();
        _this3._clearElectionTimeout();
        _this3._unsubscribe(function (error) {
          // on error restart the services
          if (error) {
            _this3.state === LEADER ? _this3._restartHeartbeatInterval() : _this3._restartElectionTimeout();
            _this3.status = STARTED;
            return done(error);
          }
          _this3.status = STOPPED;
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
      var _this4 = this;

      if (_.isFunction(id)) {
        callback = id;
        id = this.id;
      }
      id = _.isString(id) ? id : this.id;

      return new Promise(function (resolve, reject) {
        var done = doneFactory(callback, resolve, reject);
        return _this4._elect(id, done);
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
      var _this5 = this;

      try {
        return this._subscribe(function (error) {
          if (error) {
            debug$1('error during subscribe %O', error);
            _this5.emit();
            return done(error);
          }

          debug$1('subscribe successful');
          _this5.emit(SUB_STARTED);
          done(null, _this5);
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
      var _this6 = this;

      if (state === this.state) return false;
      debug$1('changed to state: %s', state);
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
              _this6._clearHeartbeatInterval();
              return _this6._changeState(FOLLOWER);
            }
            _this6._restartHeartbeatInterval();
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
      var _this7 = this;

      this._clearHeartbeatInterval();
      this._heartbeatInterval = setInterval(function () {
        return _this7._heartbeat(function (error) {
          // if there was an error updating the heartbeat, cancel the interval
          if (error) {
            _this7._clearHeartbeatInterval();
            _this7.emit(HEARTBEAT_ERROR, error);
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
      var _this8 = this;

      this._clearElectionTimeout();
      this._electionTimeout = setTimeout(function () {
        return _this8._changeState(LEADER);
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

var debug = Debug('feed:mongodb');
var DEFAULT_HEARTBEAT_INTERVAL = 1000;
var DEFAULT_COLLECTION_SIZE = 100000;
var DEFAULT_MAX_DOCS = 20;

var MongoLeaderFeed = function (_LeaderFeed) {
  inherits(MongoLeaderFeed, _LeaderFeed);

  /**
   * initializes the leaderfeed
   * @param driver
   * @param db
   * @param options
   */
  function MongoLeaderFeed(driver, url, options) {
    classCallCheck(this, MongoLeaderFeed);

    debug('initializing leader feed');
    if (_.isObject(url)) {
      options = url;
      url = null;
    }

    // check if the driver is a db or driver
    var _this = possibleConstructorReturn(this, (MongoLeaderFeed.__proto__ || Object.getPrototypeOf(MongoLeaderFeed)).call(this, options, DEFAULT_HEARTBEAT_INTERVAL));

    _this.db = _.isObject(driver) && !_.isFunction(driver.connect) ? driver : null;

    if (!driver && !_this.db) throw new Error('no driver specified');
    if (!_.isString(url) && !_this.db) throw new Error('no url specified');

    _this.collection = null;

    _this._url = url;
    _this._driver = _this.db ? null : driver;
    _this._collectionName = null;
    _this._stream = null;

    // mongo capped collection create options
    _this._createOpts = {
      capped: true,
      size: _this._options.collectionSizeBytes || DEFAULT_COLLECTION_SIZE,
      max: _this._options.collectionMaxDocs || DEFAULT_MAX_DOCS
    };

    // remove the size options
    delete _this._options.collectionSizeBytes;
    delete _this._options.collectionMaxDocs;
    return _this;
  }

  /**
   * establishes a connection
   * @param options
   * @param done
   * @returns {*}
   * @private
   */


  createClass(MongoLeaderFeed, [{
    key: '_start',
    value: function _start(options, done) {
      var _this2 = this;

      debug('called _start');
      try {
        var collection = options.collection;


        if (!_.isString(collection)) return done(new Error('missing collection argument'));
        this._collectionName = collection;

        // if the db is already connected we are done
        if (this.db) return done();

        // otherwise connect it
        return this._driver.connect(this._url, this._options, function (error, db) {
          if (error) return done(error);
          _this2.db = db;
          return done();
        });
      } catch (error) {
        debug('error in _start %O', error);
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

      try {
        return this.db.listCollections({ name: this._collectionName }).toArray(function (error, collections) {
          if (error) return done(error);

          // if the collection exists, get it and return done
          if (collections.length) {
            return _this3.db.collection(_this3._collectionName, function (error, collection) {
              if (error) return done(error);
              _this3.collection = collection;
              return done();
            });
          }

          // if the collection doesnt exist, create it and add 1 record
          return _this3.db.createCollection(_this3._collectionName, _this3._createOpts, function (error, collection) {
            var _collection$insertOne;

            if (error) return done(error);
            _this3.collection = collection;

            return collection.insertOne((_collection$insertOne = {}, defineProperty(_collection$insertOne, TYPE, pad(LEADER)), defineProperty(_collection$insertOne, VALUE, pad(_this3.id)), defineProperty(_collection$insertOne, TIMESTAMP, Date.now()), _collection$insertOne), function (error) {
              if (error) return done(error);
              return done();
            });
          });
        });
      } catch (error) {
        return done(error);
      }
    }

    /**
     * inserts a document
     * @param doc
     * @param done
     * @private
     */

  }, {
    key: '_put',
    value: function _put(doc, done) {
      try {
        this.collection.insertOne(doc, function (error) {
          return error ? done(error) : done();
        });
      } catch (error) {
        return done(error);
      }
    }

    /**
     * elects a new leader
     * @param id
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_elect',
    value: function _elect(id, done) {
      var _put2;

      return this._put((_put2 = {}, defineProperty(_put2, TYPE, pad(LEADER)), defineProperty(_put2, VALUE, pad(id)), defineProperty(_put2, TIMESTAMP, Date.now()), _put2), done);
    }

    /**
     * sends a heartbeat
     * @param done
     * @private
     */

  }, {
    key: '_heartbeat',
    value: function _heartbeat(done) {
      var _put3;

      debug('heartbeat update');

      this._put((_put3 = {}, defineProperty(_put3, TYPE, pad(LEADER)), defineProperty(_put3, VALUE, pad(this.id)), defineProperty(_put3, TIMESTAMP, Date.now()), _put3), done);
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
      var _this4 = this;

      try {
        this._stream = this.collection.find({}, {
          tailable: true,
          awaitdata: true
        }).stream();

        this._stream.on('data', function (data) {
          var type = _.get(data, TYPE, '').trim();
          var value = _.get(data, VALUE);

          return type === LEADER ? _this4.emit(HEARTBEAT, value) : _this4.emit(CHANGE, data);
        }).on('error', function (error) {
          debug('stream error: %O', error);
          return _this4.emit(SUB_ERROR, error);
        });

        return done(null, this);
      } catch (error) {
        return done(error);
      }
    }

    /**
     * stops the changefeed
     * @param done
     * @private
     */

  }, {
    key: '_unsubscribe',
    value: function _unsubscribe(done) {
      try {
        this._stream.destroy();
        return done();
      } catch (error) {
        return done(error);
      }
    }
  }]);
  return MongoLeaderFeed;
}(LeaderFeed);

var DEFAULT_HEARTBEAT_INTERVAL$1 = 1000;

var RedisLeaderFeed = function (_LeaderFeed) {
  inherits(RedisLeaderFeed, _LeaderFeed);

  function RedisLeaderFeed(driver, options) {
    classCallCheck(this, RedisLeaderFeed);

    if (!driver) throw new Error('no driver specified');
    if (!_.isObject(options) || _.isEmpty(options)) throw new Error('no options specefied');

    var _this = possibleConstructorReturn(this, (RedisLeaderFeed.__proto__ || Object.getPrototypeOf(RedisLeaderFeed)).call(this, options, DEFAULT_HEARTBEAT_INTERVAL$1));

    _this._driver = driver;
    _this.pub = null;
    _this.sub = null;
    _this.channel = null;
    _this._pubConnected = false;
    _this._subConnected = false;
    return _this;
  }

  /**
   * establishes a connection
   * @param options
   * @param done
   * @returns {*}
   * @private
   */


  createClass(RedisLeaderFeed, [{
    key: '_start',
    value: function _start(options, done) {
      var _this2 = this;

      try {
        var channel = options.channel;

        if (!_.isString(channel)) return done(new Error('missing channel'));
        this.channel = channel;

        // connect the pub and sub clients
        this.pub = this._driver.createClient(this._options);
        this.sub = this._driver.createClient(this._options);

        this.pub.once('ready', function () {
          _this2._pubConnected = true;
          if (_this2._pubConnected && _this2._subConnected) return done();
        }).once('error', function (error) {
          if (!_this2._pubConnected && !_this2._subConnected) return done(error);
        });

        this.sub.once('ready', function () {
          _this2._pubConnected = true;
          if (_this2._pubConnected && _this2._subConnected) return done();
        }).once('error', function (error) {
          if (!_this2._pubConnected && !_this2._subConnected) return done(error);
        });
      } catch (error) {
        return done(error);
      }
    }

    /**
     * passthrough
     * @param done
     * @private
     */

  }, {
    key: '_create',
    value: function _create(done) {
      // since redis uses pre-created db immediately call done
      return done();
    }

    /**
     * elects a new leader
     * @param id
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_elect',
    value: function _elect(id, done) {
      try {
        this.pub.publish(this.channel, id);
      } catch (error) {
        return done(error);
      }
    }

    /**
     * sends a heartbeat
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_heartbeat',
    value: function _heartbeat(done) {
      try {
        this.pub.publish(this.channel, this.id);
      } catch (error) {
        return done(error);
      }
    }

    /**
     * subscribes to a channel
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_subscribe',
    value: function _subscribe(done) {
      var _this3 = this;

      try {
        sub.on('subscribe', function (channel, count) {
          if (channel === _this3.channel) return done();
        }).on('message', function (channel, leader) {
          if (channel === _this3.channel) _this3.emit(HEARTBEAT, leader);
        });
        this.sub.subscribe(this.channel);
      } catch (error) {
        return done(error);
      }
    }

    /**
     * unsubscribes from a channel
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_unsubscribe',
    value: function _unsubscribe(done) {
      try {
        this.sub.unsubscribe();
        this.sub.quit();
        this.pub.quit();
        return done();
      } catch (error) {
        return done(error);
      }
    }
  }]);
  return RedisLeaderFeed;
}(LeaderFeed);

var debug$2 = Debug('feed:rethinkdb');
var DEFAULT_DB = 'test';
var ID$1 = 'id';
var DEFAULT_HEARTBEAT_INTERVAL$2 = 1000;

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

    var _this = possibleConstructorReturn(this, (RethinkLeaderFeed.__proto__ || Object.getPrototypeOf(RethinkLeaderFeed)).call(this, options, DEFAULT_HEARTBEAT_INTERVAL$2));

    _this.r = null;
    _this._db = db || DEFAULT_DB;
    _this._table = null;
    _this._driver = driver;
    _this._cursor = null;
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
        this._table = table;

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
      return r.dbList().contains(this._db).branch(r.db(this._db).tableList().contains(this._table).branch(true, r.db(this._db).tableCreate(this._table, { primaryKey: ID$1 })), r.dbCreate(this._db).do(function () {
        return r.db(_this3._db).tableCreate(_this3._table, { primaryKey: ID$1 });
      })).run(this.connection).then(function () {
        _this3.db = r.db(_this3._db);
        _this3.table = _this3.db.table(_this3._table);
        return done();
      }, done);
    }

    /**
     * puts a document
     * @param id
     * @param done
     * @private
     */

  }, {
    key: '_put',
    value: function _put(doc, done) {
      var _this4 = this;

      try {
        this.table.insert(doc, {
          durability: 'hard',
          conflict: 'update'
        }).do(function (summary) {
          return summary('errors').ne(0).branch(_this4.r.error(summary('first_error')), true);
        }).run(this.connection).then(function () {
          return done();
        }, function (error) {
          return done(error);
        });
      } catch (error) {
        return done(error);
      }
    }

    /**
     * elects a new leader
     * @param id
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_elect',
    value: function _elect(id, done) {
      var _put2;

      return this._put((_put2 = {}, defineProperty(_put2, ID$1, LEADER), defineProperty(_put2, VALUE, id), defineProperty(_put2, TIMESTAMP, this.r.now()), _put2), done);
    }

    /**
     * sends a heartbeat
     * @param done
     * @private
     */

  }, {
    key: '_heartbeat',
    value: function _heartbeat(done) {
      var _put3;

      debug$2('heartbeat update');

      return this._put((_put3 = {}, defineProperty(_put3, ID$1, LEADER), defineProperty(_put3, VALUE, this.id), defineProperty(_put3, TIMESTAMP, this.r.now()), _put3), done);
    }

    /**
     * sets up a changefeed
     * @param done
     * @returns {Promise.<TResult>}
     * @private
     */

  }, {
    key: '_subscribe',
    value: function _subscribe(done) {
      var _this5 = this;

      try {
        return this.table.changes().run(this.connection).then(function (cursor) {
          debug$2('changefeed started');

          cursor.each(function (error, change) {
            if (error) {
              debug$2('changefeed error: %O', error);
              return _this5.emit(SUB_ERROR, error);
            }

            var data = _.get(change, 'new_val');
            var id = _.get(data, ID$1);
            var value = _.get(data, VALUE);

            // emit the appropriate event
            return id === LEADER ? _this5.emit(HEARTBEAT, value) : _this5.emit(CHANGE, change);
          });

          return done(null, _this5);
        }, done);
      } catch (error) {
        done(error);
      }
    }

    /**
     * stops the changefeed
     * @param done
     * @private
     */

  }, {
    key: '_unsubscribe',
    value: function _unsubscribe(done) {
      try {
        return this._cursor.close(done);
      } catch (error) {
        return done(error);
      }
    }
  }]);
  return RethinkLeaderFeed;
}(LeaderFeed);

var index = {
  CONST: CONST,
  MongoDB: MongoLeaderFeed,
  Redis: RedisLeaderFeed,
  RethinkDB: RethinkLeaderFeed
};

module.exports = index;
