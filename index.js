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

var ChangeFeed$1 = function (_EventEmitter) {
  inherits(ChangeFeed, _EventEmitter);

  function ChangeFeed(leaderfeed, collection) {
    classCallCheck(this, ChangeFeed);

    var _this = possibleConstructorReturn(this, (ChangeFeed.__proto__ || Object.getPrototypeOf(ChangeFeed)).call(this));

    _this._leaderfeed = leaderfeed;
    _this._collection = collection;
    return _this;
  }

  createClass(ChangeFeed, [{
    key: 'changes',
    value: function changes(done) {
      done();
      return Promise.resolve();
    }
  }]);
  return ChangeFeed;
}(EventEmitter);

function pad(str) {
  var len = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 32;

  return String(new Array(len + 1).join(' ') + str).slice(-1 * Math.abs(len));
}

var debug$1 = Debug('feed:mongodb:changes');
var logCollection = 'changefeed';
var DEFAULT_COLLECTION_SIZE$1 = 100000;
var DEFAULT_MAX_DOCS$1 = 20;
var INIT = 'init';
var OLD_VAL = 'old_val';
var DOC_MAX_CHARS = 5000;
var COL_NAME_MAX_CHARS = 50;

var MongoChangeFeed = function (_ChangeFeed) {
  inherits(MongoChangeFeed, _ChangeFeed);

  function MongoChangeFeed(leaderfeed, collection) {
    classCallCheck(this, MongoChangeFeed);

    var _this = possibleConstructorReturn(this, (MongoChangeFeed.__proto__ || Object.getPrototypeOf(MongoChangeFeed)).call(this, leaderfeed, collection));

    _this._driver = leaderfeed._driver;
    _this._url = leaderfeed._url;
    _this._options = leaderfeed._options;
    _this.db = leaderfeed.db;
    _this.log = null;

    // mongo capped collection create options
    _this._createOpts = {
      capped: true,
      size: _this._options.collectionSizeBytes || DEFAULT_COLLECTION_SIZE$1,
      max: _this._options.collectionMaxDocs || DEFAULT_MAX_DOCS$1
    };

    _this._docMaxChars = _this._options.docMaxChars || DOC_MAX_CHARS;

    // remove the size options
    delete _this._options.collectionSizeBytes;
    delete _this._options.collectionMaxDocs;
    delete _this._options.docMaxChars;
    return _this;
  }

  createClass(MongoChangeFeed, [{
    key: 'changes',
    value: function changes(done) {
      var _this2 = this;

      return this._connect(function (error) {
        if (error) return done(error);

        return _this2._createLog(function (error, log) {
          if (error) return done(error);
          _this2.log = log;

          return _this2.db.collection(_this2._collection, function (error, collection) {
            if (error) return done(error);
            _this2.collection = collection;

            return _this2._subscribe(function (error) {
              if (error) {
                debug$1('subscribe error %O', error);
                return done(error);
              }
              return done(null, _this2);
            });
          });
        });
      });
    }

    /**
     * connect to the database
     * @param done
     * @returns {*}
     * @private
     */

  }, {
    key: '_connect',
    value: function _connect(done) {
      var _this3 = this;

      try {
        // if the db is already connected we are done
        if (this.db) return done

        // otherwise connect it
        ();return this._driver.connect(this._url, this._options, function (error, db) {
          if (error) return done(error);
          _this3.db = db;
          return done();
        });
      } catch (error) {
        debug$1('error in _start %O', error);
        return done(error);
      }
    }
  }, {
    key: '_appendLog',
    value: function _appendLog(type, id, old_val, callback) {
      var _this4 = this;

      return new Promise(function (resolve, reject) {
        var done = doneFactory(callback, resolve, reject);
        try {
          var _this4$log$insertOne;

          return _this4.log.insertOne((_this4$log$insertOne = {}, defineProperty(_this4$log$insertOne, TYPE, pad(type)), defineProperty(_this4$log$insertOne, 'collection', pad(_this4._collection, COL_NAME_MAX_CHARS)), defineProperty(_this4$log$insertOne, VALUE, pad(id)), defineProperty(_this4$log$insertOne, TIMESTAMP, Date.now()), defineProperty(_this4$log$insertOne, OLD_VAL, pad(JSON.stringify(old_val), _this4._docMaxChars)), _this4$log$insertOne), function (error) {
            if (error) debug$1('appendLog error %O', error);
            return error ? done(error) : done();
          });
        } catch (error) {
          debug$1('append error %O', error);
          return done(error);
        }
      });
    }
  }, {
    key: '_createLog',
    value: function _createLog(done) {
      var _this5 = this;

      try {
        return this.db.listCollections({ name: logCollection }).toArray(function (error, collections) {
          if (error) return done(error

          // if the collection exists, get it and return done
          );if (collections.length) {
            return _this5.db.collection(logCollection, function (error, collection) {
              if (error) return done(error);
              return done(null, collection);
            });
          }

          // if the collection doesnt exist, create it and add 1 record
          return _this5.db.createCollection(logCollection, _this5._createOpts, function (error, collection) {
            var _collection$insertOne;

            if (error) return done(error);

            return collection.insertOne((_collection$insertOne = {}, defineProperty(_collection$insertOne, TYPE, pad(INIT)), defineProperty(_collection$insertOne, 'collection', pad(_this5._collection, COL_NAME_MAX_CHARS)), defineProperty(_collection$insertOne, VALUE, pad(INIT)), defineProperty(_collection$insertOne, TIMESTAMP, Date.now()), defineProperty(_collection$insertOne, OLD_VAL, pad(INIT, _this5._docMaxChars)), _collection$insertOne), function (error) {
              if (error) return done(error);
              return done(null, collection);
            });
          });
        });
      } catch (error) {
        return done(error);
      }
    }
  }, {
    key: '_subscribe',
    value: function _subscribe(done) {
      var _this6 = this;

      var wait = true;
      var skip = null;

      try {
        this._stream = this.log.find({}, {
          tailable: true,
          awaitdata: true
        }).stream();

        this._stream.on('data', function (data) {
          // skip the inital data until there has been no data for 500ms
          if (skip) clearTimeout(skip);
          if (wait) {
            skip = setTimeout(function () {
              wait = false;
            }, 500);
            return true;
          }

          try {
            var name = _.get(data, 'collection').trim

            // filter old records
            ();if (name !== _this6._collection) return true;

            var type = _.get(data, TYPE, '').trim();
            var value = _.get(data, VALUE).trim();
            var old_val = JSON.parse(_.get(data, 'old_val').trim());

            if (type.match(/delete/i)) {
              _this6.emit('change', { old_val: old_val, new_val: null });
            } else {
              _this6.collection.findOne({ _id: _this6._driver.ObjectId(value) }, function (error, result) {
                if (error) return debug$1('find error %O', error);
                _this6.emit('change', { old_val: old_val, new_val: result });
              });
            }
          } catch (error) {
            debug$1('stream data error: %O', error);
          }
        }).on('error', function (error) {
          debug$1('stream error: %O', error);
        });

        return done(null, this);
      } catch (error) {
        return done(error);
      }
    }
  }, {
    key: 'insertOne',
    value: function insertOne(doc, options, callback) {
      var _this7 = this;

      if (_.isFunction(options)) {
        callback = options;
        options = {};
      }
      callback = _.isFunction(callback) ? callback : function () {
        return false;
      };

      return new Promise(function (resolve, reject) {
        var done = doneFactory(callback, resolve, reject);

        return _this7.collection.insertOne(doc, options, function (error, result) {
          if (error) return done(error);
          if (!result) return done(null, result);

          _this7._appendLog('insertOne', result.insertedId, null, function (error) {
            if (error) debug$1('insertOne error: %O', error);
            return done(null, result);
          });
        });
      });
    }
  }, {
    key: 'insertMany',
    value: function insertMany(docs, options, callback) {
      var _this8 = this;

      if (_.isFunction(options)) {
        callback = options;
        options = {};
      }
      callback = _.isFunction(callback) ? callback : function () {
        return false;
      };

      return new Promise(function (resolve, reject) {
        var done = doneFactory(callback, resolve, reject);

        return _this8.collection.insertMany(docs, options, function (error, result) {
          if (error) return done(error);
          if (!result) return done(null, result);

          return Promise.all(_.map(result.insertedIds, function (id) {
            return _this8._appendLog('insertOne', id, null);
          })).then(function () {
            return done(null, result);
          }, function (error) {
            debug$1('insertMany error: %O', error);
            return done(null, result);
          });
        });
      });
    }
  }, {
    key: 'updateOne',
    value: function updateOne() {}
  }, {
    key: 'updateMany',
    value: function updateMany() {}
  }, {
    key: 'findOneAndUpdate',
    value: function findOneAndUpdate() {}
  }, {
    key: 'deleteOne',
    value: function deleteOne() {}
  }, {
    key: 'deleteMany',
    value: function deleteMany() {}
  }, {
    key: 'findOneAndDelete',
    value: function findOneAndDelete() {}
  }]);
  return MongoChangeFeed;
}(ChangeFeed$1);

var debug$2 = Debug('feed:base');

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

    debug$2('initializing leader feed');

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
          if (leader !== _this3.id) debug$2('heartbeat from %s', leader

          // check if a new leader has been elected
          );if (_this3.leader && _this3.leader !== leader) _this3.emit(NEW_LEADER, leader);
          _this3.leader = leader;

          // if leader, do not time out self, otherwise restart the timeout
          _this3.leader === _this3.id ? _this3._clearElectionTimeout() : _this3._restartElectionTimeout

          // if the the node thinks it is the leader but the heartbeat
          // says otherwise, change to follower
          ();if (_this3.state === LEADER && leader !== _this3.id) return _this3._changeState(FOLLOWER);
        }).on(SUB_ERROR, function (error) {
          debug$2('%s %O', SUB_ERROR, error);
          return _this3._changeState(FOLLOWER);
        }).on(SUB_STARTED, function () {
          _this3.status = STARTED;
          return _this3._changeState(FOLLOWER);
        }).on(HEARTBEAT_ERROR, function (error) {
          debug$2('%s %O', HEARTBEAT_ERROR, error);
          return _this3._changeState(FOLLOWER);
        }

        // if create successful, attempt to start
        );return _this3._start(options, function (error) {
          if (error) {
            debug$2('error during start %O', error);
            return done(error);
          }

          debug$2('_start successful'

          // attempt to create
          );return _this3.create(function (error) {
            if (error) {
              debug$2('error during create %O', error);
              return done(error);
            }

            debug$2('create successful'

            // if start and create are successful, attempt to subscribe
            );debug$2('starting feed');
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
            debug$2('error during subscribe %O', error);
            _this6.emit();
            return done(error);
          }

          debug$2('subscribe successful');
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
      debug$2('changed to state: %s', state);

      switch (state) {
        case LEADER:
          this.state = LEADER;
          this.leader = this.id;
          this._clearElectionTimeout();
          this.emit(NEW_STATE, state

          // send the first heartbeat and start the heartbeat interval
          );return this._heartbeat(function (error) {
            if (error) {
              // if unable to set the heartbeat, cleat the interval and become follower
              debug$2('error sending heartbeat %O', error);
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
    var _this = possibleConstructorReturn(this, (MongoLeaderFeed.__proto__ || Object.getPrototypeOf(MongoLeaderFeed)).call(this, options, DEFAULT_HEARTBEAT_INTERVAL, MongoChangeFeed));

    _this.db = _.isObject(driver) && !_.isFunction(_.get(driver, 'MongoClient.connect')) ? driver : null;

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

      // remove the size options
    };delete _this._options.collectionSizeBytes;
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
        if (this.db) return done

        // otherwise connect it
        ();return this._driver.MongoClient.connect(this._url, this._options, function (error, db) {
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
          if (error) return done(error

          // if the collection exists, get it and return done
          );if (collections.length) {
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

var RedisChangeFeed = function (_ChangeFeed) {
  inherits(RedisChangeFeed, _ChangeFeed);

  function RedisChangeFeed(leaderfeed, collection) {
    classCallCheck(this, RedisChangeFeed);
    return possibleConstructorReturn(this, (RedisChangeFeed.__proto__ || Object.getPrototypeOf(RedisChangeFeed)).call(this, leaderfeed, collection));
  }

  createClass(RedisChangeFeed, [{
    key: 'changes',
    value: function changes(done) {
      return done();
    }
  }]);
  return RedisChangeFeed;
}(ChangeFeed$1);

var DEFAULT_HEARTBEAT_INTERVAL$1 = 1000;

var RedisLeaderFeed = function (_LeaderFeed) {
  inherits(RedisLeaderFeed, _LeaderFeed);

  function RedisLeaderFeed(driver, options) {
    classCallCheck(this, RedisLeaderFeed);

    if (!driver) throw new Error('no driver specified');
    if (!_.isObject(options) || _.isEmpty(options)) throw new Error('no options specefied');

    var _this = possibleConstructorReturn(this, (RedisLeaderFeed.__proto__ || Object.getPrototypeOf(RedisLeaderFeed)).call(this, options, DEFAULT_HEARTBEAT_INTERVAL$1, RedisChangeFeed));

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

var RethinkChangeFeed = function (_ChangeFeed) {
  inherits(RethinkChangeFeed, _ChangeFeed);

  function RethinkChangeFeed(leaderfeed, collection) {
    classCallCheck(this, RethinkChangeFeed);
    return possibleConstructorReturn(this, (RethinkChangeFeed.__proto__ || Object.getPrototypeOf(RethinkChangeFeed)).call(this, leaderfeed, collection));
  }

  createClass(RethinkChangeFeed, [{
    key: 'changes',
    value: function changes(done) {
      return done();
    }
  }]);
  return RethinkChangeFeed;
}(ChangeFeed$1);

var debug$3 = Debug('feed:rethinkdb');
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

    var _this = possibleConstructorReturn(this, (RethinkLeaderFeed.__proto__ || Object.getPrototypeOf(RethinkLeaderFeed)).call(this, options, DEFAULT_HEARTBEAT_INTERVAL$2, RethinkChangeFeed));

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

      debug$3('heartbeat update');

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
          debug$3('changefeed started');

          cursor.each(function (error, change) {
            if (error) {
              debug$3('changefeed error: %O', error);
              return _this5.emit(SUB_ERROR, error);
            }

            var data = _.get(change, 'new_val');
            var id = _.get(data, ID$1);
            var value = _.get(data, VALUE

            // emit the appropriate event
            );return id === LEADER ? _this5.emit(HEARTBEAT, value) : _this5.emit(CHANGE, change);
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
