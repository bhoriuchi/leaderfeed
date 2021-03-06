# leaderfeed
Leader election for subscription/changefeed databases

## Backends supported

* [`MongoDB`](#mongodb)
* [`Redis`](#redis) - developing
* [`RethinkDB`](#rethinkdb)

## About

`leaderfeed` is a simple leader election library for use with databases that support feeds (aka streams, subscriptions, changefeeds, live queries, streaming queries, etc.)

The election algorithm borrows from [`raft`](https://raft.github.io/) but has no concept of terms or log distribution.

### Example (ES6)

```js
import rethinkdbdash from 'rethinkdbdash'
import leaderfeed from 'leaderfeed'

const table = 'mytable'

// initialize
const feedA = new leaderfeed.RethinkDB(rethinkdbdash)
const feedB = new leaderfeed.RethinkDB(rethinkdbdash)

// add events
feedA.on('new state', state => console.log('feedA state changed to ', state))
feedB.on('new state', state => console.log('feedB state changed to ', state))

// start nodes
feedA.start({ table }, (error, feed) => {
  if (error) return console.error(error)
  
  let { r, db, table } = feed
  // issue commands
  r.db(db).table(table).run()
})

feedB.start({ table }, (error, feed) => {
  if (error) return console.error(error)
    
  // check if leader
  console.log('feedB is leader: ', feed.isLeader)
})

```

### API

#### RethinkDB

RethinkDB specific API

##### LeaderFeed#RethinkDB(`driver:Driver` [,`db:String`] [,`opts:Object`]) => `RethinkLeaderFeed`

Initializes a new `RethinkLeaderFeed`

* `driver` - rethinkdb driver
* [`db="test"`] - database name
* [`opts`] - extended rethinkdb connection options
  * [`createIfMissing=true`] - create the db and table if missing
  * [`heartbeatIntervalMs=1000`] - time between heartbeat updates
  * [`electionTimeoutMinMs=2*heartbeatIntervalMs`] - minimum time before self electing, should be at least `heartbeatIntervalMs * 2`
  * [`electionTimeoutMaxMs=2*electionTimeoutMaxMs`] - maximum time before self electing, should be at least `electionTimeoutMaxMs * 2`

##### RethinkLeaderFeed#start(`opts:Object` [,`cb:Function`]) => `Promise<RethinkLeaderFeed>`

Starts the leaderfeed

* `opts` - options hash
  * `table` - table name
  * [`connection`] - rethinkdb connection if already connected
* [`cb`] - callback, returns error as first argument or leader feed as second

##### RethinkLeaderFeed#stop([`cb:Function`]) => `Promise`

Stops the leaderfeed

##### RethinkLeaderFeed#elect([, `id:String`] [,`cb:Function`]) => `Promise`

Elects an id specified or self if no id specified

##### RethinkLeaderFeed#status => `StatusEnum`

"started" | "starting" | "stopping" | "stopped"

##### RethinkLeaderFeed#r => `Driver`

RethinkDB driver

##### RethinkLeaderFeed#connection => `Object`

RethinkDB connection (undefined if driver is `rethinkdbdash`)

##### RethinkLeaderFeed#db => `Db`

RethinkDB database selection

##### RethinkLeaderFeed#id => `String`

##### RethinkLeaderFeed#isLeader => `Boolean`

##### RethinkLeaderFeed#table => `Table`

RethinkDB table selection

---

#### Redis

Redis specific API. Please note that leaderfeed will not emit `changes` events for redis and that the pub/sub channel specified in start should be reserved for leaderfeed

##### LeaderFeed#Redis(`redis:Redis` `opts:Object`) => `RedisLeaderFeed`

Initializes a new `RedisLeaderFeed`

* `redis` - redis client library
* `opts` - [`redis client options`](https://github.com/NodeRedis/node_redis#options-object-properties)


##### RedisLeaderFeed#start(`opts:Object` [,`cb:Function`]) => `Promise<RedisLeaderFeed>`

Starts the leaderfeed

* `opts` - options hash
  * `channel` - channel name
* [`cb`] - callback, returns error as first argument or leader feed as second


##### RedisLeaderFeed#stop([`cb:Function`]) => `Promise`

Stops the leaderfeed

##### RedisLeaderFeed#elect([, `id:String`] [,`cb:Function`]) => `Promise`

Elects an id specified or self if no id specified

##### RedisLeaderFeed#status => `StatusEnum`

"started" | "starting" | "stopping" | "stopped"

##### RedisLeaderFeed#pub => `Object`

Redis publish client

##### RedisLeaderFeed#sub => `Object`

Redis subscribe client

##### RedisLeaderFeed#id => `String`

##### RedisLeaderFeed#isLeader => `Boolean`

---

#### MongoDB

MongoDB specifc API. Please note that MongoDB uses capped collections and tailable cursors for streaming queries. Because of the limitations on capped collections it is advised that the collection used for leaderfeed is dedicated and set up by leaderfeed.

##### LeaderFeed#MongoDB(`driver:MongoDB` `url:String` [,`opts:Object`]) => `MongoLeaderFeed`

Initializes a new `MongoLeaderFeed`

* `driver` - mongodb driver
* `url` - database name
* [`opts`] - extended mongodb connection options
  * [`createIfMissing=true`] - create the db and table if missing
  * [`heartbeatIntervalMs=1000`] - time between heartbeat updates
  * [`electionTimeoutMinMs=2*heartbeatIntervalMs`] - minimum time before self electing, should be at least `heartbeatIntervalMs * 2`
  * [`electionTimeoutMaxMs=2*electionTimeoutMaxMs`] - maximum time before self electing, should be at least `electionTimeoutMaxMs * 2`
  * [`collectionSizeBytes=100000`] - size in bytes to use when creating the capped collection
  * [`collectionMaxDocs=20`] - maximum documents allowed in the capped collection before overwriting begins

##### LeaderFeed#MongoDB(`db:Db` [,`opts:Object`]) => `MongoLeaderFeed`

Initializes a new `MongoLeaderFeed`

* `db` - mongodb database
* [`opts`] - options hash
  * [`createIfMissing=true`] - create the db and table if missing
  * [`heartbeatIntervalMs=1000`] - time between heartbeat updates
  * [`electionTimeoutMinMs=2*heartbeatIntervalMs`] - minimum time before self electing, should be at least `heartbeatIntervalMs * 2`
  * [`electionTimeoutMaxMs=2*electionTimeoutMaxMs`] - maximum time before self electing, should be at least `electionTimeoutMaxMs * 2`
  * [`collectionSizeBytes=100000`] - size in bytes to use when creating the capped collection
  * [`collectionMaxDocs=20`] - maximum documents allowed in the capped collection before overwriting begins

##### MongoLeaderFeed#start(`opts:Object` [,`cb:Function`]) => `Promise<MongoLeaderFeed>`

Starts the leaderfeed

* `opts` - options hash
  * `collection` - collection name
* [`cb`] - callback, returns error as first argument or leader feed as second

##### MongoLeaderFeed#stop([`cb:Function`]) => `Promise`

Stops the leaderfeed

##### MongoLeaderFeed#elect([, `id:String`] [,`cb:Function`]) => `Promise`

Elects an id specified or self if no id specified

##### MongoLeaderFeed#status => `StatusEnum`

"started" | "starting" | "stopping" | "stopped"

##### MongoLeaderFeed#db => `Object`

MongoDB database connecion

##### MongoLeaderFeed#id => `String`

##### MongoLeaderFeed#isLeader => `Boolean`

##### MongoLeaderFeed#collection => `Collection`

---

### Events

#### `change` => `change`

Fired when data other than the leader metadata has been modified. Change object is specific to the backend

#### `new state` => `state`

Fired when the leaderfeed's state changes. State is `follower` or `leader`

#### `new leader` => `leaderId`

Fired when a new leader is elected

#### `subscribe started`

Fired when the subscribe method is successful signaling that the subscription has started

#### `subscribe error` => `error`

Fired when there is an error after the subscription has started. Signals the node to change to `follower` state

#### `heartbeat error` => `error`

Fired when there is an error after trying to send a heartbeat update. Signals the node to change to `follower` state


---

### Extending

For convenience, a base class is provided in `leaderfeed/base` that can be extended to create a custom leaderfeed library. Required methods are

**`_create(done:Function)`**

Should create the storage/db/table/collection and callback done with an error or no arguments if successful

**`_heartbeat(done:Function)`**

Should commit a heartbeat to the store/table/collection which should include its id and a timestamp generated by the store/table/collection then callback done with error or no arguments if successfull

**`_subscribe(done:Function)`**

Should start a subscription/changefeed/stream of changes and emit the following events and callback done with error or no arguments if successful

* `heartbeat` => `leaderId` - when the heartbeat record is updated emit the leader id from the hearbeat
* `change` => `change` - for non heartbeat changes, emit the change object
* `subscribe error` => `error` - if an error is encountered in the subscription/changefeed/stream emit this event with the error to signal the node to become a follower

**`_unsubscribe(done:Function)`**

Should stop the subscription and callback done with error or no arguments if successful

**`_start(opts:Object, done:Function)`**

Should set up a connection to the backend and callback done with an error or no arguments if successful. The options object should contain information specific to making requests to the backend (i.e. the table name and/or connection object)

**`_elect(id:String, done:Function)`**

Should set the leader to the id value and callback done with an error or no arguments if successful

---

### Summary of algorithm

leaderfeed leverages changefeeds/subscriptions to determine node health. Unlike a leader in [`raft`](https://raft.github.io/), the leaderfeed leader does not distribute logs or calculate consensus. A leaderfeed cluster simply assigns one node the role of leader and because of this a lot of complexity is removed. Nodes can dynamically join/leave a leaderfeed cluster as long as they have access to the same changefeed/subscription

#### Metadata

* A single record of metadata is stored on a medium that supports change feeds
  * The metadata contains the current leader's id and last check in timestamp

#### Heartbeats

* Heartbeats are committed to the leader metadata record on a set interval by the leader
* Heartbeats consist of the leader's id and the current timestamp
  * the timestamp should be generated by the store (i.e. rethinkdb r.now())

#### Leaderfeeds

* A leaderfeed can be in one of 2 states `follower` or `leader`
* All leaderfeeds start with the state `follower`
* Each leaderfeed uses a randomized election timeout
  * If the election timeout is reached and no updates to the leader metadata have taken place the leaderfeed elects itself and begins sending heartbeats to the store
  * Election timeouts are randomized to prevent multiple leaderfeeds from self electing (see [`raft`](https://raft.github.io/) paper)
* One record is used to store metadata on who the current leader is
* All leaderfeeds set up a changefeed/subscription to the data source on start up
  * If there is an error in the changefeed/subscription, the leaderfeed converts to `follower`
  * If there is a change to the leader metadata (a heartbeat is sent)
    * Election timeout is reset
    * If the leaderfeed has it state set to `leader` and the changefeed metadata identifies another id as the leader, the leaderfeed transitions to `follower` state