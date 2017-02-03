import mongodb from 'mongodb'
import hat from 'hat'

let url = 'mongodb://localhost:27017/test'
let name = 'leaderfeed'
let id = hat()
const DEFAULT_COLLECTION_SIZE = 100000

function pad (str, len = 32) {
  return String(new Array(len + 1).join(' ') + str).slice(-1 * Math.abs(len))
}

function create (db, done) {
  return db.createCollection(name, done)
}

function update (collection, done) {
  return collection.findOneAndUpdate({
    _id: 'leader'
  }, {
    value: id,
    timestamp: new Date()
  }, {
    upsert: true
  }, (error, result) => {
    if (error) return done(error)
    console.log(result)
    return done()
  })
}

mongodb.MongoClient.connect(url, (error, db) => {
  let done = (error) => {
    if (error) console.error({ error })
    return db.close()
  }

  if (error) return done(error)
  console.log('connected')



  return db.listCollections({ name })
    .toArray((error, collections) => {
      if (error) return done(error)
      console.log(collections)

      if (collections.length) {
        return db.collection(name, (error, collection) => {
          if (error) return done(error)
          return update(collection, done)
        })
      }

      return create(db, (error, collection) => {
        if (error) return done(error)
        return update(collection, done)
      })
    })
})
