import mongodb from 'mongodb'
import _ from 'lodash'
let url = 'mongodb://localhost:27017/test'
let name = 'leaderfeed'

mongodb.MongoClient.connect(url, (error, db) => {
  if (error) return console.log({ error })

  console.log(db.connect)
  process.exit()

  db.collection(name, (error, collection) => {
    if (error) return console.log({ error })

    let cursor = collection.find({}, { tailable: true, awaitdata: true })

    let stream = cursor.stream()

    stream.on('data', (data) => {
      console.log({ data })
    })
    stream.on('error', (error) => {
      console.log({ error })
    })
  })
})