import mongodb from 'mongodb'

let url = 'mongodb://localhost:27017/test'
let name = 'leaderfeed'

function pad (str, len = 32) {
  return String(new Array(len + 1).join(' ') + str).slice(-1 * Math.abs(len))
}

mongodb.MongoClient.connect(url, (error, db) => {
  if (error) return console.log({ error })

  db.collection(name, (error, collection) => {
    if (error) return console.log({ error })

    collection.insert({ _id: pad('', 24) }, {
      timestamp: Date.now()
    }, { upsert: true }, (error, result) => {
      if (error) return console.log({ error })
      console.log({ result })
      db.close()
    })
  })
})