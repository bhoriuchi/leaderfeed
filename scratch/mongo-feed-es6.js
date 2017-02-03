import MongoOplog from 'mongo-oplog'

let url = 'mongodb://localhost:27017/local'
let name = 'leaderfeed'

const oplog = MongoOplog(url)

oplog.tail((err) => {
  if (err) console.log('err', err)
})

oplog.on('op', op => {
  console.log({ op })
})

oplog.on('insert', insert => {
  console.log({ insert })
})

oplog.on('update', update => {
  console.log({ update })
})

oplog.on('delete', del => {
  console.log({ del })
})

oplog.on('error', error => {
  console.log({ error })
})