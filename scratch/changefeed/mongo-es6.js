import leaderfeed from '../../src/index'
import mongodb from 'mongodb'

new leaderfeed.MongoDB(mongodb, 'mongodb://localhost/test')
  .changes('list', (error, collection) => {
    if (error) {
      console.log({ error })
      process.exit()
    }

    collection.on('change', (change) => {
      console.log(change)
    })

    setTimeout(() => {
      process.exit()
    }, 5000)

    setTimeout(() => {
      collection.insertOne({ name: 'test' }, (error, result) => {
        if (error) return console.log('insert error', { error })
        console.log({ result: result.ops })
      })
    }, 1000)
  })