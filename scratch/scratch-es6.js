import rethinkdbdash from 'rethinkdbdash'
import leaderfeed from '../index'

const table = 'leaderfeed'

// initialize
const nodeA = new leaderfeed.RethinkDB(rethinkdbdash)
const nodeB = new leaderfeed.RethinkDB(rethinkdbdash)

// add events
nodeA.on('new state', state => console.log('nodeA state changed to ', state))
nodeB.on('new state', state => console.log('nodeB state changed to ', state))

// start nodes
nodeA.start(table, (error, feed) => {
  if (error) return console.log(error)
  console.log('nodeA started')
})

nodeB.start(table, (error, feed) => {
  if (error) return console.log(error)
  // check if leader
  console.log('nodeB is leader: ', feed.isLeader)
})
