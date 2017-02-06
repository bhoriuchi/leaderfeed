import EventEmitter from 'events'

export default class ChangeFeed extends EventEmitter {
  constructor (leaderfeed, collection) {
    super()
    this._leaderfeed = leaderfeed
    this._collection = collection
  }

  changes (done) {
    done()
    return Promise.resolve()
  }
}