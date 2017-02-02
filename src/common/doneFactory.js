import _ from 'lodash'

/**
 * create a new done handler
 * @private
 * @param callback
 * @param resolve
 * @param reject
 * @returns {done}
 */
export default function doneFactory (callback, resolve, reject) {
  callback = _.isFunction(callback) ? callback : () => false
  resolve = _.isFunction(resolve) ? resolve : () => false
  reject = _.isFunction(reject) ? reject : () => false

  return function done (error, success) {
    if (error) {
      callback(error)
      return reject(success)
    }
    callback(null, success)
    return resolve(success)
  }
}