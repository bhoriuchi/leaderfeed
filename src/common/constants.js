// leader record schema properties
export const VALUE = 'value'
export const TIMESTAMP = 'timestamp'
export const TYPE = 'type'

// raft states
export const LEADER = 'leader'
export const FOLLOWER = 'follower'

// leaderfeed status
export const STOPPED = 'stopped'
export const STOPPING = 'stopping'
export const STARTING = 'starting'
export const STARTED = 'started'

// events
export const HEARTBEAT = 'heartbeat'
export const HEARTBEAT_ERROR = 'heartbeat error'
export const CHANGE = 'change'
export const NEW_STATE = 'new state'
export const NEW_LEADER = 'new leader'
export const SUB_ERROR = 'subscribe error'
export const SUB_STARTED = 'subscribe started'

export default {
  VALUE,
  TIMESTAMP,
  TYPE,
  LEADER,
  FOLLOWER,
  STOPPED,
  STOPPING,
  STARTING,
  STARTED,
  HEARTBEAT,
  HEARTBEAT_ERROR,
  CHANGE,
  NEW_LEADER,
  NEW_STATE,
  SUB_ERROR,
  SUB_STARTED
}