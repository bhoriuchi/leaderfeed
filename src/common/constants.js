// leader record schema properties
export const VALUE = 'value'
export const TIMESTAMP = 'timestamp'

// raft states
export const LEADER = 'leader'
export const FOLLOWER = 'follower'

// events
export const HEARTBEAT = 'heartbeat'
export const CHANGE = 'change'
export const NEW_STATE = 'new state'
export const NEW_LEADER = 'new leader'

export default {
  VALUE,
  TIMESTAMP,
  LEADER,
  FOLLOWER,
  HEARTBEAT,
  CHANGE,
  NEW_LEADER,
  NEW_STATE
}