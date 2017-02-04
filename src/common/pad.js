export default function pad (str, len = 32) {
  return String(new Array(len + 1).join(' ') + str).slice(-1 * Math.abs(len))
}