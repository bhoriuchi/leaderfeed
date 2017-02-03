import babel from 'rollup-plugin-babel'

export default {
  entry: 'src/LeaderFeed.js',
  format: 'cjs',
  plugins: [ babel() ],
  external: ['lodash', 'debug', 'events', 'hat'],
  dest: 'base.js'
}