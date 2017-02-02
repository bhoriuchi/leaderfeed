import babel from 'rollup-plugin-babel'

export default {
  entry: 'src/index.js',
  format: 'cjs',
  plugins: [ babel() ],
  external: ['lodash', 'debug', 'events', 'hat'],
  dest: 'index.js'
}