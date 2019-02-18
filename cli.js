#!/usr/bin/env node

const datEncoding = require('dat-encoding')
const minimist = require('minimist')
const chalk = require('chalk')

const { mount } = require('.')

const args = minimist(process.argv.slice(2), {
  string: ['key', 'mnt', 'storage'],
  boolean: ['debug'],
  default: {
    debug: false
  }
})
const key = args.key ?  datEncoding.decode(args.key) : null

async function run () {
  try {
    const { key: mountedKey } = await mount(key, args.mnt, { dir: args.dir, debug: args.debug })
    console.log(chalk.green(`Mounted key ${mountedKey} at ${args.mnt}`))
  } catch (err) {
    throw err
    console.error(chalk.red(`Could not mount the hyperdrive: ${err}`))
  }
}

run()
