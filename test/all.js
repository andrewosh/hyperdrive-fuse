const fs = require('fs')
const test = require('tape')
const hyperdrive = require('hyperdrive')
const fuse = require('fuse-native')
const ram = require('random-access-memory')
const rimraf = require('rimraf')
const { mount, unmount, getHandlers } = require('..')

test('can read/write a small file', async t => {
  const drive = hyperdrive(ram)
  const { destroy } = await mount(drive, './mnt')
  process.once('SIGINT', () => cleanup(destroy))

  const NUM_SLICES = 100
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(destroy)
  t.end()
})

test('can read/write a large file', async t => {
  const drive = hyperdrive(ram)
  const { destroy } = await mount(drive, './mnt')
  process.once('SIGINT', () => cleanup(destroy))

  const NUM_SLICES = 10000
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(destroy)
  t.end()
})

test('can read/write a huge file', async t => {
  const drive = hyperdrive(ram)
  const { destroy } = await mount(drive, './mnt')
  process.once('SIGINT', () => cleanup(destroy))

  const NUM_SLICES = 100000
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  try {
    const content = await writeData(NUM_SLICES, SLICE_SIZE)
    await readData(content, NUM_SLICES, SLICE_SIZE, READ_SIZE)
    t.pass('all slices matched')
  } catch (err) {
    t.fail(err)
  }

  await cleanup(destroy)
  t.end()
})

test('a hanging get will be aborted after a timeout', async t => {
  const drive = hyperdrive(ram)
  const handlers = getHandlers(drive, './mnt')

  // Create an artificial hang
  handlers.chmod = (path, uid, gid, cb) => {}

  const { destroy } = await mount(drive, handlers, './mnt')
  process.once('SIGINT', () => cleanup(destroy))

  await new Promise(resolve => {
    fs.writeFile('./mnt/hello', 'goodbye', err => {
      t.error(err, 'no error')
      fs.chmod('./mnt/hello', 777, err => {
        t.true(err)
        t.same(err.errno, fuse.EIO)
        return resolve()
      })
    })
  })

  await cleanup(destroy)
  t.end()
})

async function writeData (numSlices, sliceSize) {
  const content = Buffer.alloc(sliceSize * numSlices).fill('0123456789abcdefghijklmnopqrstuvwxyz')
  let slices = new Array(numSlices).fill(0).map((_, i) => content.slice(sliceSize * i, sliceSize * (i + 1)))
  let fd = await open('./mnt/hello', 'w+')
  for (let slice of slices) {
    await write(fd, slice, 0)
  }
  await close(fd)
  return content
}

async function readData (content, numSlices, sliceSize, readSize) {
  let fd = await open('./mnt/hello', 'r')
  let numReads = 0
  do {
    const pos = numReads * readSize
    const buf = Buffer.alloc(readSize)
    let bytesRead = await read(fd, buf, 0, readSize, pos)
    if (!buf.slice(0, bytesRead).equals(content.slice(pos, pos + readSize))) {
      throw new Error(`Slices do not match at position: ${pos}`)
    }
  } while (++numReads * readSize < numSlices * sliceSize)
  await close(fd)
}

function cleanup (destroy) {
  return new Promise((resolve, reject) => {
    destroy(err => {
      if (err) return reject(err)
      rimraf('./mnt', err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  })
}

function read (fd, buf, offset, len, pos) {
  return new Promise((resolve, reject) => {
    fs.read(fd, buf, offset, len, pos, (err, bytesRead) => {
      if (err) return reject(err)
      return resolve(bytesRead)
    })
  })
}

function write (fd, buf, offset, len) {
  return new Promise((resolve, reject) => {
    fs.write(fd, buf, offset, len, (err, bytesWritten) => {
      if (err) return reject(err)
      return resolve(bytesWritten)
    })
  })
}

function open (f, flags) {
  return new Promise((resolve, reject) => {
    fs.open(f, flags, (err, fd) => {
      if (err) return reject(err)
      return resolve(fd)
    })
  })
}

function close (fd) {
  return new Promise((resolve, reject) => {
    fs.close(fd, err => {
      if (err) return reject(err)
      return resolve(err)
    })
  })
}
