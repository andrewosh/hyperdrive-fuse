const fs = require('fs')
const test = require('tape')
const rimraf = require('rimraf')
const { mount, unmount } = require('..')

test('can read/write a huge file', async t => {
  const { drive } = await mount(null, './mnt')

  // Copied from hyperdrive fd tests

  const NUM_SLICES = 100
  const SLICE_SIZE = 4096
  const READ_SIZE = Math.floor(4096 * 2.76)

  const content = Buffer.alloc(SLICE_SIZE * NUM_SLICES).fill('0123456789abcdefghijklmnopqrstuvwxyz')
  let slices = new Array(NUM_SLICES).fill(0).map((_, i) => content.slice(SLICE_SIZE * i, SLICE_SIZE * (i+1)))

  let fd = await open('./mnt/hello', 'w+')
  for (let slice of slices) {
    await write(fd, slice, 0)
  }
  await close(fd)

  fd = await open('./mnt/hello', 'r')
  let numReads = 0

  try { 
    do {
      const pos = numReads * READ_SIZE
      const buf = Buffer.alloc(READ_SIZE)
      let bytesRead = await read(fd, buf, 0, READ_SIZE, pos)
      if (!buf.slice(0, bytesRead).equals(content.slice(pos, pos + READ_SIZE))) {
        throw new Error(`Slices do not match at position: ${pos}`)
      }
    } while (++numReads * READ_SIZE < NUM_SLICES * SLICE_SIZE)
    await close(fd)
  } catch (err) {
    t.fail(err)
    await close(fd)
    await cleanup()
  }

  await cleanup()
  t.pass('all slices matched')
  t.end()
})


function cleanup () {
  return new Promise((resolve, reject) => {
    unmount('./mnt', err => {
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
    fs.write(fd, buf, offset, len,(err, bytesWritten) => {
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



