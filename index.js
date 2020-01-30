const p = require('path')
const fs = require('fs')
const os = require('os')

const datEncoding = require('dat-encoding')
const mkdirp = require('mkdirp')
const fsConstants = require('filesystem-constants')
const Fuse = require('fuse-native')
const { translate, linux } = fsConstants

const debug = require('debug')('hyperdrive-fuse')

const platform = os.platform()

class HyperdriveFuse {
  constructor (drive, mnt, opts = {}) {
    this.drive = drive
    this.mnt = p.resolve(mnt)
    this.opts = opts

    // Set in mount
    this.fuse = null
  }

  getBaseHandlers () {
    const self = this
    const handlers = {}
    const log = this.opts.log || debug

    handlers.getattr = function (path, cb) {
      log('getattr', path)
      self.drive.lstat(path, (err, stat) => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        stat.uid = process.getuid()
        stat.gid = process.getgid()
        return cb(0, stat)
      })
    }

    handlers.readdir = function (path, cb) {
      log('readdir', path)
      // TODO: pass in stat objects once readdirplus is enabled (FUSE 3.x)
      self.drive.readdir(path, (err, files) => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        return cb(0, files)
      })
    }

    handlers.open = function (path, flags, cb) {
      log('open', path, flags)

      if (platform !== 'linux') {
        flags = translate(fsConstants[platform], linux, flags)
      }

      self.drive.open(path, flags, (err, fd) => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        return cb(0, fd)
      })
    }

    handlers.release = function (path, handle, cb) {
      log('release', path, handle)
      self.drive.close(handle, err => {
        if (err) return cb(-err.errno || Fuse.EBADF)
        return cb(0)
      })
    }

    handlers.releasedir = function (path, handle, cb) {
      // TODO: What to do here?
      return cb(0)
    }

    handlers.read = function (path, handle, buf, len, offset, cb) {
      log('read', path, handle, len, offset)
      self.drive.read(handle, buf, 0, len, offset, (err, bytesRead) => {
        if (err) return cb(-err.errno || Fuse.EBADF)
        return cb(bytesRead)
      })
    }

    handlers.write = function (path, handle, buf, len, offset, cb) {
      log('write', path, handle, len, offset)

      // TODO: Duplicating the input buffer is a temporary patch for a race condition.
      // (Fuse overwrites the input before the data is flushed to storage in hypercore.)
      buf = Buffer.from(buf)

      self.drive.write(handle, buf, 0, len, offset, (err, bytesWritten) => {
        if (err) return cb(-err.errno || Fuse.EBADF)
        return cb(bytesWritten)
      })
    }

    handlers.truncate = function (path, size, cb) {
      log('truncate', path, size)
      self.drive.truncate(path, size, err => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.ftruncate = function (path, fd, size, cb) {
      log('ftruncate', path, fd, size)
      self.drive.ftruncate(fd, size, err => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.unlink = function (path, cb) {
      log('unlink', path)
      self.drive.unlink(path, err => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        return cb(0)
      })
    }

    handlers.mkdir = function (path, mode, cb) {
      log('mkdir', path)
      self.drive.mkdir(path, { mode, uid: process.getuid(), gid: process.getgid() }, err => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.rmdir = function (path, cb) {
      log('rmdir', path)
      self.drive.rmdir(path, err => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        return cb(0)
      })
    }

    handlers.create = function (path, mode, cb) {
      log('create', path, mode)
      self.drive.create(path, { mode, uid: process.getuid(), gid: process.getgid() }, err => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        self.drive.open(path, 'w', (err, fd) => {
          if (err) return cb(-err.errno || Fuse.ENOENT)
          return cb(0, fd)
        })
      })
    }

    handlers.chown = function (path, uid, gid, cb) {
      log('chown', path, uid, gid)
      self.drive._update(path, { uid, gid }, err => {
        if (err) return cb(Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.chmod = function (path, mode, cb) {
      log('chmod', path, mode)
      self.drive._update(path, { mode }, err => {
        if (err) return cb(Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.utimens = function (path, atime, mtime, cb) {
      log('utimens', path, atime, mtime)
      self.drive._update(path, { atime, mtime }, err => {
        if (err) return cb(Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.symlink = function (target, linkname, cb) {
      log('symlink', target, linkname)
      self.drive.symlink(target, linkname, err => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.readlink = function (path, cb) {
      log('readlink', path)
      self.drive.lstat(path, (err, st) => {
        if (err) return cb(-err.errno || Fuse.ENOENT)
        // Always translate absolute symlinks to be relative to the mount root.
        const resolved = p.isAbsolute(st.linkname) ? p.join(self.mnt, st.linkname) : st.linkname
        return cb(0, resolved)
      })
    }

    handlers.statfs = function (path, cb) {
      cb(0, {
        bsize: 1000000,
        frsize: 1000000,
        blocks: 1000000,
        bfree: 1000000,
        bavail: 1000000,
        files: 1000000,
        ffree: 1000000,
        favail: 1000000,
        fsid: 1000000,
        flag: 1000000,
        namemax: 1000000
      })
    }

    handlers.setxattr = function (path, name, buffer, position, flags, cb) {
      log('setxattr', path, name)
      if (platform === 'darwin' && path.startsWith('com.apple')) return cb(0)
      self.drive.setMetadata(path, name, buffer, err => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        return cb(0)
      })
    }

    handlers.getxattr = function (path, name, position, cb) {
      log('getxattr', path, name)
      self.drive.stat(path, (err, stat) => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        if (!stat.metadata) return cb(0, null)
        return cb(0, stat.metadata[name])
      })
    }

    handlers.listxattr = function (path, cb) {
      log('listxattr', path)
      self.drive.stat(path, (err, stat) => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        if (!stat.metadata) return cb(0, [])
        return cb(0, Object.keys(stat.metadata))
      })
    }

    handlers.removexattr = function (path, name, cb) {
      log('removexattr', path, name)
      self.drive.removeMetadata(path, name, err => {
        if (err) return cb(-err.errno || Fuse.EPERM)
        return cb(0)
      })
    }

    return handlers
  }

  async mount (handlers) {
    if (this.fuse) throw new Error('Cannot remount the same HyperdriveFuse instance.')

    const self = this
    handlers = handlers ? { ...handlers } : this.getBaseHandlers()

    const mountOpts = {
      uid: process.getuid(),
      gid: process.getgid(),
      displayFolder: true,
      autoCache: true,
      force: true,
      mkdir: true
    }
    mountOpts.debug = this.opts.debug || debug.enabled

    const fuse = new Fuse(this.mnt, handlers, mountOpts)

    return new Promise((resolve, reject) => {
      return self.drive.ready(err => {
        if (err) return reject(err)
        return fuse.mount(err => {
          if (err) return reject(err)
          const keyString = datEncoding.encode(self.drive.key)
          self.fuse = fuse
          return resolve({
            handlers,
            mnt: self.mnt,
            key: keyString,
            drive: self.drive
          })
        })
      })
    })
  }

  unmount () {
    if (!this.fuse) return null
    return new Promise((resolve, reject) => {
      return this.fuse.unmount(err => {
        if (err) return reject(err)
        return resolve(err)
      })
    })
  }
}

module.exports = {
  HyperdriveFuse,
  configure: Fuse.configure,
  unconfigure: Fuse.unconfigure,
  isConfigured: Fuse.isConfigured,
  beforeMount: Fuse.beforeMount,
  beforeUnmount: Fuse.beforeUnmount
}
