#!/n/site/inst/Linux-i686/sys/bin/python2.5

import datetime
import dircache
import gzip
import hotshot
import hotshot.stats
import os
import posix1e
import re
import shutil
import stat
import sys
import time

from processing import Process
from Queue import Queue

TOMBSTONE = '.TOMBSTONE'
ARCHIVE_EXT = 'REPgz'
COMPRESSED_SUFFIX = ['.gz', '.bz2', '.tgz', '.Z']
NUM_THREADS = 5
MIN_SIZE_KB = 64

ProcessList = []
Options = ''

##############################################################################
# Utility functions
##############################################################################
def err(str):
    print >> sys.stderr, "Error:", str
    sys.exit(1)

def warn(str):
    print >> sys.stderr, "Warning:", str

def info(str):
    print >> sys.stderr, "Info:", str

def partition_list(f, lst):
    """Given function F and list F, return tuple (matched, nonmatched),
       where matched is a list of all elements E for which F(E) is true, and
       nonmatched the remainder.
       
       taken from 'goopy.functional'
    """
    matched = []
    nonmatched = []
    for e in lst:
        if f(e):
            matched.append(e)
        else:
            nonmatched.append(e)
    return matched, nonmatched

def memoize(f, max=1000):
    d = {}
    def g(*args):
        if not d.has_key(args):
            if len(d) > max:
                d.popitem() # Remove arbitrary (key,value) to make room
            d[args] = f(*args)
        return d[args]
    return g

def is_compressed_file(f):
    global COMPRESSED_SUFFIX 

    for e in COMPRESSED_SUFFIX:
        if f.endswith(e):
            return True
    return False

def listdir(path):
    ''' Return tuple of (dirs, files) for the given path '''
    dirs = []
    files = []
    try:
        for item in dircache.listdir(path):
            p = os.path.join(path, item)
            # Put real directories in the dirs list - symlinks are treated as files.
            if os.path.isdir(p) and not islink(p):
                dirs.append(item)
            else:
                files.append(item)

        return dirs, files
    except:
        return (), ()

def _islink(p):
    return os.path.islink(p)
islink = memoize(_islink)

def _lexists(p):
    return os.path.lexists(p)
lexists = memoize(_lexists)

def _lstat(p):
    return os.lstat(p)
lstat = memoize(_lstat)

def copystat(src, dst):
    # Preserve stat info (if not symlink)
    shutil.copystat(src, dst)

    # The above doesn't save uid, gid - set those.
    s = lstat(src)
    os.chown(dst, s[4], s[5])

    # Copy ACLs, if supported and present
    try:
        s_acl = posix1e.ACL(file=src)
        s_acl.applyto(dst)
    # Throws IOError if not supported.
    except IOError, e:
        pass
    except Exception, e:
        info("ACL copy failed:", e)


##############################################################################
# Process class for cloning files
##############################################################################
class CloneFileProcess(Process):
    def __init__(self, src, dst, gz=False, level=6):
        Process.__init__(self)
        self.src = src
        self.dst = dst
        self.gz = gz
        self.level = level

        info("Cloning '%s' to '%s' (gz = %d)" % (src, dst, gz))

    def _run(self):
        ''' Copy a file, preserving all stat information '''
        # Otherwise, it's a normal file
        if self.gz:
            # Compress copied file
            fdst = gzip.open(self.dst, 'wb', self.level)
            fsrc = open(self.src, 'r')
            shutil.copyfileobj(fsrc, fdst)
            fsrc.close()
            fdst.close()
        else:
            # Copy file,
            shutil.copyfile(self.src, self.dst)

        copystat(self.src, self.dst)
    
    def run(self):
        try:
            self._run()
        # On ANY error, remove the possibly incomplete destination file and re-raise
        except Exception, e:
            print "Exception in run(%s, %s):" % (self.src, self.dst), e
            os.unlink(self.dst)
            raise

def wait_process(how_many=0):
    ''' Block until 'how_many' slots are open in the ProcessList list. 
        If 'how_many' <= 0, then wait for all processes to exit. '''
    global Options, ProcessList

    if how_many > 0:
        exit_count = max(0, Options.num_threads - how_many)
    else:
        exit_count = 0
    
    # Loop until correct number of slot sopen 
    while len(ProcessList) > 0 and len(ProcessList) > exit_count:
        # Partition into finished and not finished processes
        ProcessList, finished = partition_list(lambda t: t.isAlive(), ProcessList)

        # Join any finished threads
        for f in finished:
            f.join()

        # If none finished in this cycle, then sleep for a bit
        if not finished:
            time.sleep(0.01)

def dst_findfile(f):
    global ARCHIVE_EXT 
    # If file doesn't exist, then search for it's archived name.
    # Note: using 'lexists' so that broken symlinks are shown to exit.
    try:
        f_dir, f_name = os.path.split(f)
        if not lexists(f) and lexists(f_dir):
            dirs, files = listdir(f_dir)
            for x in files:
                xname_parts = os.path.split(x)[1].split('.')
                if xname_parts[-1] == ARCHIVE_EXT and '.'.join(xname_parts[:-2]) == f_name:
                    return x
    except Exception, e:
        print "dst_findfile", e

    return f 

def dst_origfile(f):
    global ARCHIVE_EXT 
    # If is an archive, then remove last two file extensions (metadata and archive extension)
    if f.endswith(ARCHIVE_EXT):
        f = '.'.join(f.split('.')[:-2])
    return f

def tgz_stat(f):
    ''' Take a file named like this: "file.s100-m101223211" where 
            s100 <==> size of file was 100 bytes
            m101223211 <==> mtime of file was 101223211 bytes

        Return (size, mtime) or (0,0) if unknown
    '''
    mtime, size = 0, 0
  
    try:
        if f.endswith(ARCHIVE_EXT):
            sig = f.split('.')[-2]
            parts = sig.split('-')
            for p in parts:
                if p.startswith('m'):
                    mtime = int(p[1:])
                elif p.startswith('s'):
                    size = int(p[1:])
    except:
        pass

    return mtime, size          

def _stat(f):
    global ARCHIVE_EXT
    s = ()

    # Filter stat down to (mtime, size) for comparisons.  mtime is first, since it's most likely to change.
    if f.endswith(ARCHIVE_EXT):
        s = tgz_stat(f)
    else:
        _s = os.lstat(f)
        s = (_s[8], _s[6])

    return s

def compare(f1, f2):
    # Only matters that the symbolic link points to the same place
    if islink(f1) or islink(f2):
        try:
            return os.readlink(f1) == os.readlink(f2)
        except:
            return False
    # Otherwise, use file stat information
    else:
        for x,y in zip(_stat(f1), _stat(f2)):
            if x != y:
                return False
        return True

def sync(src, dst):
    global Options, ARCHIVE_EXT
  
    if Options.verbose:
        info("sync %s to %s" % (src, dst))

    # If it's a directory, make it in the destination
    if os.path.isdir(src):
        if not lexists(dst):
            if Options.verbose:
                info("mkdir %s" % (dst))
            os.makedirs(dst)
            copystat(src, dst)


    elif islink(src):
        linkto = os.readlink(src)
        os.symlink(linkto, dst)

    # Otherwise.. copy / archive it.
    else:
        # Wait for an opening to run
        wait_process(1)

        # If the file shouldn't be compressed, then start a normal clone thread
        if Options.no_compress or is_compressed_file(src) or os.path.getsize(src) < Options.compress_min_size * 1024:
            t = CloneFileProcess(src, dst, gz=False)
        # Otherwise, modify the name and start a gzip'd clone thread
        else:
            s = os.lstat(src)
            # Embed mtime and size in the archive file name for later use in comparison
            dst = '%s.m%d-s%d.%s' % (dst, s[8], s[6], ARCHIVE_EXT)
            t = CloneFileProcess(src, dst, gz=True)

        ## Start the clone thread
        t.start()
        ProcessList.append(t)
        
def archive(f):
    global Options, TOMBSTONE

    # If 'f' ended in a '/', then need to split again to get real last components
    dir, base = os.path.split(f)  
    if not base:
        dir, base = os.path.split(dir)  
    
    # New files are created with this special format
    #   <TOMBSTONE>.<utime>.<original_name> 
    new_f = os.path.join(dir, "%s-%s.%s" % (TOMBSTONE, int(time.time()), base))

    if Options.test or Options.verbose:
        info("archive %s to %s" % (f, new_f)) 
        if Options.test:
            return
        
    os.rename(f, new_f)


def is_to_be_archived(fn):
    """Is this not e.g. a fifo, named socket, etc.?"""
    mode = os.lstat(fn)[stat.ST_MODE]
    return stat.S_ISREG(mode) or stat.S_ISDIR(mode) or stat.S_ISLNK(mode)


def synchronize(src, dst):
    global TOMBSTONE, ARCHIVE_EXT

    for root, dirs, files in os.walk(src):

        # skip FIFOs, etc.
        files = [ fn for fn in files
                  if is_to_be_archived(os.path.join(root, fn)) ]

        rel_root = root[len(src):]
        dst_root = dst + rel_root
        dst_dirs, dst_files = listdir(dst_root)

        # Prune tombstoned files
        dst_dirs = filter(lambda x: not x.startswith(TOMBSTONE), dst_dirs)
        dst_files = filter(lambda x: not x.startswith(TOMBSTONE), dst_files)

        # Re-write any archived filenames as their original name
        dst_files = ( dst_origfile(d) for d in dst_files )

        s_src_dirs = set(dirs)
        s_src_files = set(files)
        s_dst_dirs = set(dst_dirs)
        s_dst_files = set(dst_files)

        try:
            # Dirs and files in src, but not dest need to be copied
            for d in s_src_dirs - s_dst_dirs:
                sync(os.path.join(root, d), os.path.join(dst_root, d))

            for f in s_src_files - s_dst_files:
                sync(os.path.join(root, f), dst_findfile(os.path.join(dst_root, f)))

            # Files in common between src and dst should be checked for changes
            for f in s_src_files & s_dst_files:
                # If src and dst files don't match... 
                try:
                    dst_f = dst_findfile(os.path.join(dst_root, f))
                    src_f = os.path.join(root, f)

                    if not compare(src_f, dst_f):
                        # ... archive old file and sync with new file
                        archive(dst_f)
                        sync(src_f, dst_f)
                except OSError, e:
                    info("Failed sync on %s (dst_f: %s) (%s)" % (src_f, dst_f, e))

            # Dirs and files in dst, but not in src need to be archived
            for d in s_dst_dirs - s_src_dirs:
                archive(os.path.join(dst_root, d))
            for f in s_dst_files - s_src_files:
                archive(dst_findfile(os.path.join(dst_root, f)))

        # Blanket catch all excpetions
        #   XXX This probably could be better, but seems to work in practice
        # 
        # Exceptions seen in testing:
        #  * in compare(), OSError: No such file or directory 
        except OSError, e:
            print "Error reading file (possibly broken link):", e
            pass        
            

def prune(dir, min_utime=0):
    global TOMBSTONE

    expire_re = re.compile("%s\.(\d+)\..+" % (TOMBSTONE))
    def _expired(name):
        # Only tombstoned names should be expired
        if not name.startswith(TOMBSTONE):
            return False

        # Parse the file name for utime
        utimes = re.findall(expire_re, name)

        # If didn't find the utime, then not expired
        if len(utimes) != 1:
            return False

        # If utime is more recent than min, then not expired
        if (float(utimes[0]) > min_utime):
            if Options.verbose:
                info("_expired: too new: %s (%s, %s)" % (name, utimes[0], min_utime))
            return False
        
        # If all tests pass, then the file is expired
        if Options.verbose:
            info("_expired: EXPIRED: %s (%s, %s)" % (name, utimes[0], min_utime))
        return True


    for root, dirs, files in os.walk(dir):
        # Figure out which files/dirs have been expired

        # skip FIFOs, etc.
        files = [ fn for fn in files
                  if is_to_be_archived(os.path.join(root, fn)) ]

        rm_dirs = ( os.path.join(root, d) for d in dirs if _expired(d) )
        rm_files = ( os.path.join(root, f) for f in files if _expired(f) )

        # Remove the files/dirs
        for d in rm_dirs:
            if Options.verbose or Options.test:
                info("removing dir : %s" % (d))
            if not Options.test:
                shutil.rmtree(d)

        for f in rm_files:
            if Options.verbose or Options.test:
                info("removing file: %s" % (f))
            if not Options.test:
                os.unlink(f)

        # Don't descend into tombstoned directories
        for d in list(dirs):
            if d.startswith(TOMBSTONE):
                dirs.remove(d)

def restore(src, dst):
    global TOMBSTONE, ARCHIVE_EXT

    err("Restore not implemented.  See source code for implementation"
        " pointers.")

    #def _torestore(f):
    #    if name.startswith(TOMBSTONE):
    #        return False
    #
    # for root, dirs, files in os.walk(src):
    #     rel_root = root[len(src):]
    #     dst_root = dst + rel_root
#
#        # Figure out which files/dirs have been expired
#        s_dirs = ( os.path.join(root, d) for d in dirs if _torestore(d) )
#        s_files = ( os.path.join(root, f) for f in files if _torestore(f) )

# to make this happen, there are two file cases:
# 1. The file is not administratively gzipped - just copy the file
# 2. The file IS administratively gzipped - create the file and copy the
# 'stat' information from the gzip file to the destination.
#

# Basically, look at and unerstand the replication code above, and the
# restoration code should fall into the same pattern.  There's lots of code to
# reuse.

def main():
    ''' Directory tree replication '''
    import optparse
    global NUM_THREADS, MIN_SIZE_KB

    usage = """%prog [options] <target-dir>

Modes of operation 
------------------

1. Synchronization 

    %prog --sync /path/to/src /path/to/target

2. Pruning

    # Prune all deleted files/directories more than 10 days old.
    %prog --prune 10 /path/to/target

3. Restoration

    # Restore files from source to target.
    %prog --restore /rep/source /l/target
"""
    parser = optparse.OptionParser(description=main.__doc__, usage=usage)

    # General options
    parser.add_option("-t", "--test", dest="test", default=False, action="store_true",
        help="Do a testing run - just say what would be done.")
    parser.add_option("-v", "--verbose", dest="verbose", default=False, action="store_true",
        help="Say what's being done as it happens.")
    parser.add_option("--profile", dest="profile", default=False, action="store_true",
        help="Collect and print profiling information.")
    parser.add_option("-n", "--num", dest="num_threads", default=NUM_THREADS, type="int",
        help="Number of file processes to use in parallel")

    # Compression options
    parser.add_option("--no-compress", dest="no_compress", default=False, action="store_true",
        help="Compress archived files [ default = true ]")
    parser.add_option("--compress-min", dest="compress_min_size", type="int", default=MIN_SIZE_KB,
        help="Files smaller than this number will not be compressed (in Kb) [ default = %s ]" % MIN_SIZE_KB)

    # Synchronization options 
    parser.add_option("-s", "--sync", dest="sync", default='', help="Source directory name")

    # Pruning options
    parser.add_option("-p", "--prune", dest="prune", type="float", default=-1.0, 
        help="Prune deleted files older than <prune> days will be pruned.")
  
    # Restore options
    parser.add_option("-r", "--restore", dest="restore", default='', help="Restore files.")


    global Options
    (Options, args) = parser.parse_args()

    if Options.profile:
        prof = hotshot.Profile("rep.prof")

    # Synchronization mode
    if Options.sync:
        # Check options
        if Options.prune != -1.0:
            err("Can't specify synchronization and prune options at the same time.")
        elif not os.path.isdir(Options.sync):
            err("Source (%s) isn't a directory." % (Options.sync))
        elif len(args) != 1:
            err("Must specify exactly one synchronization target.")

        # Synchronize trees
        if Options.profile:
            prof.runcall(lambda: synchronize(Options.sync, args[0]))
            prof.close()
        else:
            synchronize(Options.sync, args[0])

    # Prune mode
    elif Options.prune != -1.0:
        # Check options
        if Options.prune < 0:
            err("Must specify positive number of days for pruning.")
        elif len(args) < 1:
            err("Must specify directories to prune.")

        # The minimum time we want to exist in the directory tree is now minus the number
        #  of days (represented in seconds - this is utime!) in the prune parameter.
        min_utime = time.time() - (60 * 60 * 24) * Options.prune

        # Prune each directory in the arguments
        for dir in args:
            if not os.path.isdir(dir):
                warn("Skipping non-directory %s." % (dir))
            else:
                info("Pruning %s with min_utime=%s" % (dir, min_utime))
                if Options.profile:
                    prof.runcall(lambda: prune(dir, min_utime))
                    prof.close()
                else:
                    prune(dir, min_utime)

    elif Options.restore:
        # Check options
        if len(args) != 1:
            err("Must specify exactly one restore target.")

        restore(Options.restore, args[0])

    # No mode specified
    else:
        err("Must specify mode of operation. (Try the --help option!)")

    # Wait for all file processes to exit
    wait_process()
        
    if Options.profile:
        stats = hotshot.stats.load("rep.prof")
        stats.strip_dirs()
        stats.sort_stats('cumulative')
        stats.print_stats(100)
        stats.sort_stats('time', 'calls')
        stats.print_stats(100)
        os.unlink("rep.prof")

if __name__ == '__main__':
    main()
