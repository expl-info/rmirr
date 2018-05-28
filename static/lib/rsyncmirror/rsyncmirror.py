#! /usr/bin/env python2
#
# rsyncmirror.py

# GPL--start
# This file is part of rsyncmirror
# Copyright (C) 2018 John Marshall
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# GPL--end

import json
import os
import os.path
import pwd
import socket
import subprocess
import sys
import traceback

def whoami():
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except:
        return None

def print_usage():
    d = { "progname": os.path.basename(sys.argv[0]) }
    print """\
usage: %(progname)s [<options>] <path>

Mirror tree at <path>.

Options:
-c <path>
        Location of paths.json configuration file.
-d <hostname>[,...]
        Destinations to mirror to from list in configuration.
--debug
        Enable debugging.
--delete
        Allow file object deletion.
--dry   Dry run. Do not execute.
--dry-rsync
        Dry run for rsync.
-l      List mirrorable paths.
--verbose
        Enable verbosity.
-y      Do not ask for confirmation before executing.
""" % d

if __name__ == "__main__":
    progpath = os.path.realpath(sys.argv[0])
    libdir = os.path.realpath(os.path.dirname(progpath))
    etcdir = os.path.realpath(os.path.join(libdir, "../../etc/rsyncmirror"))
    thisusername = whoami()
    thishostname = socket.getfqdn()

    try:
        args = sys.argv[1:]

        confpath = os.path.join(etcdir, "paths.json")
        debug = False
        allowdelete = False
        destinations = None
        dry = False
        dryrsync = False
        mirrorpath = None
        showlist = False
        verbose = False
        yes = False

        while args:
            arg = args.pop(0)
            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif arg == "-c" and args:
                confpath = args.pop(0)
            elif arg == "-d" and args:
                destinations = args.pop(0).split(",")
            elif arg == "--debug":
                debug = True
            elif arg == "--delete":
                allowdelete = True
            elif arg == "--dry":
                dry = True
            elif arg == "--dry-rsync":
                dryrsync = True
            elif arg == "-l":
                showlist = True
            elif arg == "--verbose":
                verbose = True
            elif arg == "-y":
                yes = True
            elif not args:
                mirrorpath = os.path.realpath(arg)
            else:
                raise Exception()
    except SystemExit:
        raise
    except:
        #traceback.print_exc()
        sys.stderr.write("error: bad/missing arguments\n")
        sys.exit(1)

    try:
        conf = json.load(open(confpath))
        mirrorsd = conf.get("mirrors")
    except:
        #traceback.print_exc()
        sys.stderr.write("error: bad/missing configuration file\n")
        sys.exit(1)

    if showlist:
        for path in sorted(mirrorsd.keys()):
            print "path:         %s" % (path,)
            print "destinations: %s" % (", ".join(mirrorsd[path].get("destinations",[])))
            print
    else:
        bestpath = ""
        for path in mirrorsd:
            if debug:
                print "debug: path (%s) mirrorpath (%s) bestpath (%s)" % (path, mirrorpath, bestpath)
            if mirrorpath.startswith(path):
                if len(path) > len(bestpath):
                    bestpath = path

        if bestpath == "":
            sys.stderr.write("error: no match\n")
            sys.exit(1)
        else:
            mirrord = mirrorsd.get(bestpath)
            if debug:
                print "debug: bestpath (%s)" % (bestpath,)
                print "debug: mirrord (%s)" % (mirrord,)

            cmdargs = ["rsync", "-avz"]

            # excludes
            excludes = mirrord.get("excludes", [])
            for s in excludes:
                cmdargs.append("--exclude=%s" % s)

            # delete
            if allowdelete:
                cmdargs.append("--delete")

            # dry run
            if dryrsync:
                cmdargs.append("--dry-run")

            # validate source
            relpath = mirrorpath[len(bestpath):]
            srcuserhost = mirrord["source"]
            srcuser, srchost = srcuserhost.split("@", 1)
            if os.path.isdir(mirrorpath):
                srcpath = "%s/" % (mirrorpath,)
            srcuserhostpath = "%s@%s:%s" % (thisusername, thishostname, srcpath)

            if thisusername != srcuser:
                print "warning: you (%s) do not match source user (%s)" % (thisusername, srcuser)
                reply = raw_input("continue (y/n)? ")
                if not yes and reply not in ["y"]:
                    sys.exit(1)
            if thishostname != srchost:
                print "warning: this host (%s) does not match source host (%s)" % (thishostname, srchost)
                reply = raw_input("continue (y/n)? ")
                if not yes and reply not in ["y"]:
                    sys.exit(1)
            cmdargs.append(srcpath)

            # build dsthostpath (may be multiple)
            for dstuserhost in mirrord.get("destinations"):
                if "@" not in dstuserhost:
                    dstuserhost = "%s@%s" % (thisusername, dstuserhost)
                dstuser, dsthost = dstuserhost.split("@", 1)
                if destinations and dsthost not in destinations:
                    if verbose:
                        print "verbose: skipping destination (%s)" % (dsthost,)
                    continue

                xcmdargs = cmdargs[:]
                dstuserhostpath = "%s@%s:%s" % (dstuser, dsthost, mirrorpath)
                xcmdargs.append(dstuserhostpath)
                print "sync from: %s" % (srcuserhostpath,)
                print "sync to:   %s" % (dstuserhostpath,)
                print "excludes:  %s" % " ".join(excludes)
                if debug:
                    print xcmdargs
                if not yes:
                    reply = raw_input("execute (y/n/q)? ")
                    if reply == "q":
                        print "quitting"
                        sys.exit(0)
                    if reply not in ["y"]:
                        print "aborted"
                        continue
                print "running ..."
                p = subprocess.Popen(xcmdargs, shell=False, close_fds=True)
                p.wait()
                if p.returncode != 0:
                    print "warning: non-zero exit value (%s)" % (p.returncode,)
