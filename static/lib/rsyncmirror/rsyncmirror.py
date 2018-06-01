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

def do_mirror(mirrorpath, mirrors):
    bestsrcpath, bestxsrcpath, bestmirrord = find_mirror(mirrorpath, mirrors)
    if bestsrcpath == "":
        sys.stderr.write("error: no match\n")
        return
    else:
        if debug:
            print "debug: bestsrcpath (%s)" % (bestsrcpath,)
            print "debug: bestxsrcpath (%s)" % (bestxsrcpath,)
            print "debug: bestmirrord (%s)" % (bestmirrord,)

        cmdargs = ["rsync", "-avz"]

        # excludes
        excludes = bestmirrord.get("excludes", [])
        for s in excludes:
            cmdargs.append("--exclude=%s" % s)

        # delete
        if allowdelete:
            cmdargs.append("--delete")

        # dry run
        if dryrsync:
            cmdargs.append("--dry-run")

        # validate source
        relpath = mirrorpath[len(bestsrcpath)+1:]
        if safemode:
            if mirrorpath != bestsrcpath:
                if mirrorpath[len(bestsrcpath)] != "/" or relpath.startswith("/"):
                    print "warning: unexpected values for bestsrcpath (%s) and relpath (%s)" % (bestsrcpath, relpath)
                    reply = raw_input("continue (y/n)? ")
                    if reply not in ["y"]:
                        return

        srcuserhostpath = bestmirrord["source"]
        srcuser, srchost, srcpath = split_userhostpath(srcuserhostpath)

        srcpath = mirrorpath
        if not os.path.exists(mirrorpath):
            print "warning: skipping path (%s); does not exist on source" % (mirrorpath,)
            return

        if os.path.isdir(mirrorpath):
            srcpath += "/"
        srcuserhostpath = "%s@%s:%s" % (thisusername, thishostname, srcpath)
        if debug:
            print "debug: new srcuserhostpath (%s)" % (srcuserhostpath,)

        if safemode:
            if not srcuserhostpath.endswith("/"):
                print "warning: srcuserhostpath (%s) does not end with '/'" % (srcuserhostpath,)
                reply = raw_input("continue (y/n)? ")
                if reply not in ["y"]:
                    return

        if thisusername != srcuser:
            print "warning: you (%s) do not match source user (%s)" % (thisusername, srcuser)
            reply = raw_input("continue (y/n)? ")
            if not yes and reply not in ["y"]:
                return

        if thishostname != srchost:
            print "warning: this host (%s) does not match source host (%s)" % (thishostname, srchost)
            reply = raw_input("continue (y/n)? ")
            if not yes and reply not in ["y"]:
                return

        # use only srcpath part
        cmdargs.append(srcpath)

        # process for each destination
        sep = None
        for dstuserhostpath in bestmirrord.get("destinations"):
            if sep != None:
                print sep
            else:
                sep = ""

            # provide dstuser if needed
            dstuser, dsthost, dstpath = split_userhostpath(dstuserhostpath)
            if dstuser == None:
                dstuser = thisusername

            # provide/ update dstpath
            if dstpath == None:
                dstpath = mirrorpath
            else:
                if relpath:
                    dstpath = os.path.join(dstpath, relpath)

            # rebuild
            dstuserhostpath = "%s@%s:%s" % (dstuser, dsthost, dstpath)

            if destinations and dsthost not in destinations:
                if verbose:
                    print "verbose: skipping destination (%s)" % (dsthost,)
                continue

            xcmdargs = cmdargs[:]
            xcmdargs.append(dstuserhostpath)
            print "comment:   %s" % bestmirrord.get("comment", "")
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
                    print "skipped"
                    continue

            print "running ..."
            if dry:
                print " ".join(xcmdargs)
            else:
                p = subprocess.Popen(xcmdargs, shell=False, close_fds=True)
                p.wait()
                if p.returncode != 0:
                    print "warning: non-zero exit value (%s)" % (p.returncode,)

def find_mirror(mirrorpath, mirrors):
    bestmirrord = None
    bestsrcpath = ""
    bestxsrcpath = ""
    for mirrord in mirrors:
        srcuserhostpath = mirrord["source"]
        _, _, srcpath = split_userhostpath(srcuserhostpath)
        names = mirrord.get("names") or [None]
        for name in names:
            if name == None:
                xsrcpath = srcpath
            else:
                xsrcpath = os.path.join(srcpath, name)

            if mirrorpath == xsrcpath or mirrorpath.startswith(xsrcpath+"/"):
                if len(xsrcpath) > len(bestxsrcpath):
                    bestxsrcpath = xsrcpath
                    bestsrcpath = srcpath
                    bestmirrord = mirrord
        if debug:
            print "debug: mirrorpath (%s) srcpath (%s) bestsrcpath (%s) bestxsrcpath (%s)" \
                % (mirrorpath, srcpath, bestsrcpath, bestxsrcpath)
    return bestsrcpath, bestxsrcpath, bestmirrord

def show_list(suitesd, mirrors):
    sep = None

    for suitename in sorted(suitesd.keys()):
        if sep != None:
            print sep
        else:
            sep = ""

        paths = suitesd.get(suitename, [])
        print "suite:        %s" % (suitename,)
        print "paths:        %s" % (", ".join(paths))

    for mirrord in sorted(mirrors):
        if sep != None:
            print sep
        else:
            sep = ""

        print "comment:      %s" % (mirrord.get("comment"),)
        print "source:       %s" % (mirrord.get("source"),)
        print "names:        %s" % ", ".join(mirrord.get("names", []))
        print "excludes:     %s" % ", ".join(mirrord.get("excludes",[]))
        print "destinations: %s" % ", ".join(mirrord.get("destinations",[]))

def split_userhostpath(userhostpath):
    if "@" in userhostpath:
        user, rest = userhostpath.split("@", 1)
    else:
        user, rest = None, userhostpath
    if ":" in rest:
        host, path = rest.split(":", 1)
    else:
        host, path = rest, None
    return user, host, os.path.expanduser(path)

def whoami():
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except:
        return None

def print_usage():
    d = { "progname": os.path.basename(sys.argv[0]) }
    print """\
usage: %(progname)s [<options>] (-l | -p <path> | -s <suitename>)

Mirror file objects at <path>.

Where:
-l      List mirrorable paths.
-p <path>
        Mirror path.
-s <suitename>
        Mirror all paths belonging to a suite.

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
--safeoff
        Disable safemode.
--verbose
        Enable verbosity.
-y      Do not ask for confirmation before executing.""" % d

if __name__ == "__main__":
    progpath = os.path.realpath(sys.argv[0])
    libdir = os.path.realpath(os.path.dirname(progpath))
    etcdir = os.path.realpath(os.path.join(libdir, "../../etc/rsyncmirror"))
    thisusername = whoami()
    thishostname = socket.getfqdn()

    try:
        args = sys.argv[1:]

        confpath = os.path.join(os.path.expanduser("~/.rsyncmirror"), "rsyncmirror.json")
        debug = False
        allowdelete = False
        destinations = None
        dry = False
        dryrsync = False
        mirrorpath = None
        safemode = True
        showlist = False
        suitename = None
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
            elif arg == "-p" and args:
                mirrorpath = os.path.normpath(args.pop(0))
            elif arg == "-s" and args:
                suitename = args.pop(0)
            elif arg == "--safeoff":
                safemode = False
            elif arg == "--verbose":
                verbose = True
            elif arg == "-y":
                yes = True
            else:
                raise Exception()

        if not os.path.exists(confpath):
            sys.stderr.write("error: cannot find configuration file (%s)\n" % confpath)
            sys.exit(1)

        if not showlist:
            if not mirrorpath and not suitename:
                raise Exception()
    except SystemExit:
        raise
    except:
        #traceback.print_exc()
        sys.stderr.write("error: bad/missing arguments\n")
        sys.exit(1)

    try:
        conf = json.load(open(confpath))
        mirrors = conf.get("mirrors")
        suitesd = conf.get("suites", {})
    except:
        #traceback.print_exc()
        sys.stderr.write("error: bad/missing configuration file\n")
        sys.exit(1)

    if showlist:
        show_list(suitesd, mirrors)
    elif suitename:
        paths = suitesd.get(suitename)
        for path in paths:
            path = os.path.expanduser(path)
            do_mirror(path, mirrors)
    else:
        do_mirror(mirrorpath, mirrors)
