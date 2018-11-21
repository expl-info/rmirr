#! /usr/bin/env python2
#
# rmirr.py

# GPL--start
# This file is part of rmirr
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

import datetime
import fcntl
import json
import logging
import os
import os.path
import pwd
import smtplib
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import types

import globls

HISTORY_FILEPATH = os.path.expanduser("~/.rmirr/history.log")
LOCKS_DIRPATH = os.path.expanduser("~/.rmirr/locks")
REPORTS_DIRPATH = os.path.expanduser("~/.rmirr/reports")
RMIRR_DIRPATH = os.path.expanduser("~/.rmirr")

class RmirrException(Exception):
    pass

def do_mirror(mirrorpath, mirrors):
    bestsrcpath, bestxsrcpath, bestmirrord = find_mirror(mirrorpath, mirrors)
    if bestsrcpath == "":
        sys.stderr.write("error: no match\n")
        return
    else:
        if globls.debug:
            print "debug: bestsrcpath (%s)" % (bestsrcpath,)
            print "debug: bestxsrcpath (%s)" % (bestxsrcpath,)
            print "debug: bestmirrord (%s)" % (bestmirrord,)

        cmdargs = ["rsync", "-avz"]

        # name
        name = bestmirrord.get("name", None)
        comment = bestmirrord.get("comment", None)

        if globls.mailto:
            email_recipients = globls.mailto
        else:
            email_recipients = bestmirrord.get("email_recipients", globls.defaultsd.get("email_recipients", None))
            if type(email_recipients) != types.ListType:
                email_recipients = []

        # excludes
        excludes = bestmirrord.get("excludes", [])
        for s in excludes:
            cmdargs.append("--exclude=%s" % s)

        # delete
        if globls.allowdelete:
            cmdargs.append("--delete")

        # dry run
        if globls.dryrsync:
            cmdargs.append("--dry-run")

        # validate source
        relpath = mirrorpath[len(bestsrcpath)+1:]
        if globls.safemode:
            if mirrorpath != bestsrcpath:
                if mirrorpath[len(bestsrcpath)] != "/" or relpath.startswith("/"):
                    print "warning: unexpected values for bestsrcpath (%s) and relpath (%s)" % (bestsrcpath, relpath)
                    reply = raw_input("continue (y/n)? ")
                    if reply not in ["y"]:
                        return

        srcuserhostpath = bestmirrord["source"]
        srcuser, srchost, srcpath = userhostpath_split(srcuserhostpath)

        srcpath = mirrorpath
        if not os.path.exists(mirrorpath):
            print "warning: skipping path (%s); does not exist on source" % (mirrorpath,)
            return

        if os.path.isdir(mirrorpath):
            srcpath += "/"
        srcuserhostpath = "%s@%s:%s" % (globls.thisusername, globls.thishostname, srcpath)
        if globls.debug:
            print "debug: new srcuserhostpath (%s)" % (srcuserhostpath,)

        if globls.safemode:
            if not srcuserhostpath.endswith("/"):
                print "warning: srcuserhostpath (%s) does not end with '/'" % (srcuserhostpath,)
                reply = raw_input("continue (y/n)? ")
                if reply not in ["y"]:
                    return

        if globls.thisusername != srcuser:
            print "warning: you (%s) do not match source user (%s)" % (globls.thisusername, srcuser)
            reply = raw_input("continue (y/n)? ")
            if not globls.yes and reply not in ["y"]:
                return

        if globls.thishostname != srchost:
            print "warning: this host (%s) does not match source host (%s)" % (globls.thishostname, srchost)
            reply = raw_input("continue (y/n)? ")
            if not globls.yes and reply not in ["y"]:
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
            dstuser, dsthost, dstpath = userhostpath_split(dstuserhostpath)
            if dstuser == None:
                dstuser = globls.thisusername

            # provide/ update dstpath
            if dstpath == None:
                dstpath = mirrorpath
            else:
                if relpath:
                    dstpath = os.path.join(dstpath, relpath)

            # rebuild
            dstuserhostpath = "%s@%s:%s" % (dstuser, dsthost, dstpath)

            if globls.destinations and dsthost not in globls.destinations:
                if globls.verbose:
                    print "verbose: skipping destination (%s)" % (dsthost,)
                continue

            xcmdargs = cmdargs[:]
            xcmdargs.append(dstuserhostpath)
            print "name:             %s" % name
            print "comment:          %s" % comment
            print "sync from:        %s" % (srcuserhostpath,)
            print "sync to:          %s" % (dstuserhostpath,)
            print "excludes:         %s" % " ".join(excludes)
            print "uselock:          %s" % str(globls.uselock and "yes" or "no")
            print "email recipients: %s" % " ".join(email_recipients)
            if globls.debug:
                print xcmdargs

            if not globls.yes:
                reply = raw_input("execute (y/n/q)? ")
                if reply == "q":
                    print "quitting"
                    sys.exit(0)
                if reply not in ["y"]:
                    print "skipped"
                    continue

            print "running ..."
            if globls.dry:
                print " ".join(xcmdargs)
            else:
                try:
                    lockfd = None
                    repf = None

                    logger.info("starting")
                    logger.info("name=%s" % name)
                    logger.info("comment=%s" % comment)
                    logger.info("from=%s" % srcuserhostpath)
                    logger.info("to=%s" % dstuserhostpath)
                    logger.info("excludes=%s" % " ".join(excludes))
                    logger.info("uselock=%s" % str(globls.uselock and "yes" or "no"))
                    logger.info("email recipients=%s" % " ".join(email_recipients))

                    if globls.uselock:
                        try:
                            lockfd = os.open(os.path.join(LOCKS_DIRPATH, name), os.O_CREAT|os.O_WRONLY)
                            fcntl.lockf(lockfd, fcntl.LOCK_EX|fcntl.LOCK_NB)
                            logger.info("obtained lock")
                        except:
                            print "error: cannot get lock"
                            raise RmirrException("cannot get lock")
                    else:
                        logger.info("bypassing lock")
                        print "info: bypassing lock"

                    try:
                        repf, report_path  = open_report()
                        logger.info("report=%s" % report_path)

                        logger.info("command=%s" % " ".join(xcmdargs))
                        p = subprocess.Popen(xcmdargs,
                            stdout=repf, stderr=subprocess.STDOUT,
                            shell=False, close_fds=True)

                        if globls.showreport:
                            with open(report_path) as showf:
                                while p.returncode == None:
                                    p.poll()
                                    s = showf.read()
                                    sys.stdout.write(s)
                                    if s == "":
                                        time.sleep(0.5)
                        else:
                            p.wait()

                        if p.returncode != 0:
                            print "warning: non-zero exit value (%s)" % (p.returncode,)
                    except:
                        raise RmirrException("mirror failure")
                except RmirrException as e:
                    logger.info(e)
                finally:
                    if lockfd != None:
                        os.close(lockfd)
                    if repf != None:
                        repf.close()
                    logger.info("done")

            if globls.mailreport:
                try:
                    subject = "rmirr report for %s (%s)" % (name, os.path.basename(report_path))
                    sendreport(email_recipients, subject,
                        name, srcuserhostpath, dstuserhostpath, excludes, report_path)
                except:
                    traceback.print_exc()
                    logger.error("failed to send report")
                    print "error: failed to send report"

def find_mirror(mirrorpath, mirrors):
    bestmirrord = None
    bestsrcpath = ""
    bestxsrcpath = ""
    for mirrord in mirrors:
        srcuserhostpath = mirrord["source"]
        _, _, srcpath = userhostpath_split(srcuserhostpath)
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
        if globls.debug:
            print "debug: mirrorpath (%s) srcpath (%s) bestsrcpath (%s) bestxsrcpath (%s)" \
                % (mirrorpath, srcpath, bestsrcpath, bestxsrcpath)
    return bestsrcpath, bestxsrcpath, bestmirrord

def get_datetimestamp():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def load_conf(confpath, normalize):
    """Load configuration file. Ensure that settings are normalized.
    """
    conf = json.load(open(confpath))
    suitesd = conf.get("suites", {})
    mirrors = conf.get("mirrors", [])

    if normalize:
        for suitename, paths in suitesd.items():
            for i, path in enumerate(paths):
                paths[i] = os.path.expanduser(path)

        for mirrord in mirrors:
            mirrord["source"] = userhostpath_normalize(mirrord["source"])

        destinations = mirrord["destinations"]
        for i, userhostpath in enumerate(destinations):
             destinations[i] = userhostpath_normalize(userhostpath)

    return conf

def open_report():
    """Open new report file returning file object and path.
    """
    prefix = "%s-" % get_datetimestamp()
    fd, path  = tempfile.mkstemp(suffix=".txt", prefix=prefix, dir=REPORTS_DIRPATH)
    f = os.fdopen(fd, "w")
    return (f, path)

def sendreport(recipients, subject, name, srcuserhostpath, dstuserhostpath, excludes, report_path):
    sender = "%s@%s" % (whoami(), socket.getfqdn())

    if recipients:
        parts = [
            "From: %s\r\n" % sender,
            "To: %s\r\n" % ", ".join(recipients),
            "Subject: %s\r\n" % subject,
            "\r\n",
            "Name:        %s\n" % name,
            "From:        %s\n" % srcuserhostpath,
            "To:          %s\n" % dstuserhostpath,
            "Excludes:    %s\n" % " ".join(excludes),
            "Report path: %s\n" % report_path,
            "\n",
            "Report:\n",
            open(report_path, "r").read(),
        ]
        server = smtplib.SMTP("localhost")
        server.sendmail(sender, recipients, "".join(parts))
        server.quit()

def setup():
    """Setup. Includes working paths.
    """
    if not os.path.exists(RMIRR_DIRPATH):
        os.mkdir(RMIRR_DIRPATH)
    if not os.path.exists(LOCKS_DIRPATH):
        os.mkdir(LOCKS_DIRPATH)
    if not os.path.exists(REPORTS_DIRPATH):
        os.mkdir(REPORTS_DIRPATH)

    setup_logger()

def setup_logger():
    global logger

    logger = logging.basicConfig(filename=HISTORY_FILEPATH,
        format="[%(asctime)-15s] ["+str(socket.gethostname())+":%(process)d] [%(levelname)s] %(message)s",
        level=logging.NOTSET)
    logger = logging.getLogger()

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

        print "name:             %s" % (mirrord.get("name"),)
        print "comment:          %s" % (mirrord.get("comment"),)
        print "source:           %s" % (mirrord.get("source"),)
        print "names:            %s" % ", ".join(mirrord.get("names", []))
        print "excludes:         %s" % ", ".join(mirrord.get("excludes",[]))
        print "destinations:     %s" % ", ".join(mirrord.get("destinations",[]))
        print "email recipients: %s" % " ".join(mirrord.get("email_recipients", []))

def userhostpath_join(user, host, path):
    """Join user, host, and path components.
    """
    l = []
    if user:
        l.append("%s@" % user)
    l.append(host)
    if path:
        l.append(":%s" % path)
    return "".join(l)

def userhostpath_normalize(userhostpath):
    """Return a normalized userhostpath.
    """
    user, host, path = userhostpath_split(userhostpath)
    if path:
        path = os.path.expanduser(path)
    return userhostpath_join(user, host, path)

def userhostpath_split(userhostpath):
    """Split userhostpath into components and return.
    """
    if "@" in userhostpath:
        user, rest = userhostpath.split("@", 1)
    else:
        user, rest = None, userhostpath
    if ":" in rest:
        host, path = rest.split(":", 1)
    else:
        host, path = rest, None
    return user, host, path

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
--mailto <emailaddr>[,...]
        Set/override recipients when mailing report.
--mailreport
        Mail report.
--nolock
        Do not use/require lock to run.
--safeoff
        Disable safemode.
--showreport
        Show report on console.
--verbose
        Enable verbosity.
-y      Do not ask for confirmation before executing.""" % d

def main():
    progpath = os.path.realpath(sys.argv[0])
    libdir = os.path.realpath(os.path.dirname(progpath))
    etcdir = os.path.realpath(os.path.join(libdir, "../../etc/rmirr"))
    globls.thisusername = whoami()
    globls.thishostname = socket.getfqdn()

    try:
        args = sys.argv[1:]

        confpath = os.path.join(os.path.expanduser("~/.rmirr"), "rmirr.json")
        mirrorpath = None
        showlist = False
        suitename = None

        while args:
            arg = args.pop(0)
            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif arg == "-c" and args:
                confpath = args.pop(0)
            elif arg == "-d" and args:
                globls.destinations = args.pop(0).split(",")
            elif arg == "--debug":
                globls.debug = True
            elif arg == "--delete":
                globls.allowdelete = True
            elif arg == "--dry":
                globls.dry = True
            elif arg == "--dry-rsync":
                globls.dryrsync = True
            elif arg == "-l":
                showlist = True
            elif arg == "--mailto" and args:
                globls.mailto = args.pop(0).split(",")
            elif arg == "--mailreport":
                globls.mailreport = True
            elif arg == "--nolock":
                globls.uselock = False
            elif arg == "-p" and args:
                mirrorpath = os.path.normpath(args.pop(0))
            elif arg == "-s" and args:
                suitename = args.pop(0)
            elif arg == "--safeoff":
                globls.safemode = False
            elif arg == "--showreport":
                globls.showreport = True
            elif arg == "--verbose":
                globls.verbose = True
            elif arg == "-y":
                globls.yes = True
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
        setup()
    except:
        sys.stderr.write("error: cannot setup working file/dir under ~/.rmirr\n")
        sys.exit(1)

    try:
        normalize = not showlist
        globls.conf = load_conf(confpath, normalize)
        globls.defaultsd = globls.conf.get("defaults", {})
        globls.mirrors = globls.conf.get("mirrors", [])
        globls.suitesd = globls.conf.get("suites", {})
    except:
        #traceback.print_exc()
        sys.stderr.write("error: bad/missing configuration file\n")
        sys.exit(1)

    if showlist:
        show_list(globls.suitesd, globls.mirrors)
    elif suitename:
        paths = globls.suitesd.get(suitename)
        for path in paths:
            path = os.path.expanduser(path)
            do_mirror(path, globls.mirrors)
    else:
        do_mirror(mirrorpath, globls.mirrors)

if __name__ == "__main__":
    main()
