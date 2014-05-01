#!/usr/bin/env python

'''Perform a rolling reboot of the "compute" nodes.  If a begin-host and
end-host are specified, only the specified (inclusive) range is rebooted.
Otherwise all hosts are rebooted.
'''



from subprocess import call,Popen,PIPE
from time import time,sleep

import optparse
import sys


MaxParallelReboots = 16
RebootFailTime = 600


# XXX Could make this a command line option
NodeListCmd = 'nodeattr -n compute'
DownNodesCmd = 'wwlist -q -d'
ReadyNodesCmd = 'wwlist -q -r'
RebootNodeCmd = 'rsh -n %s /sbin/reboot'


ToReboot = []
RebootSent = {}
Down = {}


def warn(s):
    print >> sys.stderr, 'warning:', s
def error(s):
    sys.exit('error: ' + s)


def reboot_node(n):
    cmd = RebootNodeCmd % (n)
    print "Rebooting node", n
    retcode = call(cmd.split())
    return retcode == 0

def num_rebooting_nodes():
    return len(RebootSent) + len(Down)

def reboot_nodes():
    while(num_rebooting_nodes() < MaxParallelReboots):
        try:
            n = ToReboot.pop()
        except:
            break

        if reboot_node(n):
            RebootSent[n] = time()
        else:
            warn("%s: Reboot command failed" % n)


def service_reboots():
    '''Returns True if more reboots, False if all have been rebooted. '''
    now = time()

    while True:
        down_nodes = Popen(DownNodesCmd.split(),
                           stdout=PIPE).communicate()[0].split()
        ready_nodes = Popen(ReadyNodesCmd.split(),
                            stdout=PIPE).communicate()[0].split()
        down_nodes_1 = Popen(DownNodesCmd.split(),
                             stdout=PIPE).communicate()[0].split()
        ready_nodes_1 = Popen(ReadyNodesCmd.split(),
                              stdout=PIPE).communicate()[0].split()
        if (set(down_nodes) == set(down_nodes_1)
            and set(ready_nodes) == set(ready_nodes_1)):
            break
        warn('# race detected, retrying...')


    # Move RebootSent nodes to Down list
    for n in down_nodes:
        if n in RebootSent.keys():
            del RebootSent[n]
            Down[n] = now

    # Remove ready nodes from the Down list
    for n in ready_nodes:
        if n in Down.keys():
            del Down[n]

    # Fail reboots that take too long
    for n,t in RebootSent.items():
        if now - t > RebootFailTime:
            del RebootSent[n]
            warn("%s: Node won't go down, skipping" % n)

    # Fail nodes that are down too long
    for n,t in Down.items():
        if now - t > RebootFailTime:
            del Down[n]
            warn("%s: Node down too long, skipping" % n)

    # Reboot more nodes
    reboot_nodes()

    return (len(ToReboot) + num_rebooting_nodes()) != 0


def main():
    parser = optparse.OptionParser(usage="usage: %prog [options]"
                                   " [<begin-node> <end-node>]",
                                   description=__doc__)
    #parser.add_option("-n", "--dry-run", dest="dry_run", action="store_true",
    #                  help="don't actually reboot anything")
    #parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
    #                  help="skip warnings")
    options, args = parser.parse_args()

    if len(args) not in (0, 2):
        parser.print_help()
        sys.exit(1)

    # Only try to reboot nodes that are running right now
    global ToReboot
    ToReboot.extend(Popen(NodeListCmd.split(),
                          stdout=PIPE).communicate()[0].split())

    ToReboot.sort()
    assert len(ToReboot) == len(set(ToReboot)), "nodeattr giving duplicates"

    if len(args) == 2:
        begin_node = args[0]
        end_node = args[1]

        bi = [ i for (i, h) in enumerate(ToReboot) if h == begin_node ]
        if not bi:
            error("unknown host '%s'" % begin_node)
        bi = bi[0]

        ei = [ i for (i, h) in enumerate(ToReboot) if h == end_node ]
        if not ei:
            error("unknown host '%s'" % end_node)
        ei = ei[0]

        ToReboot = ToReboot[bi:ei+1]

    reboot_nodes()

    while service_reboots():
        if len(ToReboot) == 0:
            print "Waiting on %d nodes to boot" % num_rebooting_nodes()

        sleep(1)


if __name__ == '__main__':
    main()
