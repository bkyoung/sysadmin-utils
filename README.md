nando-wave2.py
==============
This cli utility takes a URL as an argument, and provided this URL is an RSS feed, will parse the RSS XML for enclosures of type image, download them to the specified output directory, and finally write out the RSS XML.  There is definitely some tight coupling to the particular system this tool was intended to interact with, but these are easily generalizable if needed.

atlas-uploader.py
=================
This cli utility was an experiment with 12-factor inspired ideas.  The purpose of the utility is to automate the uploading of a newly built vagrant box.  The was written before packer acquired such a capability itself, when I had an automation pipeline to build, upload, and publish these vagrant boxes unattended.

ad2nis-group-sync
=================

This is a python script intended to be run as a cron job.  Its intent is to query Active Directory for a list of members of specified groups, then compare that membership list to the equivalent NIS groups.  If there are any users in the AD group not present in the NIS group, a new proposed group file would be created on the NIS server containing the updated group info, and an email would be sent to the sys admins.  The email would detail what the proposed changes were and give instructions for how to accept the proposed changes.

# Note
This is a one-way sync.  It does NOT put absent NIS members into AD.


rep.py
======

This script is kind of a modified rsync.  What's special about it is the fact that it stores the files in compressed form in the destination directory.  There's an ability to restore files from the compressed (former) destination to an arbitrary restore location.

wwreboot.py
===========

This script is used to batch reboot nodes in an old Warewulf cluster (who remembers *that*?!).  The script is designed to give up on a node if it takes too long rebooting (going down or coming up).  Warewulf was ... difficult at times.  A later edit was added to allow one to specify a subset of nodes (basically a range secified by start and end nodes as arguments), where the original incarnation rebooted all nodes when invoked.
