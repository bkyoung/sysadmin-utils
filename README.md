About
=====

This is a python script intended to be run as a cron job.  Its intent is to query Active Directory for a list of members of specified groups, then compare that membership list to the equivalent NIS groups.  If there are any users in the AD group not present in the NIS group, a new proposed group file would be created on the NIS server containing the updated group info, and an email would be sent to the sys admins.  The email would detail what the proposed changes were and give instructions for how to accept the proposed changes.

# Note
This is a one-way sync.  It does NOT put absent NIS members into AD.
