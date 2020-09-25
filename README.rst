PyVOLTHA-min
============

PyVOLTHA-min is an updated pyVOLTHA package that provides a collection
of python 3.x libraries used to create an VOLTHA OLT device adapter
focused on the VOLTHA v2.4 release and beyond.

As some adapters (including the OpenONU) have required to be moved to
the Go language, there is was not as much maintenance performed on the
pyVOLTHA package.

The goal for this package is to begin to deprecate and remove old
VOLTHA 1.x features and focus on a minimal set of updated imports that can
be supported in Python 3.6 with the hopes to transition to later versions
of python. In addition, effort to remove any GPL or other restrictive
package requirements is also highly desired.

The current plan for version numbering is:

+---------+------------------------------------------------------------+
| Version | Notes                                                      |
+=========+============================================================+
| < 1.0.0 | Pre-release.  As version numbers increase, more imports    |
|         |               will have been upgraded to current and       |
|         |               unused imports/requirements removed          |
+---------+------------------------------------------------------------+
|   2.0.0 | Initial pre-release for VOLTHA v2.4 support several unused |
|         | or stale libraries (consul...) will be marked as being     |
|         | deprecated, but will remain for some backwards             |
|         | compatibility and may be untested                          |
+---------+------------------------------------------------------------+
|   2.4.0 | VOLTHA v2.4 release. Deprecated classes removed so that    |
|         | further cleanup of unused imports so that some work can be |
|         | performed in planning for python 3.7 support               |
+---------+------------------------------------------------------------+

Installation instruction
------------------------

.. code:: bash

   pip install pyvoltha-min

Release Notes
-------------

v2.0.2 (2020-xx-xx)
^^^^^^^^^^^^^^^^^^^

-  This release is a work in progress.  Notes will be added below as updates
   are made.
-  Default KPI subcategory is now OLT and can be set with a kwargs if needed
   for some other type

v2.0.1 (2020-09-24)
^^^^^^^^^^^^^^^^^^^

-  Move etcd/kafka address values to be similar to what OpenOLT uses
-  Small amount of 'assert' cleanup flagged by bandit


v2.0.0 (2020-09-20)
^^^^^^^^^^^^^^^^^^^

-  Pre-release with all but Alarms/Events and logging up to date
   with v2.4 release of VOLTHA
-  Much refactoring of python 2.7 code with movement toward at
   least python 3.5 and later supported
-  Dropped import of __future__ and six (to some extent)
-  Removed simple ONU-only device events related to OMCI
-  Dropped transitions, pcapy, and scapy imports (no longer required)
-  Added missing 'child_device_lost' IAdapter RPC as well as
   a few other IAdapter and inter-adapter API bit rot cleaned up

