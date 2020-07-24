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
|   1.0.0 | Initial pre-release for VOLTHA v2.4 support several unused |
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

Release History
---------------

+---------+------------+-----------------------------------------------+
| Version | Date       | Notes                                         |
+=========+============+===============================================+
| v0.0.1  | 2020-07-24 | Initial pypy pre-release available. This is   |
|         |            | primarily for testing out pip install support |
|         |            | and is not expected to be useful outside of   |
|         |            | that.                                         |
+---------+------------+-----------------------------------------------+

Detailed Release History
~~~~~~~~~~~~~~~~~~~~~~~~

v0.0.1.0 (2020-07-24)
^^^^^^^^^^^^^^^^^^^^^

-  Pre-release equivalent to pyVoltha as of this date but with many ONU
   related packages removed, some initial package upgrades, and some
   work needed to get some basic bit rot cleaned up due to lack of
   attention by the goland developers in keeping pyvoltha in-sync
